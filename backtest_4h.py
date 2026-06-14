#!/usr/bin/env python3
"""
4H consensus follow backtest: top-N symbols, proportional weights by account count.

P0: net-direction filter, next 4H candle entry.
P1: WR-weighted sizing, exposure caps.
P2: liquidity whitelist.
P3: stop-loss / take-profit, top1 strength gate.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from fetch_top_traders import (
    DEFAULT_INFO_URLS,
    DEFAULT_LIQUID_TOP_N,
    DEFAULT_STOP_LOSS_PCT,
    DEFAULT_TAKE_PROFIT_PCT,
    FILLS_CACHE_FILE,
    MAX_SYMBOL_WEIGHT,
    MAX_TOTAL_EXPOSURE,
    MIN_NET_RATIO,
    MIN_SIGNAL_ACCOUNTS,
    MIN_TOP1_SIGNAL_ACCOUNTS,
    ConsensusTarget,
    OpenRecord,
    TraderScore,
    build_consensus,
    compute_wr_weights,
    configure_info_urls,
    fetch_fills_7d,
    fetch_liquid_coin_set,
    filter_tradable_consensus,
    format_direction_zh,
    is_open_fill,
    leg_return_with_stops,
    load_fills_cache,
    post_info,
    save_fills_cache,
    utc_now_ms,
    utc_str,
)

BUCKET_MS = 4 * 3_600_000
LEGACY_FILLS_CACHE = "backtest_fills_cache.json"


@dataclass
class BacktestLeg:
    coin: str
    direction: str
    weight: float
    account_count: int
    net_ratio: float
    entry_px: float
    exit_px: float
    notional: float
    gross_pnl: float
    fees: float
    net_pnl: float
    return_pct: float
    entry_candle_ts: int = 0
    exit_reason: str = "close"


@dataclass
class BacktestPeriod:
    bucket_start: int
    bucket_end: int
    equity_before: float
    equity_after: float
    period_return: float
    legs: list[BacktestLeg] = field(default_factory=list)
    skipped: str = ""


def floor_4h(ts_ms: int) -> int:
    return ts_ms - (ts_ms % BUCKET_MS)


def load_accounts(path: str, max_accounts: int) -> list[TraderScore]:
    traders: list[TraderScore] = []
    with open(path, encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            wr = float(row.get("win_rate") or row.get("win_rate_30d") or row.get("win_rate_7d") or 0)
            traders.append(
                TraderScore(
                    address=row["address"],
                    display_name=row.get("display_name", ""),
                    account_value=float(row.get("account_value_usd", 0) or 0),
                    day_pnl=float(row.get("day_pnl", 0) or 0),
                    day_roi=float(row.get("day_roi", 0) or 0),
                    day_volume=float(row.get("day_volume", 0) or 0),
                    week_roi=float(row.get("week_roi", 0) or 0),
                    month_roi=float(row.get("month_roi", 0) or 0),
                    win_rate=wr,
                    closed_trades=int(row.get("closed_trades_7d") or row.get("closed_trades_30d") or 0),
                    score=float(row.get("score", 0) or 0),
                    platform=row.get("platform", "Hyperliquid"),
                    win_rate_source=row.get("win_rate_source", "7d"),
                    year_roi=float(row.get("year_roi") or 0),
                    history_days=int(row.get("history_days") or 0),
                )
            )
            if len(traders) >= max_accounts:
                break
    return traders


def fetch_all_fills(
    traders: list[TraderScore],
    workers: int,
    cache_path: str,
    refresh: bool,
) -> dict[str, list[dict[str, Any]]]:
    cached = {} if refresh else load_fills_cache(cache_path, wr_days=7)
    if not cached and not refresh:
        legacy = os.path.join(os.path.dirname(cache_path) or ".", LEGACY_FILLS_CACHE)
        cached = load_fills_cache(legacy, wr_days=7)
    fills_map: dict[str, list[dict[str, Any]]] = {
        k.split("|", 1)[0]: v for k, v in cached.items()
    }
    todo = [t for t in traders if t.address not in fills_map]
    if not todo:
        print(f"      Fills from cache: {len(fills_map)} accounts")
        return fills_map

    print(f"      Fetching 7D fills for {len(todo)} accounts ({workers} workers) ...")
    done = 0

    def work(trader: TraderScore) -> tuple[str, list[dict[str, Any]]]:
        return trader.address, fetch_fills_7d(trader.address)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(work, t): t for t in todo}
        for fut in as_completed(futures):
            done += 1
            if done % 25 == 0:
                print(f"      Fills {done}/{len(todo)}")
            try:
                addr, fills = fut.result()
                fills_map[addr] = fills
            except Exception as exc:
                addr = futures[fut].address
                print(f"      WARN skip {addr[:10]}... ({exc})")
                fills_map[addr] = []

    save_fills_cache(
        cache_path,
        {f"{addr}|7": fills for addr, fills in fills_map.items()},
        7,
    )
    print(f"      Fills ready: {len(fills_map)} accounts (cached)")
    return fills_map


def records_for_bucket(
    fills_map: dict[str, list[dict[str, Any]]],
    traders: list[TraderScore],
    bucket_start: int,
    bucket_end: int,
) -> list[OpenRecord]:
    addr_wr = {t.address: t.win_rate for t in traders}
    addr_name = {t.address: t.display_name for t in traders}
    addr_rank = {t.address: i for i, t in enumerate(traders, start=1)}
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}

    for address, fills in fills_map.items():
        wr = addr_wr.get(address, 0.0)
        for fill in fills:
            ts = int(fill.get("time", 0) or 0)
            if ts < bucket_start or ts >= bucket_end:
                continue
            direction = str(fill.get("dir", ""))
            if not is_open_fill(direction):
                continue
            coin = str(fill.get("coin", ""))
            px = float(fill.get("px", 0) or 0)
            sz = float(fill.get("sz", 0) or 0)
            if px <= 0 or sz <= 0:
                continue
            key = (address, coin, direction)
            b = buckets.get(key)
            if b is None:
                buckets[key] = {"notional": px * sz, "size": sz, "earliest_ts": ts, "count": 1}
            else:
                b["notional"] += px * sz
                b["size"] += sz
                b["earliest_ts"] = min(b["earliest_ts"], ts)
                b["count"] += 1

    records: list[OpenRecord] = []
    for (address, coin, direction), b in buckets.items():
        records.append(
            OpenRecord(
                address=address,
                display_name=addr_name.get(address, ""),
                win_rate=addr_wr.get(address, 0.0),
                rank=addr_rank.get(address, 0),
                window="4H",
                coin=coin,
                direction=direction,
                avg_entry_px=b["notional"] / b["size"],
                total_size=b["size"],
                open_time=utc_str(b["earliest_ts"]),
                open_ts=b["earliest_ts"],
                fill_count=b["count"],
            )
        )
    return records


_candle_cache: dict[str, dict[int, dict[str, float]]] = {}


def fetch_candles(coin: str, start_ms: int, end_ms: int) -> dict[int, dict[str, float]]:
    if coin in _candle_cache:
        return _candle_cache[coin]
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": "4h", "startTime": start_ms, "endTime": end_ms},
    }
    raw = post_info(payload)
    out: dict[int, dict[str, float]] = {}
    if isinstance(raw, list):
        for c in raw:
            t = int(c.get("t", 0) or 0)
            out[t] = {
                "open": float(c.get("o", 0) or 0),
                "close": float(c.get("c", 0) or 0),
                "high": float(c.get("h", 0) or 0),
                "low": float(c.get("l", 0) or 0),
            }
    _candle_cache[coin] = out
    time.sleep(0.05)
    return out


def pick_signals(
    consensus: list[ConsensusTarget],
    top_n: int,
    liquid_coins: set[str],
    min_top1_accounts: int,
) -> list[ConsensusTarget]:
    return filter_tradable_consensus(
        consensus, top_n, liquid_coins, min_top1_accounts=min_top1_accounts,
    )


def run_backtest(
    fills_map: dict[str, list[dict[str, Any]]],
    traders: list[TraderScore],
    days: int,
    top_n: int,
    capital: float,
    fee_bps: float,
    liquid_coins: set[str],
    max_symbol_weight: float,
    max_total_exposure: float,
    min_top1_accounts: int,
    stop_pct: float,
    tp_pct: float,
) -> tuple[list[BacktestPeriod], dict[str, Any]]:
    now = utc_now_ms()
    end = floor_4h(now)
    start = end - days * 24 * 3_600_000
    candle_start = start - BUCKET_MS
    candle_end = end + 2 * BUCKET_MS

    buckets: list[tuple[int, int]] = []
    t = start
    while t + BUCKET_MS <= end:
        buckets.append((t, t + BUCKET_MS))
        t += BUCKET_MS

    equity = capital
    periods: list[BacktestPeriod] = []
    total_accounts = len(traders)

    for bucket_start, bucket_end in buckets:
        trade_candle_ts = bucket_end
        period = BacktestPeriod(
            bucket_start=bucket_start,
            bucket_end=bucket_end,
            equity_before=equity,
            equity_after=equity,
            period_return=0.0,
        )
        records = records_for_bucket(fills_map, traders, bucket_start, bucket_end)
        if not records:
            period.skipped = "no opens in signal window"
            periods.append(period)
            continue

        consensus = build_consensus(records, "4H", total_accounts)
        signals = pick_signals(consensus, top_n, liquid_coins, min_top1_accounts)
        if not signals:
            period.skipped = "no tradable signals (P0/P2/P3 top1 gate)"
            periods.append(period)
            continue

        weights = compute_wr_weights(signals, max_symbol_weight, max_total_exposure)
        if not weights:
            period.skipped = "zero weights after P1 caps"
            periods.append(period)
            continue

        sig_by_coin = {s.coin: s for s in signals}
        period_pnl = 0.0

        for coin, w in weights.items():
            sig = sig_by_coin[coin]
            notional = equity * w
            candles = fetch_candles(coin, candle_start, candle_end)
            bar = candles.get(trade_candle_ts)
            if not bar or bar["open"] <= 0:
                continue

            entry_px = bar["open"]
            ret, exit_px, exit_reason = leg_return_with_stops(
                bar, sig.consensus_direction, entry_px, stop_pct, tp_pct,
            )

            gross = notional * ret
            fees = notional * (fee_bps / 10_000) * 2
            net = gross - fees
            period_pnl += net

            period.legs.append(
                BacktestLeg(
                    coin=sig.coin,
                    direction=sig.consensus_direction,
                    weight=w,
                    account_count=sig.account_count,
                    net_ratio=sig.net_ratio,
                    entry_px=entry_px,
                    exit_px=exit_px,
                    notional=notional,
                    gross_pnl=gross,
                    fees=fees,
                    net_pnl=net,
                    return_pct=ret,
                    entry_candle_ts=trade_candle_ts,
                    exit_reason=exit_reason,
                )
            )

        if not period.legs:
            period.skipped = "no liquid symbols with candle data"
            periods.append(period)
            continue

        equity += period_pnl
        period.equity_after = equity
        period.period_return = period_pnl / period.equity_before if period.equity_before else 0.0
        periods.append(period)

    traded = [p for p in periods if p.legs]
    wins = sum(1 for p in traded if p.period_return > 0)
    total_ret = (equity / capital - 1.0) if capital else 0.0
    peak = capital
    max_dd = 0.0
    for p in periods:
        eq = p.equity_after if p.legs else p.equity_before
        peak = max(peak, eq)
        if peak > 0:
            max_dd = max(max_dd, (peak - eq) / peak)

    stats = {
        "days": days,
        "top_n": top_n,
        "capital": capital,
        "final_equity": equity,
        "total_return": total_ret,
        "periods_total": len(buckets),
        "periods_traded": len(traded),
        "period_win_rate": wins / len(traded) if traded else 0.0,
        "max_drawdown": max_dd,
        "fee_bps": fee_bps,
        "min_signal_accounts": MIN_SIGNAL_ACCOUNTS,
        "min_net_ratio": MIN_NET_RATIO,
        "liquid_top_n": len(liquid_coins),
        "max_symbol_weight": max_symbol_weight,
        "max_total_exposure": max_total_exposure,
        "min_top1_accounts": min_top1_accounts,
        "stop_pct": stop_pct,
        "tp_pct": tp_pct,
    }
    return periods, stats


def write_trades_csv(path: str, periods: list[BacktestPeriod]) -> None:
    fields = [
        "bucket_start_utc", "signal_end_utc", "entry_candle_utc", "coin", "direction",
        "weight", "account_count", "net_ratio", "entry_px", "exit_px", "exit_reason", "notional",
        "gross_pnl", "fees", "net_pnl", "return_pct", "equity_before", "equity_after", "period_return",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for p in periods:
            if not p.legs:
                continue
            for leg in p.legs:
                w.writerow({
                    "bucket_start_utc": utc_str(p.bucket_start),
                    "signal_end_utc": utc_str(p.bucket_end),
                    "entry_candle_utc": utc_str(leg.entry_candle_ts),
                    "coin": leg.coin,
                    "direction": format_direction_zh(leg.direction),
                    "weight": f"{leg.weight:.4f}",
                    "account_count": leg.account_count,
                    "net_ratio": f"{leg.net_ratio:.4f}",
                    "entry_px": f"{leg.entry_px:.8f}",
                    "exit_px": f"{leg.exit_px:.8f}",
                    "exit_reason": leg.exit_reason,
                    "notional": f"{leg.notional:.2f}",
                    "gross_pnl": f"{leg.gross_pnl:.2f}",
                    "fees": f"{leg.fees:.2f}",
                    "net_pnl": f"{leg.net_pnl:.2f}",
                    "return_pct": f"{leg.return_pct:.6f}",
                    "equity_before": f"{p.equity_before:.2f}",
                    "equity_after": f"{p.equity_after:.2f}",
                    "period_return": f"{p.period_return:.6f}",
                })


def write_summary(path: str, stats: dict[str, Any], periods: list[BacktestPeriod]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("4H Consensus Follow Backtest (P0–P3)\n")
        fh.write(f"Generated: {utc_str(utc_now_ms())}\n\n")
        fh.write(f"Signal window: prior 4H opens → trade next 4H candle\n")
        fh.write(f"P0: net>={stats['min_net_ratio']:.0%}, accounts>={stats['min_signal_accounts']}\n")
        fh.write(f"P1: WR-weight, max {stats['max_symbol_weight']:.0%}/sym, {stats['max_total_exposure']:.0%} total\n")
        fh.write(f"P2: liquidity top {stats['liquid_top_n']}\n")
        fh.write(
            f"P3: top1>={stats['min_top1_accounts']} accts, "
            f"SL -{stats['stop_pct']:.1%}, TP +{stats['tp_pct']:.1%}\n\n"
        )
        fh.write(f"Lookback: {stats['days']} days\n")
        fh.write(f"Initial capital: ${stats['capital']:,.2f}\n")
        fh.write(f"Final equity:    ${stats['final_equity']:,.2f}\n")
        fh.write(f"Total return:    {stats['total_return']:.2%}\n")
        fh.write(f"Max drawdown:    {stats['max_drawdown']:.2%}\n")
        fh.write(f"4H periods:      {stats['periods_traded']}/{stats['periods_total']} traded\n")
        fh.write(f"Period win rate: {stats['period_win_rate']:.1%}\n")
        fh.write(f"Fee (bps/side):  {stats['fee_bps']}\n\n")
        fh.write("Recent periods:\n")
        fh.write("-" * 72 + "\n")
        for p in [x for x in periods if x.legs][-10:]:
            leg_str = ", ".join(
                f"{l.coin}({format_direction_zh(l.direction)},{l.weight:.0%},net={l.net_ratio:.0%},{l.exit_reason},{l.return_pct:+.2%})"
                for l in p.legs
            )
            fh.write(
                f"{utc_str(p.bucket_end)}  ret={p.period_return:+.2%}  eq=${p.equity_after:,.0f}  {leg_str}\n"
            )


def write_html_report(path: str, stats: dict[str, Any], periods: list[BacktestPeriod]) -> None:
    traded = [p for p in periods if p.legs]
    eq_points = [stats["capital"]]
    for p in periods:
        eq_points.append(p.equity_after if p.legs else (eq_points[-1] if eq_points else stats["capital"]))
    max_eq = max(eq_points) if eq_points else stats["capital"]
    bars: list[str] = []
    for eq in eq_points:
        h = int(40 * eq / max_eq) if max_eq else 0
        bars.append(f'<div class="bar" style="height:{h}px" title="${eq:,.0f}"></div>')

    rows: list[str] = []
    for p in reversed(traded[-30:]):
        legs = " · ".join(
            f"{html.escape(l.coin)} {html.escape(format_direction_zh(l.direction))} {l.weight:.0%} {l.exit_reason} {l.return_pct:+.2%}"
            for l in p.legs
        )
        rows.append(
            f"<tr><td>{html.escape(utc_str(p.bucket_end))}</td>"
            f"<td class='wr'>{p.period_return:+.2%}</td>"
            f"<td>${p.equity_after:,.0f}</td><td>{legs}</td></tr>"
        )

    body = f"""<!DOCTYPE html>
<html lang="zh-Hant"><head><meta charset="UTF-8">
<title>4H 跟單回測</title>
<style>
  body {{ font-family: system-ui, sans-serif; background:#0b0f14; color:#e8edf2; padding:24px; }}
  .card {{ background:#131a22; border:1px solid #1e2a38; border-radius:10px; padding:16px; margin-bottom:16px; }}
  .stats {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; }}
  .label {{ color:#8b9cb3; font-size:12px; }}
  .value {{ font-size:22px; color:#00d4aa; font-weight:700; }}
  .chart {{ display:flex; align-items:flex-end; gap:2px; height:44px; margin-top:12px; }}
  .bar {{ width:6px; background:#00d4aa; border-radius:2px 2px 0 0; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th,td {{ padding:8px; border-bottom:1px solid #1e2a38; text-align:left; }}
  .wr {{ color:#00d4aa; font-weight:600; }}
</style></head><body>
<h1>4H 共識跟單回測</h1>
<p style="color:#8b9cb3">P0 淨向≥{stats['min_net_ratio']:.0%} · P1 WR權重 單標的≤{stats['max_symbol_weight']:.0%} 總≤{stats['max_total_exposure']:.0%} · P2 Top{stats['liquid_top_n']} · P3 Top1≥{stats['min_top1_accounts']} SL-{stats['stop_pct']:.1%} TP+{stats['tp_pct']:.1%}</p>
<div class="card"><div class="stats">
  <div><div class="label">總報酬</div><div class="value">{stats['total_return']:.2%}</div></div>
  <div><div class="label">期末權益</div><div class="value">${stats['final_equity']:,.0f}</div></div>
  <div><div class="label">最大回撤</div><div class="value">{stats['max_drawdown']:.2%}</div></div>
  <div><div class="label">週期勝率</div><div class="value">{stats['period_win_rate']:.1%}</div></div>
  <div><div class="label">交易週期</div><div class="value">{stats['periods_traded']}/{stats['periods_total']}</div></div>
</div><div class="chart">{''.join(bars)}</div></div>
<div class="card"><table>
<thead><tr><th>週期結束 (UTC)</th><th>報酬</th><th>權益</th><th>持倉 (權重·報酬)</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table></div>
</body></html>"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(body)


def main() -> int:
    parser = argparse.ArgumentParser(description="4H consensus follow backtest")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--accounts-csv", default="", help="Default: output-dir/top300_accounts.csv")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--top", type=int, default=5, help="Top N consensus symbols")
    parser.add_argument("--capital", type=float, default=10_000.0)
    parser.add_argument("--fee-bps", type=float, default=5.0, help="Fee per side in bps")
    parser.add_argument("--max-accounts", type=int, default=200)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--refresh-fills", action="store_true")
    parser.add_argument("--liquid-top-n", type=int, default=DEFAULT_LIQUID_TOP_N)
    parser.add_argument("--max-symbol-weight", type=float, default=MAX_SYMBOL_WEIGHT)
    parser.add_argument("--max-total-exposure", type=float, default=MAX_TOTAL_EXPOSURE)
    parser.add_argument("--min-top1-accounts", type=int, default=MIN_TOP1_SIGNAL_ACCOUNTS)
    parser.add_argument("--stop-pct", type=float, default=DEFAULT_STOP_LOSS_PCT)
    parser.add_argument("--tp-pct", type=float, default=DEFAULT_TAKE_PROFIT_PCT)
    parser.add_argument("--info-urls", default=",".join(DEFAULT_INFO_URLS))
    args = parser.parse_args()

    configure_info_urls(args.info_urls.split(","))
    os.makedirs(args.output_dir, exist_ok=True)
    accounts_path = args.accounts_csv or os.path.join(args.output_dir, "top300_accounts.csv")
    if not os.path.isfile(accounts_path):
        print(f"ERROR: accounts file not found: {accounts_path}")
        print("Run run_top_traders.bat first.")
        return 1

    t0 = time.time()
    traders = load_accounts(accounts_path, args.max_accounts)
    if not traders:
        print("ERROR: no accounts loaded")
        return 1

    print(f"[1/4] Loaded {len(traders)} accounts from {accounts_path}")
    cache_path = os.path.join(args.output_dir, FILLS_CACHE_FILE)
    fills_map = fetch_all_fills(traders, args.workers, cache_path, args.refresh_fills)

    print(f"[2/4] Loading liquidity whitelist (top {args.liquid_top_n} by 24h vol) ...")
    liquid_coins = fetch_liquid_coin_set(args.liquid_top_n)
    print(f"      Liquid symbols: {len(liquid_coins)}")

    print(
        f"[3/4] Running backtest: {args.days}D · top {args.top} · "
        f"P1 max {args.max_symbol_weight:.0%}/{args.max_total_exposure:.0%} · "
        f"P3 SL -{args.stop_pct:.1%} TP +{args.tp_pct:.1%} ..."
    )
    periods, stats = run_backtest(
        fills_map, traders, args.days, args.top, args.capital, args.fee_bps, liquid_coins,
        args.max_symbol_weight, args.max_total_exposure, args.min_top1_accounts,
        args.stop_pct, args.tp_pct,
    )

    print("[4/4] Writing reports ...")
    write_trades_csv(os.path.join(args.output_dir, "backtest_4h_trades.csv"), periods)
    write_summary(os.path.join(args.output_dir, "backtest_4h_summary.txt"), stats, periods)
    write_html_report(os.path.join(args.output_dir, "backtest_4h_report.html"), stats, periods)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s")
    print(f"  Total return: {stats['total_return']:.2%}  (${stats['capital']:,.0f} → ${stats['final_equity']:,.2f})")
    print(f"  Max DD: {stats['max_drawdown']:.2%}  Period WR: {stats['period_win_rate']:.1%}")
    print(f"  output/backtest_4h_summary.txt")
    print(f"  output/backtest_4h_trades.csv")
    print(f"  output/backtest_4h_report.html")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        raise SystemExit(130)

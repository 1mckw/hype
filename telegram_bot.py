#!/usr/bin/env python3
"""Telegram bot: alert on new trades from tracked Hyperliquid accounts."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

from dataclasses import dataclass

from fetch_top_traders import (
    FILLS_CACHE_FILE,
    FILLS_CACHE_TTL_SEC,
    MIN_CONSENSUS_ACCOUNTS,
    MIN_YEAR_ROI,
    OpenRecord,
    build_info_url_config,
    configure_fills_cache,
    configure_info_urls,
    configure_min_year_roi,
    configure_wr_days,
    direction_side,
    fetch_leaderboard,
    format_direction_zh,
    net_ratio,
    records_from_qualified,
    records_from_qualified_minutes,
    select_top_traders,
    utc_now_ms,
    utc_str,
)

STATE_FILE = "telegram_state.json"
POLL_INTERVAL_SEC = 30 * 60
ALERT_WINDOW_MIN = 30
MAX_ALERTS = 25
TOP_N = 5
BUCKET_4H_MS = 4 * 3_600_000
BUCKET_24H_MS = 24 * 3_600_000


@dataclass
class TelegramCoinConsensus:
    coin: str
    account_count: int
    long_accounts: int
    short_accounts: int
    net_ratio: float
    consensus_direction: str
    open_long: int
    open_short: int
    close_long: int
    close_short: int


def run_trade_scan(
    output_dir: str,
    *,
    count: int = 300,
    min_closed: int = 5,
    scan_limit: int = 2000,
    workers: int = 24,
    fast: bool = True,
    use_cache: bool = True,
) -> tuple[list[OpenRecord], int, list[TelegramCoinConsensus], list[TelegramCoinConsensus]]:
    os.makedirs(output_dir, exist_ok=True)
    rows = fetch_leaderboard(output_dir, use_cache=use_cache)
    qualified = select_top_traders(
        rows,
        target=count,
        min_closed=min_closed,
        scan_limit=scan_limit,
        workers=workers,
        fast=fast,
    )
    if not qualified:
        raise RuntimeError("No qualified traders found")
    records_30m = records_from_qualified_minutes(
        qualified, ALERT_WINDOW_MIN, window="30M",
    )
    records_4h, records_24h = records_from_qualified(qualified)
    total = len(qualified)
    return (
        records_30m,
        total,
        build_telegram_consensus(records_4h, total),
        build_telegram_consensus(records_24h, total),
    )


def build_telegram_consensus(
    records: list[OpenRecord],
    total_accounts: int,
) -> list[TelegramCoinConsensus]:
    by_coin: dict[str, dict[str, set[str]]] = {}

    for record in records:
        coin = record.coin
        bucket = by_coin.get(coin)
        if bucket is None:
            bucket = {
                "open_long": set(),
                "open_short": set(),
                "close_long": set(),
                "close_short": set(),
                "long": set(),
                "short": set(),
            }
            by_coin[coin] = bucket

        dir_zh = format_direction_zh(record.direction)
        if dir_zh == "開多":
            bucket["open_long"].add(record.address)
        elif dir_zh == "開空":
            bucket["open_short"].add(record.address)
        elif dir_zh == "平多":
            bucket["close_long"].add(record.address)
        elif dir_zh == "平空":
            bucket["close_short"].add(record.address)

        side = direction_side(record.direction)
        if side == "Long":
            bucket["long"].add(record.address)
        elif side == "Short":
            bucket["short"].add(record.address)

    results: list[TelegramCoinConsensus] = []
    for coin, bucket in by_coin.items():
        long_n = len(bucket["long"])
        short_n = len(bucket["short"])
        account_count = len(bucket["long"] | bucket["short"])
        if account_count < MIN_CONSENSUS_ACCOUNTS:
            continue

        if long_n > short_n:
            direction = "Long"
        elif short_n > long_n:
            direction = "Short"
        elif long_n > 0:
            direction = "Mixed"
        else:
            direction = "—"

        results.append(
            TelegramCoinConsensus(
                coin=coin,
                account_count=account_count,
                long_accounts=long_n,
                short_accounts=short_n,
                net_ratio=net_ratio(long_n, short_n, account_count),
                consensus_direction=direction,
                open_long=len(bucket["open_long"]),
                open_short=len(bucket["open_short"]),
                close_long=len(bucket["close_long"]),
                close_short=len(bucket["close_short"]),
            )
        )

    results.sort(
        key=lambda item: (item.account_count, item.open_long + item.close_long + item.open_short + item.close_short),
        reverse=True,
    )
    return results


def trade_key(record: OpenRecord) -> str:
    return f"{record.address.lower()}|{record.coin}|{record.direction}|{record.open_ts}"


def merge_records(records: list[OpenRecord]) -> dict[str, OpenRecord]:
    merged: dict[str, OpenRecord] = {}
    for record in records:
        key = trade_key(record)
        prev = merged.get(key)
        if prev is None or record.fill_count > prev.fill_count:
            merged[key] = record
    return merged


def state_path(output_dir: str) -> str:
    return os.path.join(output_dir, STATE_FILE)


def load_trade_state(output_dir: str) -> dict[str, Any]:
    path = state_path(output_dir)
    if not os.path.isfile(path):
        return {"initialized": False, "trades": {}}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("trades"), dict):
            return data
    except (json.JSONDecodeError, OSError):
        pass
    return {"initialized": False, "trades": {}}


def save_trade_state(output_dir: str, state: dict[str, Any]) -> None:
    path = state_path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False)


def detect_new_trades(
    current: dict[str, OpenRecord],
    state: dict[str, Any],
) -> tuple[list[OpenRecord], dict[str, Any]]:
    stored: dict[str, dict[str, Any]] = dict(state.get("trades") or {})
    alerts: list[OpenRecord] = []

    if not state.get("initialized"):
        for key, record in current.items():
            stored[key] = {"fill_count": record.fill_count}
        return [], {"initialized": True, "trades": stored, "bootstrapped_at": utc_str(utc_now_ms())}

    for key, record in current.items():
        prev = stored.get(key)
        if prev is None or record.fill_count > int(prev.get("fill_count", 0)):
            alerts.append(record)
        stored[key] = {"fill_count": record.fill_count}

    alerts.sort(key=lambda r: r.open_ts, reverse=True)
    return alerts, {"initialized": True, "trades": stored, "updated_at": utc_str(utc_now_ms())}


def format_consensus_top5(
    consensus: list[TelegramCoinConsensus],
    *,
    window: str,
    top_n: int = TOP_N,
) -> str:
    lines = [f"📊 共識 TOP{top_n} · {window}"]
    if not consensus:
        lines.append("無共識標的")
        return "\n".join(lines)

    for i, item in enumerate(consensus[:top_n], start=1):
        direction = format_direction_zh(item.consensus_direction)
        lines.append(f"{i}. <b>{item.coin}</b> · {direction}")
    return "\n".join(lines)


def format_new_trades_message(records: list[OpenRecord]) -> str:
    lines = [f"🆕 新成交 · {len(records)} 筆（{ALERT_WINDOW_MIN}M）", ""]
    for i, r in enumerate(records[:MAX_ALERTS], start=1):
        lines.append(f"{i}. <b>{r.coin}</b> · {format_direction_zh(r.direction)}")
    if len(records) > MAX_ALERTS:
        lines.append(f"…另有 {len(records) - MAX_ALERTS} 筆")
    return "\n".join(lines)


def consensus_bucket(ts_ms: int, bucket_ms: int) -> int:
    return ts_ms // bucket_ms


def maybe_send_scheduled_consensus(
    token: str,
    chat_id: str,
    state: dict[str, Any],
    *,
    consensus_4h: list[TelegramCoinConsensus],
    consensus_24h: list[TelegramCoinConsensus],
    force_bootstrap: bool = False,
) -> dict[str, Any]:
    now = utc_now_ms()
    bucket_4h = consensus_bucket(now, BUCKET_4H_MS)
    bucket_24h = consensus_bucket(now, BUCKET_24H_MS)

    if force_bootstrap or not state.get("initialized"):
        state["last_consensus_4h_bucket"] = bucket_4h
        state["last_consensus_24h_bucket"] = bucket_24h
        return state

    last_4h = int(state.get("last_consensus_4h_bucket", bucket_4h))
    if bucket_4h > last_4h:
        send_telegram_message(format_consensus_top5(consensus_4h, window="4H"), token, chat_id)
        state["last_consensus_4h_bucket"] = bucket_4h
        print("  Sent 4H consensus TOP5")

    last_24h = int(state.get("last_consensus_24h_bucket", bucket_24h))
    if bucket_24h > last_24h:
        send_telegram_message(format_consensus_top5(consensus_24h, window="24H"), token, chat_id)
        state["last_consensus_24h_bucket"] = bucket_24h
        print("  Sent 24H consensus TOP5")

    return state


def send_telegram_message(text: str, token: str, chat_id: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:300]
        raise RuntimeError(f"Telegram HTTP {exc.code}: {detail}") from exc
    if not body.get("ok"):
        raise RuntimeError(f"Telegram API error: {body}")


def watch_new_trades(
    token: str,
    chat_id: str,
    output_dir: str,
    scan_kwargs: dict,
    *,
    force_bootstrap: bool = False,
) -> int:
    print("Running trade scan...")
    t0 = time.time()
    records_30m, total, consensus_4h, consensus_24h = run_trade_scan(output_dir, **scan_kwargs)
    current = merge_records(records_30m)
    state = load_trade_state(output_dir)
    if force_bootstrap:
        state = {"initialized": False, "trades": {}}

    alerts, new_state = detect_new_trades(current, state)
    new_state = maybe_send_scheduled_consensus(
        token,
        chat_id,
        new_state,
        consensus_4h=consensus_4h,
        consensus_24h=consensus_24h,
        force_bootstrap=force_bootstrap or not state.get("initialized"),
    )
    save_trade_state(output_dir, new_state)

    print(
        f"  Scan done in {time.time() - t0:.0f}s · accounts={total} · "
        f"tracked={len(current)} · new={len(alerts)}"
    )

    if not state.get("initialized") and not force_bootstrap:
        print("  First run: state bootstrapped, no alerts sent")
        return 0

    if not alerts:
        print("  No new trades")
        return 0

    send_telegram_message(format_new_trades_message(alerts), token, chat_id)
    print(f"  Sent {len(alerts)} new trade alert(s)")
    return len(alerts)


def run_loop(
    token: str,
    chat_id: str,
    output_dir: str,
    scan_kwargs: dict,
    *,
    interval_sec: int,
) -> None:
    while True:
        try:
            watch_new_trades(token, chat_id, output_dir, scan_kwargs)
        except Exception as exc:
            print(f"  ERROR: {exc}", file=sys.stderr)
        print(f"  Sleeping {interval_sec // 60} min...")
        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Telegram alerts for new Hyperliquid trades.")
    parser.add_argument("--token", default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    parser.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", ""))
    parser.add_argument("--output-dir", default=os.environ.get("TELEGRAM_OUTPUT_DIR", "output"))
    parser.add_argument("--once", action="store_true", help="Scan once and exit")
    parser.add_argument("--gha", action="store_true", help="GitHub Actions mode (same as --once)")
    parser.add_argument("--loop", action="store_true", help="Poll forever (default interval 30 min)")
    parser.add_argument("--interval-min", type=int, default=POLL_INTERVAL_SEC // 60)
    parser.add_argument("--bootstrap", action="store_true", help="Reset state without sending alerts")
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--min-year-roi", type=float, default=MIN_YEAR_ROI)
    parser.add_argument("--min-closed", type=int, default=5)
    parser.add_argument("--scan-limit", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--slow", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--goldrush-key", default=os.environ.get("GOLDRUSH_API_KEY", ""))
    parser.add_argument(
        "--info-urls",
        default=os.environ.get("HL_INFO_URLS", "https://api.hyperliquid.xyz/info,https://api-ui.hyperliquid.xyz/info"),
    )
    args = parser.parse_args()

    if not args.token or not args.chat_id:
        print("ERROR: Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID (or --token / --chat-id)", file=sys.stderr)
        return 1

    if args.gha:
        args.workers = min(args.workers, 8)

    info_urls, info_auth = build_info_url_config(args.info_urls, args.goldrush_key or None)
    configure_info_urls(info_urls, info_auth)
    configure_wr_days(7)
    configure_min_year_roi(args.min_year_roi)
    configure_fills_cache(
        os.path.join(args.output_dir, FILLS_CACHE_FILE),
        use=True,
        refresh=False,
        ttl_sec=FILLS_CACHE_TTL_SEC,
    )

    scan_kwargs = {
        "count": args.count,
        "min_closed": args.min_closed,
        "scan_limit": args.scan_limit,
        "workers": args.workers,
        "fast": not args.slow,
        "use_cache": not args.no_cache,
    }
    interval_sec = max(5, args.interval_min) * 60

    try:
        if args.bootstrap:
            watch_new_trades(
                args.token, args.chat_id, args.output_dir, scan_kwargs, force_bootstrap=True,
            )
            return 0

        if args.once or args.gha:
            watch_new_trades(args.token, args.chat_id, args.output_dir, scan_kwargs)
            return 0

        print(f"Trade alert bot started · poll every {interval_sec // 60} min")
        run_loop(args.token, args.chat_id, args.output_dir, scan_kwargs, interval_sec=interval_sec)
        return 0
    except KeyboardInterrupt:
        print("\nStopped.")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

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
from datetime import datetime, timezone
from typing import Any

from fetch_top_traders import (
    FILLS_CACHE_FILE,
    FILLS_CACHE_TTL_SEC,
    MIN_YEAR_ROI,
    OpenRecord,
    build_info_url_config,
    configure_fills_cache,
    configure_info_urls,
    configure_min_year_roi,
    configure_wr_days,
    fetch_leaderboard,
    format_direction_zh,
    records_from_qualified,
    select_top_traders,
    utc_now_ms,
    utc_str,
)

STATE_FILE = "telegram_state.json"
POLL_INTERVAL_SEC = 15 * 60
MAX_ALERTS = 25


def run_trade_scan(
    output_dir: str,
    *,
    count: int = 300,
    min_closed: int = 5,
    scan_limit: int = 2000,
    workers: int = 24,
    fast: bool = True,
    use_cache: bool = True,
) -> tuple[list[OpenRecord], list[OpenRecord], int]:
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
    records_4h, records_24h = records_from_qualified(qualified)
    return records_4h, records_24h, len(qualified)


def trade_key(record: OpenRecord) -> str:
    return f"{record.address.lower()}|{record.coin}|{record.direction}|{record.open_ts}"


def merge_records(records_4h: list[OpenRecord], records_24h: list[OpenRecord]) -> dict[str, OpenRecord]:
    merged: dict[str, OpenRecord] = {}
    for record in records_4h + records_24h:
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


def _short_addr(address: str) -> str:
    if len(address) <= 12:
        return address
    return f"{address[:6]}...{address[-4:]}"


def format_new_trades_message(records: list[OpenRecord], *, account_total: int) -> str:
    lines = [
        f"🆕 新成交 · {len(records)} 筆",
        f"時間: {utc_str(utc_now_ms())}",
        f"監控帳號: {account_total}",
        "",
    ]
    for i, r in enumerate(records[:MAX_ALERTS], start=1):
        name = r.display_name or "—"
        lines.append(
            f"{i}. <b>{r.coin}</b> · {format_direction_zh(r.direction)} · "
            f"#{r.rank} · {_short_addr(r.address)}"
        )
        lines.append(
            f"   均價 {r.avg_entry_px:,.4f} · {r.window} · {r.open_time} · {r.fill_count}筆"
        )
        if name != "—":
            lines.append(f"   {name}")
    if len(records) > MAX_ALERTS:
        lines.append(f"\n…另有 {len(records) - MAX_ALERTS} 筆")
    return "\n".join(lines)


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
    records_4h, records_24h, total = run_trade_scan(output_dir, **scan_kwargs)
    current = merge_records(records_4h, records_24h)
    state = load_trade_state(output_dir)
    if force_bootstrap:
        state = {"initialized": False, "trades": {}}

    alerts, new_state = detect_new_trades(current, state)
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

    msg = format_new_trades_message(alerts, account_total=total)
    send_telegram_message(msg, token, chat_id)
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
    parser.add_argument("--loop", action="store_true", help="Poll forever (default interval 15 min)")
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

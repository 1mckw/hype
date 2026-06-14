#!/usr/bin/env python3
"""Telegram bot: push TOP5 consensus (4H / 24H) on schedule."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from fetch_top_traders import (
    FILLS_CACHE_FILE,
    FILLS_CACHE_TTL_SEC,
    MIN_YEAR_ROI,
    ConsensusTarget,
    build_consensus,
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

if TYPE_CHECKING:
    pass

TOP_N = 5


def run_consensus_scan(
    output_dir: str,
    *,
    count: int = 300,
    min_year_roi: float = MIN_YEAR_ROI,
    min_closed: int = 5,
    scan_limit: int = 2000,
    workers: int = 24,
    fast: bool = True,
    use_cache: bool = True,
) -> tuple[list[ConsensusTarget], list[ConsensusTarget], int]:
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
    total = len(qualified)
    return (
        build_consensus(records_4h, "4H", total),
        build_consensus(records_24h, "24H", total),
        total,
    )


def format_top5_message(
    consensus: list[ConsensusTarget],
    window: str,
    *,
    account_total: int,
    top_n: int = TOP_N,
) -> str:
    lines = [
        f"📊 Hyperliquid 共識 TOP{top_n} · {window}",
        f"時間: {utc_str(utc_now_ms())}",
        f"帳號: {account_total}",
        "",
    ]
    if not consensus:
        lines.append("無共識標的")
        return "\n".join(lines)

    for i, c in enumerate(consensus[:top_n], start=1):
        direction = format_direction_zh(c.consensus_direction)
        lines.append(
            f"{i}. <b>{c.coin}</b> · {direction} · 淨比例 <b>{c.net_ratio:.0%}</b> "
            f"({c.long_accounts}多/{c.short_accounts}空 · {c.account_count}帳號)"
        )
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


def next_4h_boundary_utc(now: datetime | None = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    next_hour = ((now.hour // 4) + 1) * 4
    if next_hour >= 24:
        return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return now.replace(hour=next_hour, minute=0, second=0, microsecond=0)


def sleep_until(dt: datetime) -> None:
    delay = (dt - datetime.now(timezone.utc)).total_seconds()
    if delay > 0:
        print(f"  Next push at {dt.strftime('%Y-%m-%d %H:%M:%S UTC')} (in {delay / 60:.1f} min)")
        time.sleep(delay)


def push_consensus(
    token: str,
    chat_id: str,
    output_dir: str,
    *,
    send_4h: bool,
    send_24h: bool,
    scan_kwargs: dict,
    top_n: int = TOP_N,
) -> None:
    print("Running consensus scan...")
    t0 = time.time()
    consensus_4h, consensus_24h, total = run_consensus_scan(output_dir, **scan_kwargs)
    print(f"  Scan done in {time.time() - t0:.0f}s · accounts={total} · 4H={len(consensus_4h)} · 24H={len(consensus_24h)}")

    if send_4h:
        msg = format_top5_message(consensus_4h, "4H", account_total=total, top_n=top_n)
        send_telegram_message(msg, token, chat_id)
        print("  Sent 4H TOP5")

    if send_24h:
        msg = format_top5_message(consensus_24h, "24H", account_total=total, top_n=top_n)
        send_telegram_message(msg, token, chat_id)
        print("  Sent 24H TOP5")


def run_loop(
    token: str,
    chat_id: str,
    output_dir: str,
    scan_kwargs: dict,
    *,
    boot_notify: bool,
    top_n: int,
) -> None:
    if boot_notify:
        push_consensus(
            token, chat_id, output_dir,
            send_4h=True, send_24h=True, scan_kwargs=scan_kwargs, top_n=top_n,
        )

    while True:
        sleep_until(next_4h_boundary_utc())
        now = datetime.now(timezone.utc)
        send_24h = now.hour == 0
        push_consensus(
            token,
            chat_id,
            output_dir,
            send_4h=True,
            send_24h=send_24h,
            scan_kwargs=scan_kwargs,
            top_n=top_n,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Telegram bot for Hyperliquid consensus TOP5.")
    parser.add_argument("--token", default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    parser.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", ""))
    parser.add_argument("--output-dir", default=os.environ.get("TELEGRAM_OUTPUT_DIR", "output"))
    parser.add_argument("--once", action="store_true", help="Scan and send 4H+24H once, then exit")
    parser.add_argument(
        "--gha",
        action="store_true",
        help="GitHub Actions: send 4H always; 24H at 00:00 UTC; then exit",
    )
    parser.add_argument("--loop", action="store_true", help="Run forever on 4H UTC schedule (default)")
    parser.add_argument("--boot-notify", action="store_true", help="With --loop, also send immediately on start")
    parser.add_argument("--top-n", type=int, default=TOP_N)
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
    top_n = max(1, args.top_n)
    if args.gha:
        args.workers = min(args.workers, 8)

    try:
        if args.gha:
            hour = datetime.now(timezone.utc).hour
            push_consensus(
                args.token,
                args.chat_id,
                args.output_dir,
                send_4h=True,
                send_24h=hour == 0,
                scan_kwargs=scan_kwargs,
                top_n=top_n,
            )
            return 0

        if args.once:
            push_consensus(
                args.token,
                args.chat_id,
                args.output_dir,
                send_4h=True,
                send_24h=True,
                scan_kwargs=scan_kwargs,
                top_n=top_n,
            )
            return 0

        if args.loop or not args.once:
            print("Telegram consensus bot started.")
            print("  Schedule: 4H TOP5 every 4H UTC · 24H TOP5 daily at 00:00 UTC")
            run_loop(
                args.token,
                args.chat_id,
                args.output_dir,
                scan_kwargs,
                boot_notify=args.boot_notify,
                top_n=top_n,
            )
        return 0
    except KeyboardInterrupt:
        print("\nStopped.")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

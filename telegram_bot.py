#!/usr/bin/env python3
"""Telegram bot: scheduled HTML report (4H / 24H)."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

from fetch_top_traders import (
    FILLS_CACHE_FILE,
    FILLS_CACHE_TTL_SEC,
    MIN_YEAR_ROI,
    OpenRecord,
    TraderFills,
    build_consensus,
    build_info_url_config,
    configure_fills_cache,
    configure_info_urls,
    configure_min_year_roi,
    configure_wr_days,
    fetch_leaderboard,
    records_from_qualified,
    select_top_traders,
    utc_now_ms,
    utc_str,
    write_html_report,
)

STATE_FILE = "telegram_state.json"
HTML_REPORT_FILE = "report.html"
POLL_INTERVAL_SEC = 4 * 60 * 60
BUCKET_4H_MS = 4 * 3_600_000
BUCKET_24H_MS = 24 * 3_600_000


def run_full_scan(
    output_dir: str,
    *,
    count: int = 300,
    min_closed: int = 5,
    scan_limit: int = 2000,
    workers: int = 24,
    fast: bool = True,
    use_cache: bool = True,
) -> tuple[int, list[TraderFills], list[OpenRecord], list[OpenRecord]]:
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

    total = len(qualified)
    records_4h, records_24h = records_from_qualified(qualified)
    return total, qualified, records_4h, records_24h


def state_path(output_dir: str) -> str:
    return os.path.join(output_dir, STATE_FILE)


def load_state(output_dir: str) -> dict[str, Any]:
    path = state_path(output_dir)
    if not os.path.isfile(path):
        return {"initialized": False}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
    except (json.JSONDecodeError, OSError):
        pass
    return {"initialized": False}


def save_state(output_dir: str, state: dict[str, Any]) -> None:
    path = state_path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False)


def consensus_bucket(ts_ms: int, bucket_ms: int) -> int:
    return ts_ms // bucket_ms


def consensus_due(state: dict[str, Any], bucket_ms: int, key: str) -> bool:
    if not state.get("initialized"):
        return False
    now_bucket = consensus_bucket(utc_now_ms(), bucket_ms)
    if key not in state:
        return True
    return now_bucket > int(state[key])


def send_telegram_document(
    file_path: str,
    token: str,
    chat_id: str,
    *,
    caption: str = "",
) -> None:
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    fname = os.path.basename(file_path)
    boundary = uuid.uuid4().hex

    with open(file_path, "rb") as fh:
        file_bytes = fh.read()

    body = bytearray()
    for name, value in (("chat_id", chat_id), ("caption", caption)):
        if not value:
            continue
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        body.extend(str(value).encode("utf-8"))
        body.extend(b"\r\n")

    body.extend(f"--{boundary}\r\n".encode())
    body.extend(
        f'Content-Disposition: form-data; name="document"; filename="{fname}"\r\n'.encode()
    )
    body.extend(b"Content-Type: text/html\r\n\r\n")
    body.extend(file_bytes)
    body.extend(f"\r\n--{boundary}--\r\n".encode())

    req = urllib.request.Request(
        url,
        data=bytes(body),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:300]
        raise RuntimeError(f"Telegram HTTP {exc.code}: {detail}") from exc
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error: {result}")


def watch_consensus(
    token: str,
    chat_id: str,
    output_dir: str,
    scan_kwargs: dict,
    *,
    min_year_roi: float = MIN_YEAR_ROI,
    force: bool = False,
    force_24h: bool = False,
    force_bootstrap: bool = False,
) -> int:
    print("Checking consensus schedule...")
    t0 = time.time()
    state = load_state(output_dir)
    now = utc_now_ms()
    bucket_4h = consensus_bucket(now, BUCKET_4H_MS)
    bucket_24h = consensus_bucket(now, BUCKET_24H_MS)

    if force_bootstrap:
        state = {
            "initialized": True,
            "last_consensus_4h_bucket": bucket_4h,
            "last_consensus_24h_bucket": bucket_24h,
            "bootstrapped_at": utc_str(now),
        }
        save_state(output_dir, state)
        print("  State bootstrapped, no alerts sent")
        return 0

    if force:
        need_push = True
        update_4h = True
        update_24h = force_24h
        if not state.get("initialized"):
            state["initialized"] = True
        print("  Force mode: sending HTML report now")
    else:
        due_4h = consensus_due(state, BUCKET_4H_MS, "last_consensus_4h_bucket")
        due_24h = consensus_due(state, BUCKET_24H_MS, "last_consensus_24h_bucket")
        need_push = due_4h or due_24h
        update_4h = due_4h
        update_24h = due_24h

        if not state.get("initialized"):
            state = {
                "initialized": True,
                "last_consensus_4h_bucket": bucket_4h,
                "last_consensus_24h_bucket": bucket_24h,
                "bootstrapped_at": utc_str(now),
            }
            save_state(output_dir, state)
            print("  First run: state bootstrapped, no alerts sent")
            return 0

        if not need_push:
            print("  No report due")
            return 0

    print("Running full scan and building HTML report...")
    total, qualified, records_4h, records_24h = run_full_scan(output_dir, **scan_kwargs)
    consensus_4h = build_consensus(records_4h, "4H", total)
    consensus_24h = build_consensus(records_24h, "24H", total)

    html_path = os.path.join(output_dir, HTML_REPORT_FILE)
    write_html_report(
        html_path,
        qualified,
        records_4h,
        records_24h,
        consensus_4h,
        consensus_24h,
        min_year_roi=min_year_roi,
    )
    size_kb = os.path.getsize(html_path) / 1024
    print(f"  HTML report: {html_path} ({size_kb:.0f} KB)")

    caption = f"📊 Perp 高 ROI 帳號報告\n{utc_str(now)} · {total} 帳號"
    send_telegram_document(html_path, token, chat_id, caption=caption)

    if update_4h:
        state["last_consensus_4h_bucket"] = bucket_4h
    if update_24h:
        state["last_consensus_24h_bucket"] = bucket_24h
    state["updated_at"] = utc_str(now)
    save_state(output_dir, state)

    print(f"  Done in {time.time() - t0:.0f}s · accounts={total} · sent report.html")
    return 1


def run_loop(
    token: str,
    chat_id: str,
    output_dir: str,
    scan_kwargs: dict,
    *,
    min_year_roi: float,
    interval_sec: int,
) -> None:
    while True:
        try:
            watch_consensus(
                token, chat_id, output_dir, scan_kwargs, min_year_roi=min_year_roi,
            )
        except Exception as exc:
            print(f"  ERROR: {exc}", file=sys.stderr)
        print(f"  Sleeping {interval_sec // 60} min...")
        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Telegram scheduled HTML report.")
    parser.add_argument("--token", default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    parser.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", ""))
    parser.add_argument("--output-dir", default=os.environ.get("TELEGRAM_OUTPUT_DIR", "output"))
    parser.add_argument("--once", action="store_true", help="Check once and exit")
    parser.add_argument("--gha", action="store_true", help="GitHub Actions mode (same as --once)")
    parser.add_argument("--loop", action="store_true", help="Poll forever (default interval 4 hours)")
    parser.add_argument("--interval-min", type=int, default=POLL_INTERVAL_SEC // 60)
    parser.add_argument("--bootstrap", action="store_true", help="Reset state without sending alerts")
    parser.add_argument("--force", action="store_true", help="Send HTML report now, ignoring schedule")
    parser.add_argument("--force-24h", action="store_true", help="With --force, also update 24H schedule bucket")
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
    configure_wr_days(30)
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
            watch_consensus(
                args.token, args.chat_id, args.output_dir, scan_kwargs, force_bootstrap=True,
            )
            return 0

        if args.once or args.gha:
            watch_consensus(
                args.token,
                args.chat_id,
                args.output_dir,
                scan_kwargs,
                min_year_roi=args.min_year_roi,
                force=args.force,
                force_24h=args.force_24h,
            )
            return 0

        print(f"HTML report bot started · poll every {interval_sec // 60} min")
        run_loop(
            args.token, args.chat_id, args.output_dir, scan_kwargs,
            min_year_roi=args.min_year_roi, interval_sec=interval_sec,
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

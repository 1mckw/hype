#!/usr/bin/env python3
"""Telegram bot: scheduled 4H / 24H consensus TOP5 + HTML report."""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any

from fetch_top_traders import (
    FILLS_CACHE_FILE,
    FILLS_CACHE_TTL_SEC,
    MIN_CONSENSUS_ACCOUNTS,
    MIN_YEAR_ROI,
    OpenRecord,
    PositionChange,
    TraderFills,
    WATCHLIST_FILE,
    build_consensus,
    build_info_url_config,
    configure_fills_cache,
    configure_info_urls,
    configure_min_year_roi,
    configure_wr_days,
    consensus_direction_from_sides,
    direction_side,
    fetch_leaderboard,
    format_direction_zh,
    net_ratio_from_sides,
    records_from_qualified,
    run_position_tracking,
    select_top_traders,
    should_track_positions,
    utc_now_ms,
    utc_str,
    write_accounts_csv,
    write_html_report,
)

STATE_FILE = "telegram_state.json"
HTML_REPORT_FILE = "report.html"
ACCOUNTS_CSV_FILE = "top300_accounts.csv"
POLL_INTERVAL_SEC = 4 * 60 * 60
TOP_N = 5
BUCKET_4H_MS = 4 * 3_600_000
BUCKET_24H_MS = 24 * 3_600_000

DIRECTION_EMOJI = {
    "開多": "🟢 開多",
    "開空": "🔴 開空",
    "平多": "🟠平多",
    "平空": "🔵平空",
    "多": "🟢 多",
    "空": "🔴 空",
}
DIRECTION_ORDER = {"多": 0, "空": 1, "開多": 2, "平多": 3, "開空": 4, "平空": 5}


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


def format_direction_colored(direction: str) -> str:
    zh = format_direction_zh(direction)
    return DIRECTION_EMOJI.get(zh, zh)


def sort_consensus_by_direction(
    consensus: list[TelegramCoinConsensus],
) -> list[TelegramCoinConsensus]:
    return sorted(
        consensus,
        key=lambda item: (
            DIRECTION_ORDER.get(item.consensus_direction, 99),
            -item.account_count,
            item.coin,
        ),
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

        ol = len(bucket["open_long"])
        cl = len(bucket["close_long"])
        os_ = len(bucket["open_short"])
        cs = len(bucket["close_short"])
        open_accounts = len(bucket["open_long"] | bucket["open_short"])
        direction = consensus_direction_from_sides(ol, os_)

        results.append(
            TelegramCoinConsensus(
                coin=coin,
                account_count=account_count,
                long_accounts=long_n,
                short_accounts=short_n,
                net_ratio=net_ratio_from_sides(ol, os_, open_accounts),
                consensus_direction=direction,
                open_long=ol,
                open_short=os_,
                close_long=cl,
                close_short=cs,
            )
        )

    results.sort(
        key=lambda item: (item.account_count, item.open_long + item.close_long + item.open_short + item.close_short),
        reverse=True,
    )
    return results


def run_full_scan(
    output_dir: str,
    *,
    count: int = 300,
    min_closed: int = 5,
    scan_limit: int = 2000,
    workers: int = 24,
    fast: bool = True,
    use_cache: bool = True,
    watchlist_path: str = "",
    track_positions: bool = False,
) -> tuple[int, list[TraderFills], list[OpenRecord], list[OpenRecord], list[OpenRecord], list[PositionChange] | None]:
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

    write_accounts_csv(os.path.join(output_dir, ACCOUNTS_CSV_FILE), qualified)
    total = len(qualified)
    records_4h, records_24h, records_72h = records_from_qualified(qualified)

    position_changes: list[PositionChange] | None = None
    resolved_watchlist = watchlist_path or os.path.join(output_dir, WATCHLIST_FILE)
    if should_track_positions(track_positions, resolved_watchlist):
        position_changes = run_position_tracking(
            output_dir, resolved_watchlist, workers=workers,
        )

    return total, qualified, records_4h, records_24h, records_72h, position_changes


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


def _bucket_value(state: dict[str, Any], key: str) -> int | None:
    raw = state.get(key)
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def consensus_due(state: dict[str, Any], bucket_ms: int, key: str) -> bool:
    if not state.get("initialized"):
        return False
    now_bucket = consensus_bucket(utc_now_ms(), bucket_ms)
    last = _bucket_value(state, key)
    if last is None:
        return True
    return now_bucket > last


def format_consensus_top5(
    consensus: list[TelegramCoinConsensus],
    *,
    window: str,
    top_n: int = TOP_N,
) -> str:
    lines = [f"📊 共識 TOP{top_n} · {window}"]
    ranked = sort_consensus_by_direction(consensus)
    if not ranked:
        lines.append("無共識標的")
        return "\n".join(lines)

    for i, item in enumerate(ranked[:top_n], start=1):
        direction = html.escape(format_direction_colored(item.consensus_direction))
        lines.append(
            f"{i}. <b>{html.escape(item.coin)}</b> · {direction} · 淨比例 <b>{item.net_ratio:.0%}</b>"
        )
        lines.append(
            f"   開多 {item.open_long} · 開空 {item.open_short} · {item.account_count}帳號"
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


def send_telegram_document(
    file_path: str,
    token: str,
    chat_id: str,
    *,
    caption: str = "",
    content_type: str = "application/octet-stream",
    timeout_sec: int = 300,
) -> None:
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    fname = os.path.basename(file_path)
    boundary = uuid.uuid4().hex
    file_size = os.path.getsize(file_path)
    print(f"  Uploading {fname} ({file_size / 1024:.0f} KB)...")

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
    body.extend(f"Content-Type: {content_type}\r\n\r\n".encode())
    body.extend(file_bytes)
    body.extend(f"\r\n--{boundary}--\r\n".encode())

    req = urllib.request.Request(
        url,
        data=bytes(body),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            result = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"Telegram HTTP {exc.code}: {detail}") from exc
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error: {result}")


def send_html_report(
    html_path: str,
    token: str,
    chat_id: str,
    *,
    caption: str,
) -> None:
    size_kb = os.path.getsize(html_path) / 1024
    print(f"  HTML report: {html_path} ({size_kb:.0f} KB)")
    send_telegram_document(
        html_path,
        token,
        chat_id,
        caption=caption,
        content_type="text/html",
    )


def gha_should_run(output_dir: str, *, force: bool) -> bool:
    if force:
        return True
    state = load_state(output_dir)
    if not state.get("initialized"):
        return True
    return (
        consensus_due(state, BUCKET_4H_MS, "last_consensus_4h_bucket")
        or consensus_due(state, BUCKET_24H_MS, "last_consensus_24h_bucket")
    )


def gha_skip(output_dir: str) -> None:
    state = load_state(output_dir)
    state["last_checked_at"] = utc_str(utc_now_ms())
    save_state(output_dir, state)
    print("  GHA: no consensus due (skipped)")


def watch_consensus(
    token: str,
    chat_id: str,
    output_dir: str,
    scan_kwargs: dict,
    *,
    min_year_roi: float = MIN_YEAR_ROI,
    gha: bool = False,
    force: bool = False,
    force_24h: bool = False,
    force_bootstrap: bool = False,
    watchlist_path: str = "",
    track_positions: bool = False,
) -> int:
    print("Checking consensus schedule...")
    t0 = time.time()
    state = load_state(output_dir)
    now = utc_now_ms()
    bucket_4h = consensus_bucket(now, BUCKET_4H_MS)
    bucket_24h = consensus_bucket(now, BUCKET_24H_MS)
    last_4h = state.get("last_consensus_4h_bucket")
    last_24h = state.get("last_consensus_24h_bucket")
    print(
        f"  Buckets: now_4h={bucket_4h} last_4h={last_4h} · "
        f"now_24h={bucket_24h} last_24h={last_24h} · "
        f"initialized={state.get('initialized', False)} gha={gha} force={force}"
    )

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
        send_4h = True
        send_24h = force_24h
        if not state.get("initialized"):
            state["initialized"] = True
        print("  Force mode: sending consensus + HTML report now")
    else:
        if not state.get("initialized"):
            if gha:
                state["initialized"] = True
                print("  GHA first run: initialized, will send now")
            else:
                state = {
                    "initialized": True,
                    "last_consensus_4h_bucket": bucket_4h,
                    "last_consensus_24h_bucket": bucket_24h,
                    "bootstrapped_at": utc_str(now),
                }
                save_state(output_dir, state)
                print("  First run: state bootstrapped, no alerts sent")
                return 0

        if gha:
            send_4h = consensus_due(state, BUCKET_4H_MS, "last_consensus_4h_bucket")
            send_24h = consensus_due(state, BUCKET_24H_MS, "last_consensus_24h_bucket")
            if not send_4h and not send_24h:
                state["last_checked_at"] = utc_str(now)
                save_state(output_dir, state)
                print("  GHA: no consensus due (skipped)")
                return 0
            print(f"  GHA schedule: 4H due={send_4h}, 24H due={send_24h}")
        else:
            send_4h = consensus_due(state, BUCKET_4H_MS, "last_consensus_4h_bucket")
            send_24h = consensus_due(state, BUCKET_24H_MS, "last_consensus_24h_bucket")
            if not send_4h and not send_24h:
                print("  No consensus due")
                return 0

    print(f"  Will send: 4H={send_4h} 24H={send_24h} HTML=yes")
    print("Running full scan...")
    total, qualified, records_4h, records_24h, records_72h, position_changes = run_full_scan(
        output_dir,
        watchlist_path=watchlist_path,
        track_positions=track_positions,
        **scan_kwargs,
    )
    telegram_4h = build_telegram_consensus(records_4h, total)
    telegram_24h = build_telegram_consensus(records_24h, total)

    sent = 0
    errors: list[str] = []

    if send_4h:
        try:
            send_telegram_message(format_consensus_top5(telegram_4h, window="4H"), token, chat_id)
            state["last_consensus_4h_bucket"] = bucket_4h
            sent += 1
            print("  Sent 4H consensus TOP5")
        except Exception as exc:
            errors.append(f"4H message: {exc}")
            print(f"  ERROR 4H message: {exc}", file=sys.stderr)

    if send_24h:
        try:
            send_telegram_message(format_consensus_top5(telegram_24h, window="24H"), token, chat_id)
            state["last_consensus_24h_bucket"] = bucket_24h
            sent += 1
            print("  Sent 24H consensus TOP5")
        except Exception as exc:
            errors.append(f"24H message: {exc}")
            print(f"  ERROR 24H message: {exc}", file=sys.stderr)

    try:
        consensus_4h = build_consensus(records_4h, "4H", total)
        consensus_24h = build_consensus(records_24h, "24H", total)
        consensus_72h = build_consensus(records_72h, "72H", total)
        html_path = os.path.join(output_dir, HTML_REPORT_FILE)
        write_html_report(
            html_path,
            qualified,
            records_4h,
            records_24h,
            consensus_4h,
            consensus_24h,
            records_72h=records_72h,
            consensus_72h=consensus_72h,
            min_year_roi=min_year_roi,
            include_profiles=False,
            position_changes=position_changes,
        )
        caption = f"Perp 高 ROI 帳號報告\n{utc_str(now)} · {total} 帳號"
        send_html_report(html_path, token, chat_id, caption=caption)
        sent += 1
        print("  Sent report.html")
    except Exception as exc:
        errors.append(f"HTML report: {exc}")
        print(f"  ERROR HTML report: {exc}", file=sys.stderr)

    state["initialized"] = True
    state["updated_at"] = utc_str(now)
    save_state(output_dir, state)

    print(f"  Done in {time.time() - t0:.0f}s · accounts={total} · messages={sent}")
    if errors:
        print(f"  WARN: partial failures: {'; '.join(errors)}", file=sys.stderr)
        html_failed = any(err.startswith("HTML report:") for err in errors)
        if sent == 0 or html_failed:
            if gha:
                print(
                    "  GHA: delivery failed but scan completed; "
                    "will retry on next due bucket",
                    file=sys.stderr,
                )
            else:
                raise RuntimeError("; ".join(errors))
    return sent


def run_loop(
    token: str,
    chat_id: str,
    output_dir: str,
    scan_kwargs: dict,
    *,
    min_year_roi: float,
    interval_sec: int,
    watchlist_path: str = "",
    track_positions: bool = False,
) -> None:
    while True:
        try:
            watch_consensus(
                token, chat_id, output_dir, scan_kwargs,
                min_year_roi=min_year_roi,
                watchlist_path=watchlist_path,
                track_positions=track_positions,
            )
        except Exception as exc:
            print(f"  ERROR: {exc}", file=sys.stderr)
        print(f"  Sleeping {interval_sec // 60} min...")
        time.sleep(interval_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Telegram 4H / 24H consensus TOP5 + HTML report.")
    parser.add_argument("--token", default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    parser.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", ""))
    parser.add_argument("--output-dir", default=os.environ.get("TELEGRAM_OUTPUT_DIR", "output"))
    parser.add_argument("--once", action="store_true", help="Check once and exit")
    parser.add_argument("--gha", action="store_true", help="GitHub Actions mode (same as --once)")
    parser.add_argument("--loop", action="store_true", help="Poll forever (default interval 4 hours)")
    parser.add_argument("--interval-min", type=int, default=POLL_INTERVAL_SEC // 60)
    parser.add_argument("--bootstrap", action="store_true", help="Reset state without sending alerts")
    parser.add_argument("--force", action="store_true", help="Send 4H consensus + HTML now, ignoring schedule")
    parser.add_argument("--force-24h", action="store_true", help="With --force, also send 24H consensus")
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
    parser.add_argument(
        "--watchlist",
        default="",
        help="Watchlist file path (default: {output_dir}/watchlist.txt)",
    )
    parser.add_argument(
        "--track-positions",
        action="store_true",
        help="Track position changes for watchlist addresses",
    )
    args = parser.parse_args()

    if not args.token or not args.chat_id:
        if args.gha and not gha_should_run(args.output_dir, force=args.force):
            gha_skip(args.output_dir)
            return 0
        print("ERROR: Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID (or --token / --chat-id)", file=sys.stderr)
        return 1

    if args.gha:
        args.workers = min(args.workers, 12)
        if args.scan_limit > 1500:
            args.scan_limit = 1500

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
    watchlist_path = args.watchlist or os.path.join(args.output_dir, WATCHLIST_FILE)

    try:
        if args.bootstrap:
            watch_consensus(
                args.token, args.chat_id, args.output_dir, scan_kwargs,
                force_bootstrap=True,
                watchlist_path=watchlist_path,
                track_positions=args.track_positions,
            )
            return 0

        if args.once or args.gha:
            watch_consensus(
                args.token,
                args.chat_id,
                args.output_dir,
                scan_kwargs,
                min_year_roi=args.min_year_roi,
                gha=args.gha,
                force=args.force,
                force_24h=args.force_24h,
                watchlist_path=watchlist_path,
                track_positions=args.track_positions,
            )
            return 0

        print(f"Consensus + HTML bot started · poll every {interval_sec // 60} min")
        run_loop(
            args.token, args.chat_id, args.output_dir, scan_kwargs,
            min_year_roi=args.min_year_roi,
            interval_sec=interval_sec,
            watchlist_path=watchlist_path,
            track_positions=args.track_positions,
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

#!/usr/bin/env python3
"""
Fetch active high-win-rate Hyperliquid traders and export 4H / 24H open records.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
DEFAULT_INFO_URLS = [
    "https://api.hyperliquid.xyz/info",
    "https://api-ui.hyperliquid.xyz/info",
]
GOLDRUSH_INFO_URL = "https://hypercore.goldrushdata.com/info"
CACHE_FILE = "leaderboard_cache.json"
FILLS_CACHE_FILE = "fills_cache.json"
PROFILES_CACHE_FILE = "profiles_cache.json"
FILLS_CACHE_TTL_SEC = 1800
CACHE_TTL_SEC = 600
DEFAULT_WR_DAYS = 30
MAX_FILLS_PER_REQUEST = 2000
MIN_YEAR_ROI = 1.5  # ROI > 150% (trailing 365D, or since inception if <365d)
MIN_DAY_VOLUME_USD = 5_000
MIN_WEEK_VOLUME_USD = 25_000
MIN_ACCOUNT_VALUE_USD = 1_000
MIN_FILLS_30D = 3             # must have >=3 fills in last 30d
MIN_HISTORY_DAYS = 30          # account must be open at least 30 days
MAX_PEAK_DRAWDOWN = 0.50       # exclude if ever lost >= 50% from equity peak

HOUR_MS = 3_600_000
MINUTE_MS = 60_000
DAY_MS = 24 * HOUR_MS
YEAR_MS = 365 * DAY_MS

_rate_lock = threading.Lock()
_info_url_lock = threading.Lock()
_fills_cache_lock = threading.Lock()
_thread_local = threading.local()
_info_url_idx = 0
_info_urls: list[str] = list(DEFAULT_INFO_URLS)
_info_auth: dict[str, str] = {}
_rate_by_url: dict[str, dict[str, float]] = {}
_fills_cache_path = ""
_fills_cache_refresh = False
_fills_cache_use = True
_fills_cache_ttl = FILLS_CACHE_TTL_SEC
_fills_cache: dict[str, list[dict[str, Any]]] = {}
_wr_days = DEFAULT_WR_DAYS
_min_year_roi = MIN_YEAR_ROI
MAX_WEIGHT_PER_MIN = 900  # per info host; stay under 1200 IP limit

# Exclude suspected market makers: tiny per-close PnL at high frequency (Hyperliquid only).
MM_MEDIAN_ABS_PNL_USD = 5.0
MM_MIN_CLOSED_TRADES = 100
MIN_CONSENSUS_ACCOUNTS = 2  # hide symbols with only 1 account


@dataclass
class TraderScore:
    address: str
    display_name: str
    account_value: float
    day_pnl: float
    day_roi: float
    day_volume: float
    week_roi: float
    month_roi: float
    month_pnl: float = 0.0
    alltime_pnl: float = 0.0
    year_roi: float = 0.0
    history_days: int = 0
    win_rate: float = 0.0
    closed_trades: int = 0
    score: float = 0.0
    platform: str = "Hyperliquid"
    win_rate_source: str = "30d"


@dataclass
class OpenRecord:
    address: str
    display_name: str
    win_rate: float
    rank: int
    window: str
    coin: str
    direction: str
    avg_entry_px: float
    total_size: float
    open_time: str
    open_ts: int
    fill_count: int
    platform: str = "Hyperliquid"


@dataclass
class ConsensusTarget:
    coin: str
    window: str
    account_count: int
    long_accounts: int
    short_accounts: int
    consensus_direction: str
    avg_entry_px: float
    total_size: float
    avg_account_wr: float
    account_pct: float
    avg_long_wr: float = 0.0
    avg_short_wr: float = 0.0
    net_ratio: float = 0.0


@dataclass
class DirectionWrRank:
    rank: int
    coin: str
    window: str
    side: str
    account_count: int
    avg_account_wr: float


@dataclass
class TraderFills:
    trader: TraderScore
    fills: list[dict[str, Any]]
    win_rate: float
    closed_trades: int


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def configure_info_urls(urls: list[str], auth_by_url: dict[str, str] | None = None) -> None:
    global _info_urls, _info_url_idx, _rate_by_url, _info_auth
    cleaned = [u.strip().rstrip("/") for u in urls if u.strip()]
    if not cleaned:
        raise ValueError("At least one info URL is required")
    with _info_url_lock:
        _info_urls = cleaned
        _info_url_idx = 0
        _rate_by_url = {}
        _info_auth = {k.strip().rstrip("/"): v for k, v in (auth_by_url or {}).items() if v}


def configure_min_year_roi(min_roi: float) -> None:
    global _min_year_roi
    _min_year_roi = min_roi


def configure_wr_days(days: int) -> None:
    global _wr_days
    if days < 1:
        raise ValueError("WR window must be >= 1 day")
    _wr_days = days


def wr_source_label(days: int | None = None) -> str:
    return f"{days if days is not None else _wr_days}d"


def configure_fills_cache(path: str, *, use: bool = True, refresh: bool = False, ttl_sec: int = FILLS_CACHE_TTL_SEC) -> None:
    global _fills_cache_path, _fills_cache_use, _fills_cache_refresh, _fills_cache, _fills_cache_ttl
    _fills_cache_path = path
    _fills_cache_use = use
    _fills_cache_refresh = refresh
    _fills_cache_ttl = ttl_sec
    _fills_cache = {} if refresh else load_fills_cache(path, ttl_sec, _wr_days)


def load_fills_cache(path: str, ttl_sec: int = FILLS_CACHE_TTL_SEC, wr_days: int = DEFAULT_WR_DAYS) -> dict[str, list[dict[str, Any]]]:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    now = time.time()
    out: dict[str, list[dict[str, Any]]] = {}
    for key, entry in raw.items():
        if isinstance(entry, list):
            legacy_key = str(key) if "|" in str(key) else f"{key}|7"
            out[legacy_key] = entry
            continue
        if not isinstance(entry, dict):
            continue
        saved = float(entry.get("saved", 0) or 0)
        fills = entry.get("fills")
        entry_days = int(entry.get("wr_days", 7) or 7)
        if entry_days and wr_days is not None and entry_days != wr_days:
            continue
        if saved > 0 and now - saved > ttl_sec:
            continue
        if isinstance(fills, list):
            cache_key = str(key) if "|" in str(key) else f"{key}|{entry_days}"
            out[cache_key] = fills
    return out


def save_fills_cache(path: str, fills_map: dict[str, list[dict[str, Any]]], wr_days: int = DEFAULT_WR_DAYS) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    payload: dict[str, Any] = {}
    for key, fills in fills_map.items():
        if "|" in key:
            cache_key = key
            days = int(key.split("|", 1)[1])
        else:
            days = wr_days
            cache_key = f"{key}|{days}"
        payload[cache_key] = {"saved": time.time(), "wr_days": days, "fills": fills}
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)


def _pick_info_url(*, rotate: bool = False) -> str:
    if not rotate:
        sticky = getattr(_thread_local, "info_url", None)
        if sticky and sticky in _info_urls:
            return sticky
        url = _next_info_url()
        _thread_local.info_url = url
        return url
    return _next_info_url()


def _info_headers(url: str) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    key = _info_auth.get(url.rstrip("/"))
    if key:
        headers["Authorization"] = f"Bearer {key}"
    return headers


def _next_info_url() -> str:
    global _info_url_idx
    with _info_url_lock:
        url = _info_urls[_info_url_idx % len(_info_urls)]
        _info_url_idx += 1
        return url


def _acquire_rate_weight(url: str, weight: int) -> None:
    while True:
        with _rate_lock:
            state = _rate_by_url.setdefault(url, {"start": time.monotonic(), "used": 0.0})
            elapsed = time.monotonic() - state["start"]
            if elapsed >= 60:
                state["start"] = time.monotonic()
                state["used"] = 0.0
            if state["used"] + weight <= MAX_WEIGHT_PER_MIN:
                state["used"] += weight
                return
            wait = 60 - elapsed
        time.sleep(max(wait, 0.05))


def _add_rate_weight(url: str, extra: int) -> None:
    if extra <= 0:
        return
    with _rate_lock:
        state = _rate_by_url.setdefault(url, {"start": time.monotonic(), "used": 0.0})
        state["used"] += extra


RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})


def post_info(payload: dict[str, Any], retries: int = 8) -> Any:
    data = json.dumps(payload).encode("utf-8")
    tried: set[str] = set()
    last_http: urllib.error.HTTPError | None = None
    for attempt in range(retries):
        info_url = _pick_info_url(rotate=attempt > 0)
        if info_url in tried and len(tried) >= len(_info_urls):
            info_url = _info_urls[0]
        tried.add(info_url)
        req = urllib.request.Request(
            info_url,
            data=data,
            headers=_info_headers(info_url),
            method="POST",
        )
        _acquire_rate_weight(info_url, 20)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode("utf-8"))
                if isinstance(body, list) and body:
                    _add_rate_weight(info_url, len(body) // 20)
                return body
        except urllib.error.HTTPError as exc:
            last_http = exc
            if exc.code in RETRYABLE_HTTP_CODES and attempt < retries - 1:
                backoff = min(0.5 * (2 ** attempt), 12.0)
                if exc.code in (500, 504):
                    backoff = max(backoff, 1.5)
                time.sleep(backoff)
                continue
            raise
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(min(0.5 * (attempt + 1), 5.0))
                continue
            raise
    if last_http is not None:
        raise last_http
    return None


def fetch_leaderboard(cache_dir: str, use_cache: bool) -> list[dict[str, Any]]:
    cache_path = os.path.join(cache_dir, CACHE_FILE)
    if use_cache and os.path.isfile(cache_path):
        age = time.time() - os.path.getmtime(cache_path)
        if age < CACHE_TTL_SEC:
            with open(cache_path, encoding="utf-8") as fh:
                payload = json.load(fh)
            rows = payload.get("leaderboardRows", [])
            print(f"[1/3] Leaderboard from cache ({len(rows)} rows, {int(age)}s old)")
            return rows

    print("[1/3] Downloading Hyperliquid leaderboard ...")
    with urllib.request.urlopen(LEADERBOARD_URL, timeout=120) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    os.makedirs(cache_dir, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
    rows = payload.get("leaderboardRows", [])
    print(f"      Leaderboard rows: {len(rows)}")
    return rows


def parse_performance(row: dict[str, Any]) -> TraderScore | None:
    perf = {w[0]: w[1] for w in row.get("windowPerformances", [])}
    day = perf.get("day", {})
    week = perf.get("week", {})
    month = perf.get("month", {})
    all_time = perf.get("allTime", {})

    day_volume = float(day.get("vlm", 0) or 0)
    day_roi = float(day.get("roi", 0) or 0)
    day_pnl = float(day.get("pnl", 0) or 0)
    week_roi = float(week.get("roi", 0) or 0)
    month_roi = float(month.get("roi", 0) or 0)
    month_pnl = float(month.get("pnl", 0) or 0)
    week_volume = float(week.get("vlm", 0) or 0)
    alltime_pnl = float(all_time.get("pnl", 0) or 0)
    account_value = float(row.get("accountValue", 0) or 0)

    if day_volume < MIN_DAY_VOLUME_USD:
        return None
    if week_volume < MIN_WEEK_VOLUME_USD:
        return None
    if day_pnl <= 0 or day_roi <= 0:
        return None
    if week_roi <= 0:
        return None
    if month_pnl <= 0:
        return None
    if alltime_pnl <= 0:
        return None
    if account_value < MIN_ACCOUNT_VALUE_USD:
        return None

    return TraderScore(
        address=row["ethAddress"],
        display_name=row.get("displayName") or "",
        account_value=account_value,
        day_pnl=day_pnl,
        day_roi=day_roi,
        day_volume=day_volume,
        week_roi=week_roi,
        month_roi=month_roi,
        month_pnl=month_pnl,
        alltime_pnl=alltime_pnl,
        score=day_roi * math.log10(day_volume + 10) * max(week_roi, 0.0001),
    )


def _hist_value_at(hist: list[list[Any]], ts: int) -> float | None:
    if not hist:
        return None
    chosen = hist[0]
    for pt in hist:
        if int(pt[0]) <= ts:
            chosen = pt
        else:
            break
    try:
        return float(chosen[1])
    except (TypeError, ValueError, IndexError):
        return None


def _max_drawdown_from_av_hist(av_hist: list[list[Any]]) -> float:
    """Peak-to-trough drawdown on account value history (0.0–1.0)."""
    peak = 0.0
    max_dd = 0.0
    for pt in av_hist:
        try:
            val = float(pt[1])
        except (TypeError, ValueError, IndexError):
            continue
        if val <= 0:
            continue
        if val > peak:
            peak = val
        if peak > 0:
            max_dd = max(max_dd, (peak - val) / peak)
    return max_dd


def portfolio_stats(address: str) -> tuple[float | None, int, float]:
    """Return (year_roi, history_days, max_peak_drawdown)."""
    raw = post_info({"type": "portfolio", "user": address})
    if not isinstance(raw, list):
        return None, 0, 0.0
    blocks = {k: v for k, v in raw if isinstance(k, str)}
    block = blocks.get("allTime") or blocks.get("perpAllTime")
    if not block:
        return None, 0, 0.0
    pnl_hist = block.get("pnlHistory") or []
    av_hist = block.get("accountValueHistory") or []
    if len(pnl_hist) < 2 or len(av_hist) < 2:
        return None, 0, 0.0
    span_ms = int(pnl_hist[-1][0]) - int(pnl_hist[0][0])
    history_days = span_ms // DAY_MS
    if history_days < 1:
        return None, history_days, 0.0
    end_ts = int(pnl_hist[-1][0])
    if history_days >= 365:
        cutoff = end_ts - YEAR_MS
        av_start = _hist_value_at(av_hist, cutoff)
        pnl_start = _hist_value_at(pnl_hist, cutoff)
    else:
        av_start = _hist_value_at(av_hist, int(pnl_hist[0][0]))
        pnl_start = _hist_value_at(pnl_hist, int(pnl_hist[0][0]))
    pnl_now = float(pnl_hist[-1][1])
    if av_start is None or pnl_start is None or av_start <= 0:
        return None, history_days, 0.0
    max_dd = _max_drawdown_from_av_hist(av_hist)
    time.sleep(0.03)
    return (pnl_now - pnl_start) / av_start, history_days, max_dd


def year_roi_from_portfolio(address: str) -> tuple[float | None, int]:
    """ROI from portfolio: trailing 365D if history >=365d, else since account inception."""
    year_roi, history_days, _ = portfolio_stats(address)
    return year_roi, history_days


def fetch_fills_by_days(address: str, days: int | None = None) -> list[dict[str, Any]]:
    window_days = days if days is not None else _wr_days
    cache_key = f"{address}|{window_days}"
    if _fills_cache_use and not _fills_cache_refresh:
        with _fills_cache_lock:
            cached = _fills_cache.get(cache_key)
        if cached is not None:
            return cached

    now = utc_now_ms()
    start = now - window_days * DAY_MS
    all_fills: list[dict[str, Any]] = []
    chunk_start = start
    while chunk_start < now:
        batch = post_info(
            {
                "type": "userFillsByTime",
                "user": address,
                "startTime": chunk_start,
                "endTime": now,
                "aggregateByTime": True,
            }
        )
        if not isinstance(batch, list) or not batch:
            break
        all_fills.extend(batch)
        if len(batch) < MAX_FILLS_PER_REQUEST:
            break
        last_ts = max(int(f.get("time", 0) or 0) for f in batch)
        if last_ts <= chunk_start:
            break
        chunk_start = last_ts + 1

    if _fills_cache_use and _fills_cache_path:
        with _fills_cache_lock:
            _fills_cache[cache_key] = all_fills
    return all_fills


def fetch_fills_7d(address: str) -> list[dict[str, Any]]:
    return fetch_fills_by_days(address, DEFAULT_WR_DAYS)


def win_rate_from_fills(fills: list[dict[str, Any]]) -> tuple[float, int]:
    wins = losses = 0
    for fill in fills:
        direction = str(fill.get("dir", ""))
        if not direction.startswith("Close"):
            continue
        pnl = float(fill.get("closedPnl", 0) or 0)
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
    total = wins + losses
    if total == 0:
        return 0.0, 0
    return wins / total, total


def median_abs_closed_pnl(fills: list[dict[str, Any]]) -> tuple[float, int]:
    abs_pnls: list[float] = []
    for fill in fills:
        if not str(fill.get("dir", "")).startswith("Close"):
            continue
        abs_pnls.append(abs(float(fill.get("closedPnl", 0) or 0)))
    if not abs_pnls:
        return 0.0, 0
    return statistics.median(abs_pnls), len(abs_pnls)


def is_suspected_market_maker(
    fills: list[dict[str, Any]],
    median_threshold: float = MM_MEDIAN_ABS_PNL_USD,
    min_closed: int = MM_MIN_CLOSED_TRADES,
) -> bool:
    median_pnl, close_count = median_abs_closed_pnl(fills)
    return close_count > min_closed and median_pnl < median_threshold


def filter_fills_by_hours(fills: list[dict[str, Any]], hours: int) -> list[dict[str, Any]]:
    cutoff = utc_now_ms() - hours * HOUR_MS
    return [f for f in fills if int(f.get("time", 0) or 0) >= cutoff]


def filter_fills_by_minutes(fills: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    cutoff = utc_now_ms() - minutes * MINUTE_MS
    return [f for f in fills if int(f.get("time", 0) or 0) >= cutoff]


def is_active_trader(
    fills: list[dict[str, Any]],
    min_fills: int = MIN_FILLS_30D,
    lookback_days: int | None = None,
) -> bool:
    """Active = >=3 fills in the configured lookback window (default 30d)."""
    if not fills:
        return False
    days = lookback_days if lookback_days is not None else _wr_days
    cutoff = utc_now_ms() - days * DAY_MS
    recent_n = 0
    for fill in fills:
        ts = int(fill.get("time", 0) or 0)
        if ts >= cutoff:
            recent_n += 1
    return recent_n >= min_fills


def is_open_fill(direction: str) -> bool:
    return direction.startswith("Open ") or direction in ("開多", "開空")


def is_tracked_fill(direction: str) -> bool:
    """Open or close perp fill (English or Chinese label)."""
    d = str(direction).strip()
    if not d:
        return False
    if d.startswith("Open ") or d.startswith("Close "):
        return direction_side(d) is not None
    return d in ("開多", "開空", "平多", "平空")


def direction_side(direction: str) -> str | None:
    """Return 'Long' or 'Short' for English or Chinese direction labels."""
    d = direction.strip()
    if d in ("開多", "平多", "Long", "Open Long", "Close Long"):
        return "Long"
    if d in ("開空", "平空", "Short", "Open Short", "Close Short"):
        return "Short"
    if "Long" in d:
        return "Long"
    if "Short" in d:
        return "Short"
    return None


def format_direction_zh(direction: str) -> str:
    mapping = {
        "Open Long": "開多",
        "Close Long": "平多",
        "Open Short": "開空",
        "Close Short": "平空",
        "Long": "開多",
        "Short": "開空",
        "Mixed": "混合",
        "—": "—",
    }
    if direction in mapping:
        return mapping[direction]
    if direction.startswith("Open "):
        side = direction[5:]
        if side == "Long":
            return "開多"
        if side == "Short":
            return "開空"
    if direction.startswith("Close "):
        side = direction[6:]
        if side == "Long":
            return "平多"
        if side == "Short":
            return "平空"
    return direction


def format_order_type_zh(order_type: str) -> str:
    mapping = {
        "Limit": "限價",
        "Market": "市價",
        "Stop Market": "止損市價",
        "Stop Limit": "止損限價",
        "Take Profit Market": "止盈市價",
        "Take Profit Limit": "止盈限價",
        "Stop": "止損",
    }
    key = str(order_type).strip()
    return mapping.get(key, key or "—")


def summarize_opens(
    trader: TraderScore,
    rank: int,
    window: str,
    fills: list[dict[str, Any]],
) -> list[OpenRecord]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}

    for fill in fills:
        direction = str(fill.get("dir", ""))
        if not is_tracked_fill(direction):
            continue
        coin = str(fill.get("coin", ""))
        px = float(fill.get("px", 0) or 0)
        sz = float(fill.get("sz", 0) or 0)
        ts = int(fill.get("time", 0) or 0)
        if px <= 0 or sz <= 0 or ts <= 0:
            continue

        key = (coin, format_direction_zh(direction))
        bucket = buckets.get(key)
        if bucket is None:
            buckets[key] = {"notional": px * sz, "size": sz, "earliest_ts": ts, "count": 1}
        else:
            bucket["notional"] += px * sz
            bucket["size"] += sz
            bucket["earliest_ts"] = min(bucket["earliest_ts"], ts)
            bucket["count"] += 1

    records: list[OpenRecord] = []
    for (coin, direction), bucket in buckets.items():
        records.append(
            OpenRecord(
                address=trader.address,
                display_name=trader.display_name,
                win_rate=trader.win_rate,
                rank=rank,
                window=window,
                coin=coin,
                direction=direction,
                avg_entry_px=bucket["notional"] / bucket["size"],
                total_size=bucket["size"],
                open_time=utc_str(bucket["earliest_ts"]),
                open_ts=bucket["earliest_ts"],
                fill_count=bucket["count"],
                platform=trader.platform,
            )
        )
    records.sort(key=lambda r: (r.coin, r.direction, r.open_ts))
    return records


def analyze_trader(trader: TraderScore, *, attempts: int = 3) -> TraderFills | None:
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            year_roi, history_days, max_drawdown = portfolio_stats(trader.address)
            if history_days < MIN_HISTORY_DAYS:
                return None
            if year_roi is None or year_roi < _min_year_roi:
                return None
            if max_drawdown >= MAX_PEAK_DRAWDOWN:
                return None
            trader.year_roi = year_roi
            trader.history_days = history_days
            fills = fetch_fills_by_days(trader.address)
            if not is_active_trader(fills):
                return None
            win_rate, closed = win_rate_from_fills(fills)
            trader.win_rate_source = wr_source_label()
            return TraderFills(trader=trader, fills=fills, win_rate=win_rate, closed_trades=closed)
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code in RETRYABLE_HTTP_CODES and attempt < attempts - 1:
                time.sleep(min(1.5 * (attempt + 1), 6.0))
                continue
            raise
        except urllib.error.URLError as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(min(1.0 * (attempt + 1), 5.0))
                continue
            raise
    if last_exc is not None:
        raise last_exc
    return None


def parallel_analyze(
    candidates: list[TraderScore],
    workers: int,
    min_closed: int,
    target: int,
) -> list[TraderFills]:
    qualified: list[TraderFills] = []
    excluded_mm = 0
    excluded_skip = 0
    api_errors = 0
    done = 0
    total = len(candidates)
    chunk = max(workers * 4, 40)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for start in range(0, total, chunk):
            if len(qualified) >= target:
                break
            batch = candidates[start : start + chunk]
            futures = {pool.submit(analyze_trader, t): t for t in batch}
            for future in as_completed(futures):
                done += 1
                if done % 50 == 0:
                    print(
                        f"      Scanned {done}/{total}, qualified {len(qualified)}/{target}, "
                        f"skipped {excluded_skip}, api_err {api_errors}, excluded_mm {excluded_mm}"
                    )

                try:
                    result = future.result()
                except Exception as exc:
                    api_errors += 1
                    print(f"      WARN skip {futures[future].address[:10]}... ({exc})")
                    continue

                if result is None:
                    excluded_skip += 1
                    continue
                if result.closed_trades < min_closed:
                    continue
                if is_suspected_market_maker(result.fills):
                    excluded_mm += 1
                    continue

                t = result.trader
                t.win_rate = result.win_rate
                t.closed_trades = result.closed_trades
                t.score = max(t.year_roi, 0.0001) * t.score
                qualified.append(result)

                if len(qualified) >= target:
                    break

    print(
        f"      Scanned {done}/{total}, qualified {len(qualified)}/{target}, "
        f"skipped {excluded_skip}, api_err {api_errors}, excluded_mm {excluded_mm}"
    )
    if _fills_cache_use and _fills_cache_path:
        with _fills_cache_lock:
            save_fills_cache(_fills_cache_path, _fills_cache, _wr_days)
    qualified.sort(key=lambda x: (x.trader.year_roi, x.trader.score), reverse=True)
    return qualified[:target]


def select_top_traders(
    rows: list[dict[str, Any]],
    target: int,
    min_closed: int,
    scan_limit: int,
    workers: int,
    fast: bool,
) -> list[TraderFills]:
    print(
        f"[2/3] Scoring + parallel scan (30D active + account>={MIN_HISTORY_DAYS}d + ROI>{_min_year_roi:.0%} "
        f"+ max DD<{MAX_PEAK_DRAWDOWN:.0%}) ..."
    )
    candidates: list[TraderScore] = []
    for row in rows:
        parsed = parse_performance(row)
        if parsed is not None:
            candidates.append(parsed)
    candidates.sort(key=lambda t: t.score, reverse=True)

    # Fast mode: scan top candidates; higher WR thresholds need a wider pool.
    limit = min(scan_limit, 1500 if fast else scan_limit)
    batch = candidates[:limit]
    print(f"      Leaderboard candidates: {len(candidates)}, scanning: {len(batch)}, workers: {workers}")
    if _fills_cache_use and _fills_cache:
        print(f"      Fills cache loaded: {len(_fills_cache)} accounts (TTL {_fills_cache_ttl}s)")

    return parallel_analyze(batch, workers, min_closed, target)


def net_ratio(long_n: int, short_n: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return max(long_n, short_n) / total


_mid_prices_cache: dict[str, float] | None = None


def fetch_mid_prices(refresh: bool = False) -> dict[str, float]:
    global _mid_prices_cache
    if _mid_prices_cache is not None and not refresh:
        return _mid_prices_cache
    raw = post_info({"type": "allMids"})
    mids: dict[str, float] = {}
    if isinstance(raw, dict):
        for coin, px in raw.items():
            try:
                val = float(px)
                if val > 0:
                    mids[str(coin)] = val
            except (TypeError, ValueError):
                continue
    _mid_prices_cache = mids
    return mids


def _avg_wr_by_side(addr_wr: dict[str, float]) -> float:
    if not addr_wr:
        return 0.0
    return sum(addr_wr.values()) / len(addr_wr)


def build_consensus(records: list[OpenRecord], window: str, total_accounts: int) -> list[ConsensusTarget]:
    """Aggregate symbols by how many distinct accounts opened them (high → low)."""
    by_coin: dict[str, dict[str, Any]] = {}

    for r in records:
        coin = r.coin
        bucket = by_coin.get(coin)
        if bucket is None:
            bucket = {
                "long_addr_wr": {},
                "short_addr_wr": {},
                "notional": 0.0,
                "size": 0.0,
                "wr_sum": 0.0,
                "wr_n": 0,
            }
            by_coin[coin] = bucket

        if direction_side(r.direction) == "Long":
            bucket["long_addr_wr"][r.address] = r.win_rate
        elif direction_side(r.direction) == "Short":
            bucket["short_addr_wr"][r.address] = r.win_rate

        bucket["notional"] += r.avg_entry_px * r.total_size
        bucket["size"] += r.total_size
        bucket["wr_sum"] += r.win_rate
        bucket["wr_n"] += 1

    results: list[ConsensusTarget] = []
    for coin, b in by_coin.items():
        long_wr_map: dict[str, float] = b["long_addr_wr"]
        short_wr_map: dict[str, float] = b["short_addr_wr"]
        long_n = len(long_wr_map)
        short_n = len(short_wr_map)
        all_addrs = set(long_wr_map) | set(short_wr_map)
        account_count = len(all_addrs)
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

        nr = net_ratio(long_n, short_n, account_count)

        results.append(
            ConsensusTarget(
                coin=coin,
                window=window,
                account_count=account_count,
                long_accounts=long_n,
                short_accounts=short_n,
                consensus_direction=direction,
                avg_entry_px=b["notional"] / b["size"] if b["size"] > 0 else 0.0,
                total_size=b["size"],
                avg_account_wr=b["wr_sum"] / b["wr_n"] if b["wr_n"] > 0 else 0.0,
                account_pct=account_count / total_accounts if total_accounts > 0 else 0.0,
                avg_long_wr=_avg_wr_by_side(long_wr_map),
                avg_short_wr=_avg_wr_by_side(short_wr_map),
                net_ratio=nr,
            )
        )

    results.sort(key=lambda x: (x.account_count, x.total_size, x.avg_account_wr), reverse=True)
    return results


def build_direction_wr_ranks(
    consensus: list[ConsensusTarget],
    side: str,
    descending: bool,
) -> list[DirectionWrRank]:
    """Rank symbols by average account win rate on Long or Short side."""
    key = "avg_long_wr" if side == "Long" else "avg_short_wr"
    count_key = "long_accounts" if side == "Long" else "short_accounts"
    items = [c for c in consensus if getattr(c, count_key) >= MIN_CONSENSUS_ACCOUNTS]
    items.sort(key=lambda c: (getattr(c, key), getattr(c, count_key), c.coin), reverse=descending)
    return [
        DirectionWrRank(
            rank=i,
            coin=c.coin,
            window=c.window,
            side=side,
            account_count=getattr(c, count_key),
            avg_account_wr=getattr(c, key),
        )
        for i, c in enumerate(items, start=1)
    ]


def records_from_qualified(qualified: list[TraderFills]) -> tuple[list[OpenRecord], list[OpenRecord]]:
    records_4h: list[OpenRecord] = []
    records_24h: list[OpenRecord] = []
    for rank, item in enumerate(qualified, start=1):
        trader = item.trader
        records_4h.extend(summarize_opens(trader, rank, "4H", filter_fills_by_hours(item.fills, 4)))
        records_24h.extend(summarize_opens(trader, rank, "24H", filter_fills_by_hours(item.fills, 24)))
    return records_4h, records_24h


def records_from_qualified_minutes(
    qualified: list[TraderFills],
    minutes: int,
    *,
    window: str = "30M",
) -> list[OpenRecord]:
    records: list[OpenRecord] = []
    for rank, item in enumerate(qualified, start=1):
        trader = item.trader
        records.extend(
            summarize_opens(trader, rank, window, filter_fills_by_minutes(item.fills, minutes))
        )
    return records


def write_accounts_csv(path: str, qualified: list[TraderFills]) -> None:
    fields = [
        "rank", "platform", "address", "display_name", "win_rate", "win_rate_source",
        "closed_trades_30d", "year_roi", "history_days", "account_value_usd", "day_pnl", "day_roi", "day_volume",
        "week_roi", "month_roi", "month_pnl", "alltime_pnl", "score",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for i, item in enumerate(qualified, start=1):
            t = item.trader
            writer.writerow({
                "rank": i, "platform": t.platform, "address": t.address,
                "display_name": t.display_name, "win_rate": f"{t.win_rate:.4f}",
                "win_rate_source": t.win_rate_source,
                "closed_trades_30d": t.closed_trades,
                "year_roi": f"{t.year_roi:.6f}",
                "history_days": t.history_days,
                "account_value_usd": f"{t.account_value:.2f}",
                "day_pnl": f"{t.day_pnl:.2f}", "day_roi": f"{t.day_roi:.6f}",
                "day_volume": f"{t.day_volume:.2f}", "week_roi": f"{t.week_roi:.6f}",
                "month_roi": f"{t.month_roi:.6f}",
                "month_pnl": f"{t.month_pnl:.2f}", "alltime_pnl": f"{t.alltime_pnl:.2f}",
                "score": f"{t.score:.6f}",
            })


def write_consensus_csv(path: str, items: list[ConsensusTarget]) -> None:
    fields = [
        "rank", "coin", "window", "account_count", "account_pct", "consensus_direction",
        "long_accounts", "short_accounts", "avg_long_wr", "avg_short_wr",
        "net_ratio", "avg_entry_px", "total_size", "avg_account_wr",
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for i, c in enumerate(items, start=1):
            writer.writerow({
                "rank": i,
                "coin": c.coin,
                "window": c.window,
                "account_count": c.account_count,
                "account_pct": f"{c.account_pct:.4f}",
                "consensus_direction": format_direction_zh(c.consensus_direction),
                "long_accounts": c.long_accounts,
                "short_accounts": c.short_accounts,
                "avg_long_wr": f"{c.avg_long_wr:.4f}" if c.long_accounts else "",
                "avg_short_wr": f"{c.avg_short_wr:.4f}" if c.short_accounts else "",
                "net_ratio": f"{c.net_ratio:.4f}",
                "avg_entry_px": f"{c.avg_entry_px:.8f}",
                "total_size": f"{c.total_size:.8f}",
                "avg_account_wr": f"{c.avg_account_wr:.4f}",
            })


def write_direction_wr_csv(path: str, items: list[DirectionWrRank], sort_order: str) -> None:
    fields = ["rank", "coin", "window", "side", "account_count", "avg_account_wr", "sort_order"]
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for item in items:
            writer.writerow({
                "rank": item.rank,
                "coin": item.coin,
                "window": item.window,
                "side": format_direction_zh(item.side),
                "account_count": item.account_count,
                "avg_account_wr": f"{item.avg_account_wr:.4f}",
                "sort_order": sort_order,
            })


def write_direction_wr_exports(output_dir: str, consensus: list[ConsensusTarget], window: str) -> dict[str, list[DirectionWrRank]]:
    tag = window.lower()
    exports: dict[str, list[DirectionWrRank]] = {}
    for side in ("Long", "Short"):
        for descending, label in ((True, "desc"), (False, "asc")):
            items = build_direction_wr_ranks(consensus, side, descending)
            path = os.path.join(output_dir, f"consensus_{tag}_{side.lower()}_wr_{label}.csv")
            write_direction_wr_csv(path, items, "high_to_low" if descending else "low_to_high")
            exports[f"{window}_{side}_{label}"] = items
    return exports


def write_trades_csv(path: str, records: list[OpenRecord]) -> None:
    fields = [
        "account_rank", "platform", "address", "display_name", "window",
        "coin", "direction", "avg_entry_px", "total_size", "open_datetime_utc", "open_fill_count",
    ]
    records_sorted = sorted(records, key=lambda r: (r.rank, r.window, r.coin, r.direction, r.open_ts))
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for r in records_sorted:
            writer.writerow({
                "account_rank": r.rank, "platform": r.platform, "address": r.address,
                "display_name": r.display_name,
                "window": r.window, "coin": r.coin, "direction": format_direction_zh(r.direction),
                "avg_entry_px": f"{r.avg_entry_px:.8f}", "total_size": f"{r.total_size:.8f}",
                "open_datetime_utc": r.open_time, "open_fill_count": r.fill_count,
            })


def _fmt_usd(val: float | str) -> str:
    try:
        return f"${float(val):,.2f}"
    except (TypeError, ValueError):
        return str(val)


def _fmt_pct(val: float | str) -> str:
    try:
        return f"{float(val):.2%}"
    except (TypeError, ValueError):
        return str(val)


def _dir_badge(direction: str) -> str:
    label = format_direction_zh(direction)
    d = html.escape(label)
    side = direction_side(direction)
    if side == "Long":
        cls = "long"
    elif side == "Short":
        cls = "short"
    else:
        cls = "neutral"
    return f'<span class="badge {cls}">{d}</span>'


def _order_side_badge(side: str) -> str:
    label = html.escape(str(side).strip())
    if label == "買":
        cls = "long"
    elif label == "賣":
        cls = "short"
    else:
        cls = "neutral"
    return f'<span class="badge {cls}">{label}</span>'


def _position_entry_map(profile: dict[str, Any]) -> dict[str, float]:
    entries: dict[str, float] = {}
    for pos in profile.get("positions") or []:
        if not isinstance(pos, dict):
            continue
        coin = str(pos.get("coin", ""))
        if not coin:
            continue
        try:
            entry = float(pos.get("entry_px", 0) or 0)
        except (TypeError, ValueError):
            continue
        if entry > 0:
            entries[coin] = entry
    return entries


def _pnl_at_hist(hist: list[list[Any]], ts: int) -> float | None:
    val = _hist_value_at(hist, ts)
    return val


def _parse_position_row(pos_wrap: dict[str, Any]) -> dict[str, Any] | None:
    pos = pos_wrap.get("position") or pos_wrap
    if not isinstance(pos, dict):
        return None
    coin = str(pos.get("coin", ""))
    if not coin:
        return None
    try:
        szi = float(pos.get("szi", 0) or 0)
    except (TypeError, ValueError):
        szi = 0.0
    if szi == 0:
        return None
    lev_obj = pos.get("leverage") or {}
    try:
        leverage = float(lev_obj.get("value", 0) or 0)
    except (TypeError, ValueError):
        leverage = 0.0
    lev_type = str(lev_obj.get("type", "cross"))
    try:
        entry_px = float(pos.get("entryPx", 0) or 0)
        pos_val = float(pos.get("positionValue", 0) or 0)
        upnl = float(pos.get("unrealizedPnl", 0) or 0)
        roe = float(pos.get("returnOnEquity", 0) or 0)
        liq = float(pos.get("liquidationPx", 0) or 0)
    except (TypeError, ValueError):
        entry_px = pos_val = upnl = roe = liq = 0.0
    funding = pos.get("cumFunding") or {}
    try:
        funding_all = float(funding.get("allTime", 0) or 0)
    except (TypeError, ValueError):
        funding_all = 0.0
    side = "Long" if szi > 0 else "Short"
    return {
        "coin": coin,
        "side": side,
        "leverage": leverage,
        "lev_type": lev_type,
        "value": pos_val,
        "size": abs(szi),
        "entry_px": entry_px,
        "upnl": upnl,
        "roe": roe,
        "funding": funding_all,
        "liq_px": liq,
    }


def fetch_address_profile(address: str) -> dict[str, Any]:
    """Fetch Coinglass-style wallet snapshot for HTML modal."""
    profile: dict[str, Any] = {
        "address": address,
        "pnl": {"total": 0.0, "h24": 0.0, "h48": 0.0, "d7": 0.0, "d30": 0.0},
        "chart": [],
        "perp_value": 0.0,
        "position_count": 0,
        "leverage": 0.0,
        "total_assets": 0.0,
        "perp_equity": 0.0,
        "spot_usd": 0.0,
        "withdrawable": 0.0,
        "withdraw_pct": 0.0,
        "positions": [],
        "orders": [],
        "fills": [],
        "ledger": [],
        "spot_balances": [],
    }
    now = utc_now_ms()
    try:
        ch = post_info({"type": "clearinghouseState", "user": address})
        spot = post_info({"type": "spotClearinghouseState", "user": address})
        orders_raw = post_info({"type": "frontendOpenOrders", "user": address})
        portfolio_raw = post_info({"type": "portfolio", "user": address})
        fills_raw = post_info({"type": "userFills", "user": address, "aggregateByTime": True})
        ledger_raw = post_info({
            "type": "userNonFundingLedgerUpdates",
            "user": address,
            "startTime": now - 90 * DAY_MS,
        })
    except Exception:
        return profile

    if isinstance(ch, dict):
        ms = ch.get("marginSummary") or {}
        try:
            profile["perp_equity"] = float(ms.get("accountValue", 0) or 0)
            profile["perp_value"] = float(ms.get("totalNtlPos", 0) or 0)
            profile["withdrawable"] = float(ch.get("withdrawable", 0) or 0)
        except (TypeError, ValueError):
            pass
        positions: list[dict[str, Any]] = []
        lev_sum = 0.0
        for wrap in ch.get("assetPositions") or []:
            row = _parse_position_row(wrap)
            if row:
                positions.append(row)
                lev_sum += row["leverage"]
        profile["positions"] = positions
        profile["position_count"] = len(positions)
        profile["leverage"] = lev_sum / len(positions) if positions else 0.0

    if isinstance(spot, dict):
        spot_usd = 0.0
        spot_balances: list[dict[str, Any]] = []
        for bal in spot.get("balances") or []:
            try:
                total = float(bal.get("total", 0) or 0)
                hold = float(bal.get("hold", 0) or 0)
                entry_ntl = float(bal.get("entryNtl", 0) or 0)
                coin = str(bal.get("coin", ""))
                if total <= 0 and hold <= 0:
                    continue
                spot_balances.append({
                    "coin": coin,
                    "total": total,
                    "hold": hold,
                    "entry_ntl": entry_ntl,
                })
                if coin == "USDC":
                    spot_usd += total
                else:
                    spot_usd += entry_ntl if entry_ntl > 0 else 0
            except (TypeError, ValueError):
                continue
        profile["spot_balances"] = spot_balances
        profile["spot_usd"] = spot_usd

    profile["total_assets"] = profile["perp_equity"] + profile["spot_usd"]
    if profile["total_assets"] > 0:
        profile["withdraw_pct"] = profile["withdrawable"] / profile["total_assets"]

    if isinstance(portfolio_raw, list):
        blocks = {k: v for k, v in portfolio_raw if isinstance(k, str)}
        all_block = blocks.get("allTime") or blocks.get("perpAllTime") or {}
        pnl_hist = all_block.get("pnlHistory") or []
        if len(pnl_hist) >= 2:
            end_ts = int(pnl_hist[-1][0])
            pnl_now = float(pnl_hist[-1][1])
            profile["pnl"]["total"] = pnl_now
            for key, hours in (("h24", 24), ("h48", 48)):
                start = _pnl_at_hist(pnl_hist, end_ts - hours * HOUR_MS)
                if start is not None:
                    profile["pnl"][key] = pnl_now - start
            start7 = _pnl_at_hist(pnl_hist, end_ts - 7 * DAY_MS)
            start30 = _pnl_at_hist(pnl_hist, end_ts - 30 * DAY_MS)
            if start7 is not None:
                profile["pnl"]["d7"] = pnl_now - start7
            if start30 is not None:
                profile["pnl"]["d30"] = pnl_now - start30
            step = max(1, len(pnl_hist) // 120)
            profile["chart"] = [[int(pt[0]), float(pt[1])] for pt in pnl_hist[::step]]

    if isinstance(orders_raw, list):
        orders: list[dict[str, Any]] = []
        for o in orders_raw:
            if not isinstance(o, dict):
                continue
            side = "賣" if str(o.get("side", "")).upper() == "A" else "買"
            try:
                orders.append({
                    "coin": str(o.get("coin", "")),
                    "side": side,
                    "px": float(o.get("limitPx", 0) or 0),
                    "sz": float(o.get("sz", 0) or 0),
                    "oid": int(o.get("oid", 0) or 0),
                    "type": str(o.get("orderType", "Limit")),
                    "reduce_only": bool(o.get("reduceOnly")),
                })
            except (TypeError, ValueError):
                continue
        profile["orders"] = orders

    if isinstance(fills_raw, list):
        fills: list[dict[str, Any]] = []
        for fill in fills_raw[:150]:
            if not isinstance(fill, dict):
                continue
            try:
                fills.append({
                    "coin": str(fill.get("coin", "")),
                    "direction": format_direction_zh(str(fill.get("dir", ""))),
                    "px": float(fill.get("px", 0) or 0),
                    "sz": float(fill.get("sz", 0) or 0),
                    "time": int(fill.get("time", 0) or 0),
                    "closed_pnl": float(fill.get("closedPnl", 0) or 0),
                    "fee": float(fill.get("fee", 0) or 0),
                })
            except (TypeError, ValueError):
                continue
        profile["fills"] = fills

    if isinstance(ledger_raw, list):
        ledger: list[dict[str, Any]] = []
        for entry in ledger_raw[:100]:
            if not isinstance(entry, dict):
                continue
            delta = entry.get("delta") or {}
            if not isinstance(delta, dict):
                continue
            kind = str(delta.get("type", ""))
            amount = 0.0
            for key in ("usdc", "amount", "netWithdrawnUsd", "requestedUsd"):
                if key in delta:
                    try:
                        amount = float(delta[key])
                        break
                    except (TypeError, ValueError):
                        continue
            ledger.append({
                "time": int(entry.get("time", 0) or 0),
                "hash": str(entry.get("hash", ""))[:10],
                "type": _ledger_type_zh(kind),
                "amount": amount,
            })
        profile["ledger"] = ledger

    time.sleep(0.05)
    return profile


def _ledger_type_zh(kind: str) -> str:
    mapping = {
        "deposit": "充值",
        "withdraw": "提現",
        "internalTransfer": "內部轉帳",
        "accountClassTransfer": "帳戶轉移",
        "spotTransfer": "現貨轉移",
        "vaultDeposit": "金庫存入",
        "vaultWithdraw": "金庫提取",
        "subAccountTransfer": "子帳戶轉移",
    }
    return mapping.get(kind, kind or "—")


def fetch_profiles_batch(addresses: list[str], workers: int = 12) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    if not addresses:
        return profiles
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_address_profile, addr): addr for addr in addresses}
        for future in as_completed(futures):
            addr = futures[future]
            try:
                profiles[addr.lower()] = future.result()
            except Exception:
                profiles[addr.lower()] = {
                    "address": addr, "pnl": {}, "chart": [], "positions": [], "orders": [],
                    "fills": [], "ledger": [], "spot_balances": [],
                }
    return profiles


def load_profiles_cache(path: str) -> dict[str, dict[str, Any]]:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_profiles_cache(path: str, profiles: dict[str, dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(profiles, fh, ensure_ascii=False)


def _addr_link(address: str, platform: str = "Hyperliquid") -> str:
    short = f"{address[:6]}...{address[-4:]}"
    return (
        f'<button type="button" class="addr-btn" data-address="{html.escape(address)}" '
        f'title="{html.escape(address)}">{html.escape(short)}</button>'
    )


def _fmt_px(val: float | None) -> str:
    if val is None or val <= 0:
        return "—"
    return f"{val:,.4f}"


def _fmt_px_range(lo: float, hi: float) -> str:
    if lo <= 0 and hi <= 0:
        return "—"
    if hi <= 0 or abs(lo - hi) < 1e-12:
        return _fmt_px(lo if lo > 0 else hi)
    return f"{lo:,.4f} – {hi:,.4f}"


def fetch_candles_chart(coin: str, interval: str = "4h", bars: int = 72) -> list[list[float]]:
    end_ms = utc_now_ms()
    interval_ms = 4 * HOUR_MS if interval == "4h" else HOUR_MS
    start_ms = end_ms - bars * interval_ms
    try:
        raw = post_info({
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms},
        })
    except Exception:
        return []
    candles: list[list[float]] = []
    if isinstance(raw, list):
        for c in raw:
            if not isinstance(c, dict):
                continue
            t = int(c.get("t", 0) or 0)
            if t <= 0:
                continue
            try:
                candles.append([
                    float(t),
                    float(c.get("o", 0) or 0),
                    float(c.get("h", 0) or 0),
                    float(c.get("l", 0) or 0),
                    float(c.get("c", 0) or 0),
                ])
            except (TypeError, ValueError):
                continue
    candles.sort(key=lambda x: x[0])
    time.sleep(0.03)
    return candles[-bars:]


def _cluster_order_levels(items: list[tuple[float, str]]) -> list[dict[str, Any]]:
    counts: dict[tuple[str, float], int] = {}
    for px, kind in items:
        if px <= 0:
            continue
        key = (kind, round(px, 8))
        counts[key] = counts.get(key, 0) + 1
    levels = [{"kind": k, "price": p, "count": c} for (k, p), c in counts.items()]
    levels.sort(key=lambda x: x["price"])
    return levels


def build_orders_chart_data(
    qualified: list[TraderFills],
    profiles: dict[str, dict[str, Any]],
    mids: dict[str, float],
    fetch_candles: bool = True,
) -> dict[str, Any]:
    coin_levels: dict[str, list[tuple[float, str]]] = {}
    coin_accounts: dict[str, set[str]] = {}
    total_orders = 0

    for item in qualified:
        t = item.trader
        profile = profiles.get(t.address.lower()) or {}
        entry_by_coin = _position_entry_map(profile)
        orders_by_coin: dict[str, list[dict[str, Any]]] = {}
        for o in profile.get("orders") or []:
            if not isinstance(o, dict):
                continue
            coin_raw = str(o.get("coin", ""))
            if not coin_raw:
                continue
            orders_by_coin.setdefault(coin_raw, []).append(o)

        for coin_raw, orders in orders_by_coin.items():
            total_orders += len(orders)
            coin_accounts.setdefault(coin_raw, set()).add(t.address.lower())
            levels = coin_levels.setdefault(coin_raw, [])
            entry_px = entry_by_coin.get(coin_raw)
            if entry_px and entry_px > 0:
                levels.append((entry_px, "entry"))
            for o in orders:
                side = str(o.get("side", ""))
                try:
                    px = float(o.get("px", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if px <= 0:
                    continue
                if side == "買":
                    levels.append((px, "buy"))
                elif side == "賣":
                    levels.append((px, "sell"))

    coins = sorted(
        coin_levels.keys(),
        key=lambda c: (-len(coin_accounts.get(c, set())), c),
    )

    by_coin: dict[str, dict[str, Any]] = {}
    candle_map: dict[str, list[list[float]]] = {}
    if fetch_candles and coins:
        print(f"  Fetching 4H candles for order charts ({len(coins)} coins)...")
        with ThreadPoolExecutor(max_workers=8) as pool:
            candle_futures = {pool.submit(fetch_candles_chart, coin): coin for coin in coins}
            for future in as_completed(candle_futures):
                coin = candle_futures[future]
                try:
                    candle_map[coin] = future.result()
                except Exception:
                    candle_map[coin] = []

    for coin in coins:
        order_count = 0
        for addr in coin_accounts.get(coin, set()):
            prof = profiles.get(addr) or {}
            order_count += sum(
                1 for o in (prof.get("orders") or [])
                if isinstance(o, dict) and str(o.get("coin", "")) == coin
            )
        by_coin[coin] = {
            "mark": mids.get(coin, 0.0) or 0.0,
            "accounts": len(coin_accounts.get(coin, set())),
            "orders": order_count,
            "levels": _cluster_order_levels(coin_levels.get(coin, [])),
            "candles": candle_map.get(coin, []) if fetch_candles else [],
        }

    return {
        "coins": coins,
        "by_coin": by_coin,
        "stats": {
            "total_orders": total_orders,
            "coin_count": len(coins),
            "account_count": len(qualified),
        },
    }


def _orders_chart_panel(chart_data: dict[str, Any]) -> str:
    coins = chart_data.get("coins") or []
    stats = chart_data.get("stats") or {}
    if not coins:
        return """
        <p class="count">無當前委託</p>"""

    tabs: list[str] = []
    for i, coin in enumerate(coins):
        acct_n = (chart_data.get("by_coin") or {}).get(coin, {}).get("accounts", 0)
        active = " active" if i == 0 else ""
        tabs.append(
            f'<button type="button" class="order-coin-btn{active}" data-coin="{html.escape(coin)}">'
            f'{html.escape(coin)} <span class="coin-acct">{acct_n}</span></button>'
        )

    return f"""
        <p class="count" style="margin-bottom:8px">當前委託 · {stats.get("coin_count", 0)} 個標的（共 {stats.get("total_orders", 0)} 筆 · {stats.get("account_count", 0)} 個帳號）</p>
        <div class="order-coin-tabs">{"".join(tabs)}</div>
        <div class="order-chart-head">
          <strong id="order-chart-title">{html.escape(coins[0])}</strong>
          <div class="order-zoom-bar">
            <button type="button" class="order-zoom-btn" id="order-zoom-out" title="縮小">−</button>
            <input type="range" id="order-zoom-range" min="1" max="100" value="35" title="價格縮放">
            <button type="button" class="order-zoom-btn" id="order-zoom-in" title="放大">+</button>
            <button type="button" class="order-zoom-btn" id="order-zoom-focus">重設</button>
          </div>
          <span class="order-legend">
            <span class="lg-entry">━ 開倉價</span>
            <span class="lg-buy">━ 買單</span>
            <span class="lg-sell">━ 賣單</span>
            <span class="lg-mark">━ 現價</span>
          </span>
        </div>
        <div class="order-chart-wrap" id="order-chart-wrap">
          <svg id="order-chart-svg" viewBox="0 0 960 420" preserveAspectRatio="none"></svg>
        </div>
        <p class="count" style="margin-top:8px">4H K 線 · 僅顯示現價 ±50% 範圍內委託 · 滾輪可再縮放</p>"""


def _consensus_rows(items: list[ConsensusTarget], mids: dict[str, float], window: str) -> str:
    rows: list[str] = []
    for i, c in enumerate(items, start=1):
        mark = mids.get(c.coin)
        rows.append(
            f"<tr data-window=\"{html.escape(window)}\">"
            f"<td>{i}</td>"
            f"<td>{html.escape(window)}</td>"
            f"<td><strong>{html.escape(c.coin)}</strong></td>"
            f"<td data-sort='{c.account_count}' class='wr'>{c.account_count}</td>"
            f"<td>{_dir_badge(c.consensus_direction) if c.consensus_direction not in ('Mixed', '—') else html.escape(format_direction_zh(c.consensus_direction))}</td>"
            f"<td data-sort='{c.net_ratio:.6f}'>{c.net_ratio:.0%}</td>"
            f"<td data-sort='{c.long_accounts}'>{c.long_accounts}</td>"
            f"<td data-sort='{c.short_accounts}'>{c.short_accounts}</td>"
            f"<td data-sort='{c.avg_entry_px:.8f}'>{c.avg_entry_px:,.4f}</td>"
            f"<td data-sort='{mark or 0}'>{_fmt_px(mark)}</td>"
            f"</tr>"
        )
    return "\n".join(rows)


def _unified_consensus_section(
    consensus_24h: list[ConsensusTarget],
    consensus_4h: list[ConsensusTarget],
    records_24h: list[OpenRecord],
    records_4h: list[OpenRecord],
    mids: dict[str, float],
    orders_chart_panel: str,
) -> str:
    consensus_body = _consensus_rows(consensus_24h, mids, "24H") + _consensus_rows(consensus_4h, mids, "4H")
    if not consensus_body:
        consensus_body = "<tr><td colspan='10'>無共識標的</td></tr>"

    trade_items: list[tuple[OpenRecord, str]] = []
    for r in records_24h:
        trade_items.append((r, "24H"))
    for r in records_4h:
        trade_items.append((r, "4H"))
    trade_items.sort(key=lambda x: x[0].open_ts, reverse=True)

    def unified_trade_rows() -> str:
        rows: list[str] = []
        for r, window in trade_items:
            name = html.escape(r.display_name or "—")
            mark = mids.get(r.coin)
            rows.append(
                f"<tr data-window=\"{html.escape(window)}\">"
                f"<td>{html.escape(window)}</td>"
                f"<td>{r.rank}</td>"
                f"<td>{html.escape(r.platform)}</td>"
                f"<td>{_addr_link(r.address, r.platform)}</td>"
                f"<td>{name}</td>"
                f"<td><strong>{html.escape(r.coin)}</strong></td>"
                f"<td>{_dir_badge(r.direction)}</td>"
                f"<td data-sort='{r.avg_entry_px:.8f}'>{r.avg_entry_px:,.4f}</td>"
                f"<td data-sort='{mark or 0}'>{_fmt_px(mark)}</td>"
                f"<td data-sort='{r.open_ts}'>{html.escape(r.open_time)}</td>"
                f"<td>{r.fill_count}</td>"
                f"</tr>"
            )
        return "\n".join(rows) if rows else "<tr><td colspan='11'>無成交紀錄</td></tr>"

    total_consensus = len(consensus_24h) + len(consensus_4h)
    total_trades = len(records_24h) + len(records_4h)
    return f"""
  <div id="consensus" class="panel active">
    <div class="subtabs">
      <button type="button" class="subtab active" data-subpanel="consensus-targets">共識標的</button>
      <button type="button" class="subtab" data-subpanel="consensus-trades">成交紀錄</button>
      <button type="button" class="subtab" data-subpanel="consensus-orders">當前委託</button>
    </div>
    <div class="win-filter">
      <span class="count">時間窗：</span>
      <button type="button" class="filter-btn active" data-filter="all">全部</button>
      <button type="button" class="filter-btn" data-filter="24H">24H</button>
      <button type="button" class="filter-btn" data-filter="4H">4H</button>
    </div>
    <div class="subpanels">
      <div id="consensus-targets" class="sub-panel active">
        <p class="count" style="margin-bottom:8px">共識標的 · {total_consensus} 個（24H {len(consensus_24h)} · 4H {len(consensus_4h)}）</p>
        <div class="table-wrap">
          <table class="sortable" id="consensus-table">
            <thead><tr>
              <th>#</th><th>時間窗</th><th>標的</th><th>帳號數</th><th>共識方向</th>
              <th>淨比例</th><th>做多</th><th>做空</th><th>平均開倉價</th><th>現價</th>
            </tr></thead>
            <tbody>{consensus_body}</tbody>
          </table>
        </div>
      </div>
      <div id="consensus-trades" class="sub-panel">
        <p class="count" style="margin-bottom:8px">成交紀錄 · {total_trades} 筆</p>
        <div class="table-wrap">
          <table class="sortable" id="trades-table">
            <thead><tr>
              <th>時間窗</th><th>排名</th><th>平台</th><th>地址</th><th>名稱</th><th>標的</th>
              <th>方向</th><th>平均成交價</th><th>現價</th><th>成交時間 (UTC)</th><th>成交筆數</th>
            </tr></thead>
            <tbody>{unified_trade_rows()}</tbody>
          </table>
        </div>
      </div>
      <div id="consensus-orders" class="sub-panel">
        {orders_chart_panel}
      </div>
    </div>
  </div>"""


def _direction_wr_rows(items: list[DirectionWrRank]) -> str:
    rows: list[str] = []
    for item in items:
        badge = _dir_badge(item.side)
        rows.append(
            f"<tr>"
            f"<td>{item.rank}</td>"
            f"<td><strong>{html.escape(item.coin)}</strong></td>"
            f"<td>{badge}</td>"
            f"<td data-sort='{item.account_count}'>{item.account_count}</td>"
            f"<td data-sort='{item.avg_account_wr:.6f}' class='wr'>{item.avg_account_wr:.1%}</td>"
            f"</tr>"
        )
    return "\n".join(rows)


def _direction_wr_panel_toggle(
    panel_id: str,
    desc: list[DirectionWrRank],
    asc: list[DirectionWrRank],
    side: str,
    window: str,
) -> str:
    side_label = "做多" if side == "Long" else "做空"
    count = max(len(desc), len(asc))
    return f"""
  <div id="{panel_id}" class="sub-panel">
    <div class="panel-head">
      <p class="count">{html.escape(window)} {side_label}帳號平均勝率 · {count} 個標的</p>
      <button type="button" class="sort-toggle active-desc" data-target="{panel_id}" aria-pressed="true">勝率 高→低</button>
    </div>
    <div class="table-wrap">
      <table class="sortable">
        <thead><tr>
          <th>#</th><th>標的</th><th>方向</th><th>帳號數</th><th>平均勝率</th>
        </tr></thead>
        <tbody class="wr-body wr-desc active">{_direction_wr_rows(desc)}</tbody>
        <tbody class="wr-body wr-asc">{_direction_wr_rows(asc)}</tbody>
      </table>
    </div>
  </div>"""


def write_html_report(
    path: str,
    qualified: list[TraderFills],
    records_4h: list[OpenRecord],
    records_24h: list[OpenRecord],
    consensus_4h: list[ConsensusTarget],
    consensus_24h: list[ConsensusTarget],
    min_year_roi: float = MIN_YEAR_ROI,
    profiles: dict[str, dict[str, Any]] | None = None,
    fetch_profiles: bool = True,
) -> None:
    generated = utc_str(utc_now_ms())
    info_hosts = ", ".join(u.replace("https://", "") for u in _info_urls)
    mids = fetch_mid_prices(refresh=True)
    out_dir = os.path.dirname(path) or "."
    cache_path = os.path.join(out_dir, PROFILES_CACHE_FILE)

    if profiles is None and fetch_profiles and qualified:
        addresses = [q.trader.address for q in qualified]
        print(f"  Fetching wallet profiles ({len(addresses)})...")
        profiles = fetch_profiles_batch(addresses, workers=12)
        save_profiles_cache(cache_path, profiles)
    elif profiles is None:
        profiles = load_profiles_cache(cache_path)
    profiles = profiles or {}
    profiles_json = json.dumps(profiles, ensure_ascii=False).replace("</", "<\\/")
    mids_json = json.dumps(mids, ensure_ascii=False).replace("</", "<\\/")

    account_rows: list[str] = []
    for i, item in enumerate(qualified, start=1):
        t = item.trader
        name = html.escape(t.display_name or "—")
        account_rows.append(
            f"<tr>"
            f"<td>{i}</td>"
            f"<td>{html.escape(t.platform)}</td>"
            f"<td>{_addr_link(t.address, t.platform)}</td>"
            f"<td>{name}</td>"
            f"<td data-sort='{t.year_roi:.6f}' class='wr'>{_fmt_pct(t.year_roi)}</td>"
            f"<td data-sort='{t.history_days}'>{t.history_days}</td>"
            f"<td data-sort='{t.account_value:.2f}'>{_fmt_usd(t.account_value)}</td>"
            f"<td data-sort='{t.day_pnl:.2f}'>{_fmt_usd(t.day_pnl)}</td>"
            f"<td data-sort='{t.day_roi:.6f}'>{_fmt_pct(t.day_roi)}</td>"
            f"<td data-sort='{t.day_volume:.2f}'>{_fmt_usd(t.day_volume)}</td>"
            f"<td data-sort='{t.week_roi:.6f}'>{_fmt_pct(t.week_roi)}</td>"
            f"</tr>"
        )

    orders_chart_data = build_orders_chart_data(qualified, profiles, mids)
    orders_chart_json = json.dumps(orders_chart_data, ensure_ascii=False).replace("</", "<\\/")
    orders_chart_panel = _orders_chart_panel(orders_chart_data)

    consensus_section = _unified_consensus_section(
        consensus_24h, consensus_4h, records_24h, records_4h, mids,
        orders_chart_panel,
    )

    body = _build_report_html(
        generated, info_hosts, min_year_roi, qualified, records_24h, records_4h,
        consensus_24h, consensus_4h, account_rows, consensus_section, profiles_json, mids_json,
        orders_chart_json,
    )

    with open(path, "w", encoding="utf-8") as fh:
        fh.write(body)


def _build_report_html(
    generated: str,
    info_hosts: str,
    min_year_roi: float,
    qualified: list[TraderFills],
    records_24h: list[OpenRecord],
    records_4h: list[OpenRecord],
    consensus_24h: list[ConsensusTarget],
    consensus_4h: list[ConsensusTarget],
    account_rows: list[str],
    consensus_section: str,
    profiles_json: str,
    mids_json: str,
    orders_chart_json: str,
) -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Perp 高 ROI 帳號報告</title>
<style>
  :root {{
    --bg: #0b0f14; --card: #131a22; --border: #1e2a38;
    --text: #e8edf2; --muted: #8b9cb3;
    --accent: #00d4aa; --long: #22c55e; --short: #ef4444;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: "Segoe UI", system-ui, sans-serif; background: var(--bg); color: var(--text); line-height: 1.5; }}
  .wrap {{ max-width: 1400px; margin: 0 auto; padding: 24px 16px 48px; }}
  h1 {{ font-size: 1.5rem; margin-bottom: 4px; }}
  .sub {{ color: var(--muted); font-size: 0.875rem; margin-bottom: 20px; }}
  .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }}
  .stat {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }}
  .stat .label {{ color: var(--muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; }}
  .stat .value {{ font-size: 1.4rem; font-weight: 700; color: var(--accent); margin-top: 4px; }}
  .toolbar {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-bottom: 12px; }}
  .tabs, .subtabs {{ display: flex; flex-wrap: wrap; gap: 6px; }}
  .tab, .subtab, .sort-toggle {{
    background: var(--card); border: 1px solid var(--border); color: var(--muted);
    padding: 8px 16px; border-radius: 8px; cursor: pointer; font-size: 0.875rem;
  }}
  .tab.active, .subtab.active {{ background: var(--accent); color: #000; border-color: var(--accent); font-weight: 600; }}
  .sort-toggle {{ padding: 6px 14px; font-size: 0.8125rem; margin-left: auto; }}
  .sort-toggle.active-desc {{ border-color: var(--long); color: var(--long); }}
  .sort-toggle.active-asc {{ border-color: var(--short); color: var(--short); }}
  #search {{ flex: 1; min-width: 200px; background: var(--card); border: 1px solid var(--border); color: var(--text); padding: 8px 12px; border-radius: 8px; font-size: 0.875rem; }}
  .win-panel {{ display: none; }}
  .win-panel.active {{ display: block; }}
  .sub-panel {{ display: none; }}
  .sub-panel.active {{ display: block; }}
  .panel-head {{ display: flex; flex-wrap: wrap; align-items: center; gap: 10px; margin-bottom: 8px; }}
  .panel-head .count {{ margin: 0; flex: 1; }}
  .subtabs {{ margin-bottom: 12px; }}
  tbody.wr-body {{ display: none; }}
  tbody.wr-body.active {{ display: table-row-group; }}
  .panel {{ display: none; }}
  .panel.active {{ display: block; }}
  .table-wrap {{ overflow-x: auto; border: 1px solid var(--border); border-radius: 10px; background: var(--card); }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.8125rem; }}
  th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--border); white-space: nowrap; }}
  th {{ background: #0f151c; color: var(--muted); font-weight: 600; cursor: pointer; user-select: none; position: sticky; top: 0; }}
  th:hover {{ color: var(--accent); }}
  tr:hover td {{ background: rgba(0,212,170,0.05); }}
  a {{ color: var(--accent); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; }}
  .badge.long {{ background: rgba(34,197,94,0.15); color: var(--long); }}
  .badge.short {{ background: rgba(239,68,68,0.15); color: var(--short); }}
  .badge.neutral {{ background: rgba(139,156,179,0.15); color: var(--muted); }}
  .wr {{ color: var(--accent); font-weight: 600; }}
  .count {{ color: var(--muted); font-size: 0.8125rem; }}
  .addr-btn {{ background: none; border: none; color: var(--accent); cursor: pointer; font: inherit; padding: 0; }}
  .addr-btn:hover {{ text-decoration: underline; }}
  .filter-btn {{
    background: var(--card); border: 1px solid var(--border); color: var(--muted);
    padding: 6px 12px; border-radius: 8px; cursor: pointer; font-size: 0.8125rem;
  }}
  .filter-btn.active {{ border-color: var(--accent); color: var(--accent); }}
  .win-filter {{ display: flex; gap: 8px; align-items: center; margin-bottom: 12px; flex-wrap: wrap; }}
  .cg-tab-panel {{ display: none; }}
  .cg-tab-panel.active {{ display: block; }}
  .cg-modal {{ position: fixed; inset: 0; z-index: 9999; display: flex; align-items: flex-start; justify-content: center; padding: 24px 12px; overflow-y: auto; }}
  .cg-modal.hidden {{ display: none; }}
  .cg-backdrop {{ position: fixed; inset: 0; background: rgba(0,0,0,0.72); }}
  .cg-panel {{ position: relative; width: min(1200px, 100%); background: #0d1117; border: 1px solid #21262d; border-radius: 12px; margin-top: 20px; }}
  .cg-header {{ display: flex; align-items: center; gap: 10px; padding: 16px 20px; border-bottom: 1px solid #21262d; flex-wrap: wrap; }}
  .cg-header .addr-full {{ font-family: ui-monospace, monospace; font-size: 0.875rem; word-break: break-all; }}
  .cg-icon-btn {{ background: #161b22; border: 1px solid #30363d; color: #8b949e; border-radius: 6px; padding: 6px 10px; cursor: pointer; font-size: 0.8125rem; }}
  .cg-icon-btn:hover {{ color: #58a6ff; border-color: #58a6ff; }}
  .cg-body {{ padding: 16px 20px 24px; }}
  .cg-layout {{ display: grid; grid-template-columns: 180px 1fr; gap: 16px; }}
  @media (max-width: 900px) {{ .cg-layout {{ grid-template-columns: 1fr; }} }}
  .cg-pnl-stack {{ display: flex; flex-direction: column; gap: 8px; }}
  .cg-pnl-card {{ background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 12px; }}
  .cg-pnl-card .lbl {{ color: #8b949e; font-size: 0.75rem; }}
  .cg-pnl-card .val {{ font-size: 1.1rem; font-weight: 700; margin-top: 4px; }}
  .cg-pnl-card .val.pos {{ color: #3fb950; }}
  .cg-pnl-card .val.neg {{ color: #f85149; }}
  .cg-chart-wrap {{ background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 12px; margin-bottom: 12px; height: 220px; }}
  .cg-chart-wrap svg {{ width: 100%; height: 190px; }}
  .cg-cards {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 12px; }}
  @media (max-width: 800px) {{ .cg-cards {{ grid-template-columns: 1fr; }} }}
  .cg-card {{ background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 14px; }}
  .cg-card .title {{ color: #8b949e; font-size: 0.75rem; }}
  .cg-card .big {{ font-size: 1.25rem; font-weight: 700; margin: 6px 0; }}
  .cg-card .meta {{ color: #8b949e; font-size: 0.75rem; line-height: 1.6; }}
  .cg-tabs {{ display: flex; gap: 6px; margin-bottom: 10px; flex-wrap: wrap; }}
  .cg-tab {{ background: #161b22; border: 1px solid #30363d; color: #8b949e; padding: 8px 14px; border-radius: 8px; cursor: pointer; font-size: 0.8125rem; }}
  .cg-tab.active {{ background: #238636; color: #fff; border-color: #238636; }}
  .cg-table-wrap {{ overflow-x: auto; border: 1px solid #21262d; border-radius: 8px; }}
  .cg-table-wrap table {{ font-size: 0.8125rem; }}
  .cg-table-wrap th {{ background: #0d1117; cursor: default; }}
  .cg-dir {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; border: 1px solid; }}
  .cg-dir.long {{ color: #3fb950; border-color: #3fb950; background: rgba(63,185,80,0.1); }}
  .cg-dir.short {{ color: #f85149; border-color: #f85149; background: rgba(248,81,73,0.1); }}
  .order-coin-tabs {{ display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; max-height: 120px; overflow-y: auto; }}
  .order-coin-btn {{
    background: var(--card); border: 1px solid var(--border); color: var(--muted);
    padding: 6px 10px; border-radius: 8px; cursor: pointer; font-size: 0.8125rem;
  }}
  .order-coin-btn.active {{ border-color: var(--accent); color: var(--accent); background: rgba(0,212,170,0.08); }}
  .order-coin-btn .coin-acct {{ opacity: 0.65; font-size: 0.75rem; margin-left: 4px; }}
  .order-chart-head {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 8px; flex-wrap: wrap; }}
  .order-zoom-bar {{ display: flex; align-items: center; gap: 6px; }}
  .order-zoom-btn {{
    background: var(--card); border: 1px solid var(--border); color: var(--text);
    width: 28px; height: 28px; border-radius: 6px; cursor: pointer; font-size: 0.9rem;
  }}
  .order-zoom-btn:not([id^="order-zoom-"]) {{ width: auto; padding: 0 10px; font-size: 0.75rem; }}
  #order-zoom-focus {{ width: auto; padding: 0 10px; font-size: 0.75rem; }}
  .order-zoom-btn:hover {{ border-color: var(--accent); color: var(--accent); }}
  #order-zoom-range {{ width: 110px; accent-color: var(--accent); cursor: pointer; }}
  .order-legend {{ display: flex; gap: 14px; font-size: 0.75rem; color: var(--muted); flex-wrap: wrap; }}
  .lg-entry {{ color: #fbbf24; }}
  .lg-buy {{ color: var(--long); }}
  .lg-sell {{ color: var(--short); }}
  .lg-mark {{ color: var(--accent); }}
  .order-chart-wrap {{
    background: #161b22; border: 1px solid #21262d; border-radius: 8px;
    padding: 8px 8px 4px; height: 420px; cursor: grab; touch-action: none;
  }}
  .order-chart-wrap.panning {{ cursor: grabbing; }}
  .order-chart-wrap svg {{ width: 100%; height: 400px; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>Perp 活躍高 ROI 帳號</h1>
  <p class="sub">產生時間：{html.escape(generated)} · Hyperliquid 免費 API · Info: {html.escape(info_hosts)} · 活躍：30D≥{MIN_FILLS_30D}筆 · 帳齡≥{MIN_HISTORY_DAYS}D · ROI&gt;{min_year_roi:.0%} · 最大回撤&lt;{MAX_PEAK_DRAWDOWN:.0%} · 點擊地址查看詳情</p>

  <div class="stats">
    <div class="stat"><div class="label">帳號總數</div><div class="value">{len(qualified)}</div></div>
    <div class="stat"><div class="label">24H 成交</div><div class="value">{len(records_24h)}</div></div>
    <div class="stat"><div class="label">24H 共識</div><div class="value">{len(consensus_24h)}</div></div>
    <div class="stat"><div class="label">4H 共識</div><div class="value">{len(consensus_4h)}</div></div>
  </div>

  <div class="toolbar">
    <div class="tabs main-tabs">
      <button type="button" class="tab active" data-panel="consensus">共識</button>
      <button type="button" class="tab" data-panel="accounts">帳號排名</button>
    </div>
    <input id="search" type="search" placeholder="搜尋標的、地址...">
  </div>

{consensus_section}

  <div id="accounts" class="panel">
    <p class="count" style="margin-bottom:8px">共 {len(qualified)} 個帳號 · 點地址開啟詳情</p>
    <div class="table-wrap">
      <table class="sortable">
        <thead><tr>
          <th>#</th><th>平台</th><th>地址</th><th>名稱</th><th>ROI</th><th>帳齡(D)</th>
          <th>帳戶價值</th><th>日PnL</th><th>日ROI</th><th>日成交量</th><th>週ROI</th>
        </tr></thead>
        <tbody>{"".join(account_rows)}</tbody>
      </table>
    </div>
  </div>
</div>

<div id="profile-modal" class="cg-modal hidden" aria-hidden="true">
  <div class="cg-backdrop" id="cg-close-backdrop"></div>
  <div class="cg-panel">
    <div class="cg-header">
      <span style="color:#8b949e">地址：</span>
      <span class="addr-full" id="cg-addr"></span>
      <button type="button" class="cg-icon-btn" id="cg-copy" title="複製">複製</button>
      <a class="cg-icon-btn" id="cg-explorer" target="_blank" rel="noopener">Explorer</a>
      <button type="button" class="cg-icon-btn" id="cg-close" style="margin-left:auto">關閉</button>
    </div>
    <div class="cg-body">
      <div class="cg-layout">
        <div class="cg-pnl-stack" id="cg-pnl-stack"></div>
        <div>
          <div class="cg-chart-wrap"><svg id="cg-chart" viewBox="0 0 800 190" preserveAspectRatio="none"></svg></div>
          <div class="cg-cards" id="cg-cards"></div>
          <div class="cg-tabs">
            <button type="button" class="cg-tab active" data-cgtab="pos">倉位</button>
            <button type="button" class="cg-tab" data-cgtab="fills">交易</button>
            <button type="button" class="cg-tab" data-cgtab="orders">當前委託(<span id="cg-ord-count">0</span>)</button>
            <button type="button" class="cg-tab" data-cgtab="ledger">充值 &amp; 提現</button>
            <button type="button" class="cg-tab" data-cgtab="spot">現貨持倉</button>
          </div>
          <div class="cg-tab-panel active" id="cg-tab-pos"><div class="cg-table-wrap"><table><thead><tr>
            <th>代幣</th><th>方向</th><th>槓桿</th><th>價值</th><th>數量</th><th>開倉價</th><th>盈虧</th><th>資金費</th><th>爆倉價</th>
          </tr></thead><tbody id="cg-pos-body"></tbody></table></div></div>
          <div class="cg-tab-panel" id="cg-tab-fills"><div class="cg-table-wrap"><table><thead><tr>
            <th>時間</th><th>代幣</th><th>方向</th><th>價格</th><th>數量</th><th>已實現PnL</th><th>手續費</th>
          </tr></thead><tbody id="cg-fill-body"></tbody></table></div></div>
          <div class="cg-tab-panel" id="cg-tab-orders"><div class="cg-table-wrap"><table><thead><tr>
            <th>代幣</th><th>方向</th><th>類型</th><th>開倉價</th><th>委託價</th><th>現價</th>
          </tr></thead><tbody id="cg-ord-body"></tbody></table></div></div>
          <div class="cg-tab-panel" id="cg-tab-ledger"><div class="cg-table-wrap"><table><thead><tr>
            <th>時間</th><th>類型</th><th>金額</th><th>Hash</th>
          </tr></thead><tbody id="cg-ledger-body"></tbody></table></div></div>
          <div class="cg-tab-panel" id="cg-tab-spot"><div class="cg-table-wrap"><table><thead><tr>
            <th>代幣</th><th>總量</th><th>凍結</th><th>成本</th>
          </tr></thead><tbody id="cg-spot-body"></tbody></table></div></div>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
const PROFILES = {profiles_json};
const MIDS = {mids_json};
const ORDER_CHARTS = {orders_chart_json};
let activeWindowFilter = 'all';
let activeOrderCoin = null;
let orderChartsReady = false;
const orderChartViews = {{}};
let orderChartPan = null;

const ORDER_CHART_PAD = {{ W: 960, H: 420, L: 12, R: 138, T: 18, B: 30 }};

function orderChartData(coin) {{
  return (ORDER_CHARTS.by_coin || {{}})[coin];
}}

function computeOrderFullRange(data) {{
  const candles = data.candles || [];
  const levels = data.levels || [];
  const mark = Number(data.mark) || 0;
  let yMin = Infinity, yMax = -Infinity;
  candles.forEach(c => {{ yMin = Math.min(yMin, c[3]); yMax = Math.max(yMax, c[2]); }});
  levels.forEach(l => {{ yMin = Math.min(yMin, l.price); yMax = Math.max(yMax, l.price); }});
  if (mark > 0) {{ yMin = Math.min(yMin, mark); yMax = Math.max(yMax, mark); }}
  if (!isFinite(yMin)) {{ yMin = 0; yMax = 1; }}
  const pad = (yMax - yMin) * 0.04 || Math.max(Math.abs(yMax), 1) * 0.02 || 1;
  return {{ yMin: yMin - pad, yMax: yMax + pad }};
}}

function computeOrderMarkBand(data) {{
  const mark = Number(data.mark) || 0;
  if (mark > 0) {{
    return {{ yMin: mark * 0.5, yMax: mark * 1.5, mark }};
  }}
  const candles = data.candles || [];
  const recent = candles.slice(-24);
  let rMin = Infinity, rMax = -Infinity;
  recent.forEach(c => {{ rMin = Math.min(rMin, c[3]); rMax = Math.max(rMax, c[2]); }});
  if (isFinite(rMin) && isFinite(rMax)) {{
    const mid = (rMin + rMax) / 2;
    return {{ yMin: mid * 0.5, yMax: mid * 1.5, mark: mid }};
  }}
  const full = computeOrderFullRange(data);
  const mid = (full.yMin + full.yMax) / 2;
  return {{ yMin: full.yMin, yMax: full.yMax, mark: mid }};
}}

function filterLevelsNearMark(data) {{
  const mark = Number(data.mark) || 0;
  const levels = data.levels || [];
  if (mark <= 0) return levels;
  const lo = mark * 0.5, hi = mark * 1.5;
  return levels.filter(l => l.price >= lo && l.price <= hi);
}}

function ensureOrderChartView(coin, data) {{
  if (!orderChartViews[coin]) {{
    const band = computeOrderMarkBand(data);
    orderChartViews[coin] = {{
      yMin: band.yMin, yMax: band.yMax,
      fullYMin: band.yMin, fullYMax: band.yMax,
      mark: band.mark,
      mode: 'mark50',
    }};
  }}
  return orderChartViews[coin];
}}

function clampOrderView(view) {{
  const span = view.yMax - view.yMin;
  const minSpan = (view.fullYMax - view.fullYMin) * 0.008 || span * 0.05 || 1e-9;
  if (span < minSpan) {{
    const c = (view.yMin + view.yMax) / 2;
    view.yMin = c - minSpan / 2;
    view.yMax = c + minSpan / 2;
  }}
  if (view.yMin < view.fullYMin) {{
    view.yMax += view.fullYMin - view.yMin;
    view.yMin = view.fullYMin;
  }}
  if (view.yMax > view.fullYMax) {{
    view.yMin -= view.yMax - view.fullYMax;
    view.yMax = view.fullYMax;
  }}
  if (view.yMin < view.fullYMin) view.yMin = view.fullYMin;
  if (view.yMax > view.fullYMax) view.yMax = view.fullYMax;
}}

function orderZoomFromSlider(val) {{
  const coin = activeOrderCoin;
  const data = orderChartData(coin);
  if (!data) return;
  const view = ensureOrderChartView(coin, data);
  const fullSpan = view.fullYMax - view.fullYMin;
  const t = Number(val) / 100;
  const span = fullSpan * (0.98 - t * 0.93);
  const anchor = Number(data.mark) > 0 ? Number(data.mark) : (view.yMin + view.yMax) / 2;
  view.yMin = anchor - span / 2;
  view.yMax = anchor + span / 2;
  view.mode = 'custom';
  clampOrderView(view);
  drawOrderChart(coin);
}}

function syncOrderZoomSlider(coin) {{
  const slider = document.getElementById('order-zoom-range');
  const data = orderChartData(coin);
  if (!slider || !data) return;
  const view = ensureOrderChartView(coin, data);
  const fullSpan = view.fullYMax - view.fullYMin;
  const span = view.yMax - view.yMin;
  const ratio = fullSpan > 0 ? span / fullSpan : 1;
  slider.value = String(Math.round(Math.max(1, Math.min(100, (0.98 - ratio) / 0.93 * 100))));
}}

function layoutOrderLabels(items, yScale, chartTop, chartBottom) {{
  const sorted = items.map(it => ({{ ...it, lineY: yScale(it.price) }}))
    .sort((a, b) => a.lineY - b.lineY);
  const minGap = 13;
  let lastY = chartTop - minGap;
  return sorted.map(it => {{
    let labelY = Math.max(chartTop + 8, Math.min(chartBottom - 4, it.lineY));
    if (labelY - lastY < minGap) labelY = lastY + minGap;
    lastY = labelY;
    return {{ ...it, labelY }};
  }});
}}

function activeViewRoot() {{
  const panel = document.querySelector('.panel.active');
  if (!panel) return document.getElementById('consensus');
  const sub = panel.querySelector('.sub-panel.active');
  return sub || panel;
}}

function fmtUsd(n) {{
  const v = Number(n) || 0;
  const abs = Math.abs(v);
  let s = abs >= 1e6 ? (abs/1e6).toFixed(2)+'M' : abs >= 1e3 ? (abs/1e3).toFixed(2)+'K' : abs.toFixed(4);
  return (v < 0 ? '-$' : '$') + s;
}}

function fmtPnlClass(v) {{ return Number(v) >= 0 ? 'pos' : 'neg'; }}

function fmtTime(ms) {{
  if (!ms) return '—';
  return new Date(ms).toISOString().replace('T', ' ').replace('.000Z', ' UTC');
}}

function fmtPx(v) {{
  const n = Number(v) || 0;
  if (n <= 0) return '—';
  return n.toLocaleString(undefined, {{ minimumFractionDigits: 4, maximumFractionDigits: 4 }});
}}

function orderSideBadge(side) {{
  const cls = side === '買' ? 'long' : side === '賣' ? 'short' : 'neutral';
  return `<span class="badge ${{cls}}">${{side}}</span>`;
}}

function orderTypeZh(t, reduceOnly) {{
  const map = {{
    'Limit': '限價', 'Market': '市價', 'Stop Market': '止損市價', 'Stop Limit': '止損限價',
    'Take Profit Market': '止盈市價', 'Take Profit Limit': '止盈限價', 'Stop': '止損',
  }};
  let label = map[t] || t || '—';
  if (reduceOnly) label += '·只減';
  return label;
}}

function applyWindowFilter(filter) {{
  activeWindowFilter = filter;
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.toggle('active', b.dataset.filter === filter));
  document.querySelectorAll('#consensus-table tbody tr, #trades-table tbody tr').forEach(row => {{
    const w = row.dataset.window || '';
    row.style.display = (filter === 'all' || w === filter) ? '' : 'none';
  }});
}}

function levelStyle(kind) {{
  if (kind === 'entry') return {{ color: '#fbbf24', dash: '7,5', label: '開倉' }};
  if (kind === 'buy') return {{ color: '#22c55e', dash: 'none', label: '買' }};
  return {{ color: '#ef4444', dash: 'none', label: '賣' }};
}}

function drawOrderChart(coin) {{
  const svg = document.getElementById('order-chart-svg');
  const title = document.getElementById('order-chart-title');
  if (!svg || !ORDER_CHARTS.by_coin) return;
  const data = ORDER_CHARTS.by_coin[coin];
  if (!data) return;
  activeOrderCoin = coin;
  if (title) title.textContent = coin;
  const view = ensureOrderChartView(coin, data);
  syncOrderZoomSlider(coin);

  const {{ W, H, L: padL, R: padR, T: padT, B: padB }} = ORDER_CHART_PAD;
  const chartW = W - padL - padR, chartH = H - padT - padB;
  const candles = data.candles || [];
  const levels = filterLevelsNearMark(data);
  const mark = Number(data.mark) || 0;
  const yMin = view.yMin, yMax = view.yMax;
  const yScale = p => padT + chartH - ((p - yMin) / (yMax - yMin)) * chartH;

  let out = `<rect x="0" y="0" width="${{W}}" height="${{H}}" fill="#161b22"/>`;
  const labelX = W - 8;
  const labelItems = [];
  for (let i = 0; i < 6; i++) {{
    const p = yMin + (yMax - yMin) * i / 5;
    const y = yScale(p);
    out += `<line x1="${{padL}}" y1="${{y}}" x2="${{padL + chartW}}" y2="${{y}}" stroke="#21262d" stroke-width="1"/>`;
    labelItems.push({{ kind: 'grid', price: p, count: 0, st: {{ color: '#8b949e', label: '' }}, lineY: y }});
  }}

  const n = Math.max(candles.length, 1);
  const step = chartW / n;
  const cw = Math.max(2, step * 0.62);
  candles.forEach((c, i) => {{
    const x = padL + i * step + step / 2;
    const o = c[1], h = c[2], l = c[3], cl = c[4];
    if (h < yMin && l > yMax) return;
    const up = cl >= o;
    const color = up ? '#22c55e' : '#ef4444';
    const yH = yScale(Math.min(h, yMax)), yL = yScale(Math.max(l, yMin));
    if (yL - yH < 0.5) return;
    out += `<line x1="${{x}}" y1="${{yH}}" x2="${{x}}" y2="${{yL}}" stroke="${{color}}" stroke-width="1"/>`;
    const top = Math.min(yScale(o), yScale(cl)), bot = Math.max(yScale(o), yScale(cl));
    out += `<rect x="${{x - cw / 2}}" y="${{top}}" width="${{cw}}" height="${{Math.max(1, bot - top)}}" fill="${{color}}"/>`;
  }});

  levels.forEach(l => {{
    if (l.price < yMin || l.price > yMax) return;
    const st = levelStyle(l.kind);
    const y = yScale(l.price);
    out += `<line x1="${{padL}}" y1="${{y}}" x2="${{padL + chartW}}" y2="${{y}}" stroke="${{st.color}}" stroke-width="1.5" stroke-dasharray="${{st.dash}}" opacity="0.9"/>`;
    labelItems.push({{ ...l, st, lineY: y }});
  }});

  if (mark > 0 && mark >= yMin && mark <= yMax) {{
    const y = yScale(mark);
    out += `<line x1="${{padL}}" y1="${{y}}" x2="${{padL + chartW}}" y2="${{y}}" stroke="#00d4aa" stroke-width="2"/>`;
    labelItems.push({{ kind: 'mark', price: mark, count: 0, st: {{ color: '#00d4aa', label: '現價' }}, lineY: y }});
  }}

  layoutOrderLabels(labelItems, yScale, padT, padT + chartH).forEach(it => {{
    const st = it.st;
    const marginX = padL + chartW;
    if (Math.abs(it.labelY - it.lineY) > 2) {{
      out += `<line x1="${{marginX}}" y1="${{it.lineY}}" x2="${{labelX - 4}}" y2="${{it.labelY - 4}}" stroke="${{st.color}}" stroke-width="1" opacity="0.55"/>`;
    }}
    const text = it.kind === 'grid'
      ? fmtPx(it.price)
      : `${{st.label}} ${{fmtPx(it.price)}}${{it.kind === 'mark' ? '' : ` ×${{it.count}}`}}`;
    const fs = it.kind === 'grid' ? 10 : 11;
    const fw = it.kind === 'mark' ? ' font-weight="700"' : '';
    out += `<text x="${{labelX}}" y="${{it.labelY}}" text-anchor="end" fill="${{st.color}}" font-size="${{fs}}"${{fw}}>${{text}}</text>`;
  }});

  if (!candles.length) {{
    out += `<text x="${{padL + 20}}" y="${{padT + 40}}" fill="#8b949e" font-size="13">無 K 線資料 · 仍顯示委託價位</text>`;
  }}

  svg.innerHTML = out;
}}

function zoomOrderChart(factor) {{
  const coin = activeOrderCoin;
  const data = orderChartData(coin);
  if (!data) return;
  const view = ensureOrderChartView(coin, data);
  const center = Number(data.mark) > 0 ? Number(data.mark) : (view.yMin + view.yMax) / 2;
  const span = (view.yMax - view.yMin) / factor;
  view.yMin = center - span / 2;
  view.yMax = center + span / 2;
  view.mode = 'custom';
  clampOrderView(view);
  drawOrderChart(coin);
}}

function panOrderChart(deltaYpx) {{
  const coin = activeOrderCoin;
  const data = orderChartData(coin);
  if (!data) return;
  const view = ensureOrderChartView(coin, data);
  const chartH = ORDER_CHART_PAD.H - ORDER_CHART_PAD.T - ORDER_CHART_PAD.B;
  const span = view.yMax - view.yMin;
  const deltaPrice = (deltaYpx / chartH) * span;
  view.yMin += deltaPrice;
  view.yMax += deltaPrice;
  view.mode = 'custom';
  clampOrderView(view);
  drawOrderChart(coin);
}}

function resetOrderChartFocus() {{
  const coin = activeOrderCoin;
  const data = orderChartData(coin);
  if (!data) return;
  const band = computeOrderMarkBand(data);
  orderChartViews[coin] = {{
    yMin: band.yMin, yMax: band.yMax,
    fullYMin: band.yMin, fullYMax: band.yMax,
    mark: band.mark,
    mode: 'mark50',
  }};
  drawOrderChart(coin);
}}

function initOrderCharts() {{
  const coins = ORDER_CHARTS.coins || [];
  if (!coins.length) return;

  if (!orderChartsReady) {{
    document.querySelectorAll('.order-coin-btn').forEach(btn => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.order-coin-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        drawOrderChart(btn.dataset.coin);
      }});
    }});

    const wrap = document.getElementById('order-chart-wrap');
    const zoomIn = document.getElementById('order-zoom-in');
    const zoomOut = document.getElementById('order-zoom-out');
    const zoomRange = document.getElementById('order-zoom-range');
    const zoomFocus = document.getElementById('order-zoom-focus');

    zoomIn?.addEventListener('click', () => zoomOrderChart(1.35));
    zoomOut?.addEventListener('click', () => zoomOrderChart(1 / 1.35));
    zoomRange?.addEventListener('input', e => orderZoomFromSlider(e.target.value));
    zoomFocus?.addEventListener('click', resetOrderChartFocus);

    wrap?.addEventListener('wheel', e => {{
      e.preventDefault();
      zoomOrderChart(e.deltaY < 0 ? 1.18 : 1 / 1.18);
    }}, {{ passive: false }});

    wrap?.addEventListener('mousedown', e => {{
      orderChartPan = {{ y: e.clientY }};
      wrap.classList.add('panning');
    }});
    window.addEventListener('mousemove', e => {{
      if (!orderChartPan) return;
      const dy = e.clientY - orderChartPan.y;
      if (Math.abs(dy) > 0) {{
        panOrderChart(dy);
        orderChartPan.y = e.clientY;
      }}
    }});
    window.addEventListener('mouseup', () => {{
      orderChartPan = null;
      wrap?.classList.remove('panning');
    }});

    orderChartsReady = true;
  }}

  const activeBtn = document.querySelector('.order-coin-btn.active');
  const coin = activeBtn?.dataset.coin || coins[0];
  drawOrderChart(coin);
}}

document.addEventListener('click', e => {{
  const t = e.target;
  if (t.classList.contains('addr-btn')) openProfile(t.dataset.address);
  if (t.classList.contains('filter-btn')) applyWindowFilter(t.dataset.filter);
}});

function openProfile(addr) {{
  const p = PROFILES[(addr||'').toLowerCase()] || {{
    address: addr, pnl: {{}}, chart: [], positions: [], orders: [], fills: [], ledger: [], spot_balances: []
  }};
  const modal = document.getElementById('profile-modal');
  document.getElementById('cg-addr').textContent = addr;
  document.getElementById('cg-explorer').href = 'https://app.hyperliquid.xyz/explorer/address/' + addr;
  const pnl = p.pnl || {{}};
  const labels = [['total','總盈虧'],['h24','24小時'],['h48','48小時'],['d7','7天'],['d30','30天']];
  document.getElementById('cg-pnl-stack').innerHTML = labels.map(([k,l]) => {{
    const v = pnl[k] || 0;
    return `<div class="cg-pnl-card"><div class="lbl">${{l}}</div><div class="val ${{fmtPnlClass(v)}}">${{fmtUsd(v)}}</div></div>`;
  }}).join('');
  document.getElementById('cg-cards').innerHTML = `
    <div class="cg-card"><div class="title">永續合約倉位價值</div><div class="big">${{fmtUsd(p.perp_value)}}</div>
      <div class="meta">倉位：${{p.position_count||0}}<br>槓桿：${{(p.leverage||0).toFixed(2)}}x</div></div>
    <div class="cg-card"><div class="title">總資產</div><div class="big">${{fmtUsd(p.total_assets)}}</div>
      <div class="meta">永續：${{fmtUsd(p.perp_equity)}}<br>現貨：${{fmtUsd(p.spot_usd)}}</div></div>
    <div class="cg-card"><div class="title">可用保證金</div><div class="big">${{fmtUsd(p.withdrawable)}}</div>
      <div class="meta">可提取：${{((p.withdraw_pct||0)*100).toFixed(2)}}%</div></div>`;
  drawChart(p.chart || []);
  document.getElementById('cg-pos-body').innerHTML = (p.positions||[]).map(row => {{
    const dir = row.side === 'Long' ? '多' : '空';
    const cls = row.side === 'Long' ? 'long' : 'short';
    const roe = (Number(row.roe||0)*100).toFixed(2);
    return `<tr><td>${{row.coin}}</td><td><span class="cg-dir ${{cls}}">${{dir}}</span></td>
      <td>${{Number(row.leverage).toFixed(0)}}X ${{row.lev_type==='cross'?'全倉':'逐倉'}}</td>
      <td>${{fmtUsd(row.value)}}</td><td>${{Number(row.size).toFixed(4)}}</td>
      <td>${{Number(row.entry_px).toFixed(4)}}</td>
      <td class="${{fmtPnlClass(row.upnl)}}">${{fmtUsd(row.upnl)}} (${{roe}}%)</td>
      <td class="${{fmtPnlClass(row.funding)}}">${{fmtUsd(row.funding)}}</td>
      <td>${{row.liq_px > 0 ? Number(row.liq_px).toFixed(4) : '—'}}</td></tr>`;
  }}).join('') || '<tr><td colspan="9">無倉位</td></tr>';
  document.getElementById('cg-fill-body').innerHTML = (p.fills||[]).map(f =>
    `<tr><td>${{fmtTime(f.time)}}</td><td>${{f.coin}}</td><td>${{f.direction}}</td>
     <td>${{Number(f.px).toFixed(4)}}</td><td>${{Number(f.sz).toFixed(4)}}</td>
     <td class="${{fmtPnlClass(f.closed_pnl)}}">${{fmtUsd(f.closed_pnl)}}</td>
     <td>${{fmtUsd(f.fee)}}</td></tr>`
  ).join('') || '<tr><td colspan="7">無交易</td></tr>';
  const orders = p.orders || [];
  const entryByCoin = {{}};
  (p.positions || []).forEach(row => {{
    const entry = Number(row.entry_px) || 0;
    if (entry > 0) entryByCoin[row.coin] = entry;
  }});
  document.getElementById('cg-ord-count').textContent = orders.length;
  document.getElementById('cg-ord-body').innerHTML = orders.map(o =>
    `<tr><td>${{o.coin}}</td><td>${{orderSideBadge(o.side)}}</td><td>${{orderTypeZh(o.type, o.reduce_only)}}</td>
     <td>${{fmtPx(entryByCoin[o.coin])}}</td>
     <td>${{Number(o.px).toFixed(4)}}</td><td>${{fmtPx(MIDS[o.coin])}}</td></tr>`
  ).join('') || '<tr><td colspan="6">無掛單</td></tr>';
  document.getElementById('cg-ledger-body').innerHTML = (p.ledger||[]).map(l =>
    `<tr><td>${{fmtTime(l.time)}}</td><td>${{l.type}}</td>
     <td class="${{fmtPnlClass(l.amount)}}">${{fmtUsd(l.amount)}}</td><td>${{l.hash||'—'}}</td></tr>`
  ).join('') || '<tr><td colspan="4">無紀錄</td></tr>';
  document.getElementById('cg-spot-body').innerHTML = (p.spot_balances||[]).map(s =>
    `<tr><td>${{s.coin}}</td><td>${{Number(s.total).toFixed(6)}}</td>
     <td>${{Number(s.hold).toFixed(6)}}</td><td>${{fmtUsd(s.entry_ntl)}}</td></tr>`
  ).join('') || '<tr><td colspan="4">無現貨持倉</td></tr>';
  document.querySelectorAll('.cg-tab').forEach((b,i) => b.classList.toggle('active', i===0));
  document.querySelectorAll('.cg-tab-panel').forEach((el,i) => el.classList.toggle('active', i===0));
  modal.classList.remove('hidden');
  modal.setAttribute('aria-hidden', 'false');
}}

function drawChart(points) {{
  const svg = document.getElementById('cg-chart');
  if (!points.length) {{ svg.innerHTML = '<text x="10" y="100" fill="#8b949e">無圖表資料</text>'; return; }}
  const xs = points.map(p => p[0]), ys = points.map(p => p[1]);
  const minY = Math.min(...ys), maxY = Math.max(...ys);
  const pad = 8, w = 800, h = 190;
  const scaleX = i => pad + (i / (points.length - 1 || 1)) * (w - pad * 2);
  const scaleY = v => h - pad - ((v - minY) / ((maxY - minY) || 1)) * (h - pad * 2);
  const zeroY = scaleY(0);
  const coords = points.map((p, i) => `${{scaleX(i)}},${{scaleY(p[1])}}`).join(' ');
  const fill = `M ${{scaleX(0)}},${{zeroY}} L ${{coords}} L ${{scaleX(points.length-1)}},${{zeroY}} Z`;
  svg.innerHTML = `<path d="${{fill}}" fill="rgba(63,185,80,0.15)"/><polyline points="${{coords}}" fill="none" stroke="#3fb950" stroke-width="2"/>`;
}}

function closeProfile() {{
  const modal = document.getElementById('profile-modal');
  modal.classList.add('hidden');
  modal.setAttribute('aria-hidden', 'true');
}}

document.getElementById('cg-close').addEventListener('click', closeProfile);
document.getElementById('cg-close-backdrop').addEventListener('click', closeProfile);
document.getElementById('cg-copy').addEventListener('click', () => {{
  const addr = document.getElementById('cg-addr').textContent;
  navigator.clipboard.writeText(addr).catch(() => {{}});
}});

document.querySelectorAll('.cg-tab').forEach(btn => {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.cg-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const tab = btn.dataset.cgtab;
    document.querySelectorAll('.cg-tab-panel').forEach(el => {{
      el.classList.toggle('active', el.id === 'cg-tab-' + tab);
    }});
  }});
}});

document.querySelectorAll('.main-tabs .tab').forEach(btn => {{
  btn.addEventListener('click', () => {{
    document.querySelectorAll('.main-tabs .tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    document.getElementById(btn.dataset.panel).classList.add('active');
  }});
}});

document.querySelectorAll('#consensus .subtab').forEach(btn => {{
  btn.addEventListener('click', () => {{
    const root = document.getElementById('consensus');
    root.querySelectorAll('.subtab').forEach(b => b.classList.remove('active'));
    root.querySelectorAll('.sub-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    const panel = document.getElementById(btn.dataset.subpanel);
    panel.classList.add('active');
    if (btn.dataset.subpanel === 'consensus-orders') initOrderCharts();
  }});
}});

document.getElementById('search').addEventListener('input', e => {{
  const q = e.target.value.toLowerCase();
  const root = activeViewRoot();
  if (!root) return;
  root.querySelectorAll('tbody').forEach(tbody => {{
    tbody.querySelectorAll('tr').forEach(row => {{
      row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
    }});
  }});
}});

document.querySelectorAll('table.sortable').forEach(table => {{
  table.querySelectorAll('th').forEach((th, idx) => {{
    th.addEventListener('click', () => {{
      const tbody = table.querySelector('tbody');
      if (!tbody) return;
      const rows = Array.from(tbody.querySelectorAll('tr'));
      const asc = th.dataset.asc !== 'true';
      th.parentElement.querySelectorAll('th').forEach(h => delete h.dataset.asc);
      th.dataset.asc = asc;
      rows.sort((a, b) => {{
        const av = a.children[idx]?.dataset?.sort ?? a.children[idx]?.textContent ?? '';
        const bv = b.children[idx]?.dataset?.sort ?? b.children[idx]?.textContent ?? '';
        const an = parseFloat(av), bn = parseFloat(bv);
        const cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : String(av).localeCompare(String(bv));
        return asc ? cmp : -cmp;
      }});
      rows.forEach(r => tbody.appendChild(r));
    }});
  }});
}});

applyWindowFilter('all');
if (document.getElementById('consensus-orders')?.classList.contains('active')) initOrderCharts();
</script>
</body>
</html>"""


def html_from_csv(output_dir: str) -> None:
    """Build HTML from existing CSV files without re-fetching API."""
    accounts_path = os.path.join(output_dir, "top300_accounts.csv")
    trades_4h_path = os.path.join(output_dir, "trades_4h.csv")
    trades_24h_path = os.path.join(output_dir, "trades_24h.csv")

    qualified: list[TraderFills] = []
    with open(accounts_path, encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            wr = row.get("win_rate") or row.get("win_rate_7d") or row.get("win_rate_30d") or "0"
            closed = row.get("closed_trades_7d") or row.get("closed_trades_30d") or "0"
            t = TraderScore(
                address=row["address"],
                display_name=row.get("display_name", ""),
                account_value=float(row["account_value_usd"]),
                day_pnl=float(row["day_pnl"]),
                day_roi=float(row["day_roi"]),
                day_volume=float(row["day_volume"]),
                week_roi=float(row["week_roi"]),
                month_roi=float(row["month_roi"]),
                month_pnl=float(row.get("month_pnl") or 0),
                alltime_pnl=float(row.get("alltime_pnl") or 0),
                win_rate=float(wr),
                closed_trades=int(closed),
                score=float(row["score"]),
                platform=row.get("platform", "Hyperliquid"),
                win_rate_source=row.get("win_rate_source", "30d"),
                year_roi=float(row.get("year_roi") or 0),
                history_days=int(row.get("history_days") or 0),
            )
            qualified.append(TraderFills(trader=t, fills=[], win_rate=t.win_rate, closed_trades=t.closed_trades))

    def load_records(path: str, window: str) -> list[OpenRecord]:
        records: list[OpenRecord] = []
        with open(path, encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                ts_str = row["open_datetime_utc"].replace(" UTC", "")
                ts = int(datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)
                wr = row.get("win_rate_7d") or row.get("win_rate_30d") or row.get("win_rate") or "0"
                records.append(OpenRecord(
                    address=row["address"],
                    display_name=row.get("display_name", ""),
                    win_rate=float(wr),
                    rank=int(row["account_rank"]),
                    window=window,
                    coin=row["coin"],
                    direction=row["direction"],
                    avg_entry_px=float(row["avg_entry_px"]),
                    total_size=float(row["total_size"]),
                    open_time=row["open_datetime_utc"],
                    open_ts=ts,
                    fill_count=int(row["open_fill_count"]),
                    platform=row.get("platform", "Hyperliquid"),
                ))
        return records

    records_4h = load_records(trades_4h_path, "4H")
    records_24h = load_records(trades_24h_path, "24H")
    total = len(qualified)
    consensus_4h = build_consensus(records_4h, "4H", total)
    consensus_24h = build_consensus(records_24h, "24H", total)
    html_path = os.path.join(output_dir, "report.html")
    write_html_report(html_path, qualified, records_4h, records_24h, consensus_4h, consensus_24h, fetch_profiles=False)
    print(f"  HTML report: {html_path}")


def build_info_url_config(info_urls_arg: str, goldrush_key: str | None = None) -> tuple[list[str], dict[str, str]]:
    urls = [u.strip().rstrip("/") for u in info_urls_arg.split(",") if u.strip()]
    auth: dict[str, str] = {}
    key = goldrush_key or os.environ.get("GOLDRUSH_API_KEY") or os.environ.get("HL_GOLDRUSH_KEY")
    if key:
        if GOLDRUSH_INFO_URL not in urls:
            urls.append(GOLDRUSH_INFO_URL)
        auth[GOLDRUSH_INFO_URL] = key
    return urls, auth


def probe_info_mirrors(
    sample_user: str | None = None,
    extra_urls: list[str] | None = None,
    auth_by_url: dict[str, str] | None = None,
) -> list[tuple[str, str, int, float, str]]:
    if not sample_user:
        sample_user = "0xd307f9b1a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0"
        accounts_csv = os.path.join("output", "top300_accounts.csv")
        if os.path.isfile(accounts_csv):
            with open(accounts_csv, encoding="utf-8-sig") as fh:
                row = next(csv.DictReader(fh), None)
                if row:
                    sample_user = row["address"]

    now = utc_now_ms()
    payloads = {
        "meta": {"type": "meta"},
        "userFillsByTime": {
            "type": "userFillsByTime",
            "user": sample_user,
            "startTime": now - 7 * DAY_MS,
            "endTime": now,
            "aggregateByTime": True,
        },
    }
    urls = list(dict.fromkeys(DEFAULT_INFO_URLS + list(extra_urls or [])))
    auth = auth_by_url or {}
    results: list[tuple[str, str, int, float, str]] = []

    for url in urls:
        for ptype, payload in payloads.items():
            t0 = time.time()
            try:
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers=_info_headers_for(url, auth.get(url.rstrip("/"))),
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=20) as resp:
                    body = resp.read(120).decode("utf-8", "replace")
                    results.append((url, ptype, resp.status, time.time() - t0, body[:80]))
            except urllib.error.HTTPError as exc:
                results.append((url, ptype, exc.code, time.time() - t0, str(exc.reason)[:80]))
            except Exception as exc:
                results.append((url, ptype, -1, time.time() - t0, str(exc)[:80]))
    return results


def _info_headers_for(url: str, api_key: str | None = None) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def print_mirror_probe(extra_urls: list[str] | None = None, auth_by_url: dict[str, str] | None = None) -> int:
    print("Probing Hyperliquid /info mirrors (meta + userFillsByTime) ...")
    rows = probe_info_mirrors(extra_urls=extra_urls, auth_by_url=auth_by_url)
    by_url: dict[str, list[tuple[str, int, float, str]]] = {}
    for url, ptype, code, dt, msg in rows:
        by_url.setdefault(url, []).append((ptype, code, dt, msg))
    ok_fills: list[str] = []
    for url, items in by_url.items():
        summary = " | ".join(f"{ptype}={code}({dt:.2f}s)" for ptype, code, dt, _ in items)
        print(f"  {url}")
        print(f"    {summary}")
        fills = next((code for ptype, code, _, _ in items if ptype == "userFillsByTime"), -1)
        if fills == 200:
            ok_fills.append(url)
    print("")
    print(f"FREE mainnet mirrors for userFillsByTime: {len([u for u in ok_fills if u in DEFAULT_INFO_URLS])}/{len(DEFAULT_INFO_URLS)}")
    if ok_fills:
        print("Working for scanner fills:")
        for url in ok_fills:
            tag = "FREE" if url in DEFAULT_INFO_URLS else "API KEY"
            print(f"  [{tag}] {url}")
    print("")
    print("Paid / optional (need API key, add via --goldrush-key or GOLDRUSH_API_KEY):")
    print(f"  {GOLDRUSH_INFO_URL}")
    print("  https://hyperliquid-mainnet.g.alchemy.com/v2/<KEY>/info")
    print("  https://api-hyperliquid-mainnet-info.n.dwellir.com/info  (Dwellir: userFillsByTime often unsupported)")
    return 0


def write_hl_routes(path: str) -> None:
    lines = [
        "Hyperliquid API routes (used by this scanner)",
        f"Updated: {utc_str(utc_now_ms())}",
        "",
        "FREE / ACTIVE (mainnet, no key):",
        "  Leaderboard : stats-data.hyperliquid.xyz/Mainnet/leaderboard",
        "  Info #1     : api.hyperliquid.xyz/info",
        "  Info #2     : api-ui.hyperliquid.xyz/info       [only other free mirror; tested OK]",
        "",
        "PARALLEL / SPEED:",
        "  Each mirror has ~1200 weight/min/IP. Scanner pins one mirror per worker thread.",
        "  fills_cache.json caches fills by address+window for 30 min — rerun is much faster.",
        "  Optional 3rd lane: GoldRush (--goldrush-key or env GOLDRUSH_API_KEY).",
        "",
        "NOT USABLE (tested):",
        "  app.hyperliquid.xyz/info          -> 403",
        "  stats-data.hyperliquid.xyz/info   -> 403",
        "  rpc.hyperliquid.xyz/info          -> 404",
        "  api.hyperliquid-testnet.xyz/info  -> testnet only (wrong chain)",
        "",
        "OPTIONAL (API key / paid):",
        "  hypercore.goldrushdata.com/info   - GoldRush drop-in, supports userFillsByTime",
        "  hyperliquid-mainnet.g.alchemy.com/v2/<KEY>/info - Alchemy proxy",
        "  api-hyperliquid-mainnet-info.n.dwellir.com/info - Dwellir (userFillsByTime -> use public API)",
        "  data.blockliquidity.xyz           - winrate only, not full fills",
        "",
        "Commands:",
        "  python fetch_top_traders.py --probe-mirrors",
        "  python fetch_top_traders.py --goldrush-key <KEY> --workers 32",
        "  python fetch_top_traders.py --refresh-fills   # ignore fills cache",
        "",
    ]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def write_summary_txt(
    path: str,
    qualified: list[TraderFills],
    records_4h: list[OpenRecord],
    records_24h: list[OpenRecord],
    consensus_4h: list[ConsensusTarget],
    consensus_24h: list[ConsensusTarget],
    min_year_roi: float = MIN_YEAR_ROI,
) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("Hyperliquid Top Active High-Win-Rate Accounts\n")
        fh.write(f"Generated: {utc_str(utc_now_ms())}\n")
        fh.write(f"Info URLs: {', '.join(_info_urls)}\n")
        fh.write(f"Filter: active account (>= {MIN_FILLS_30D} fills in 30d)\n")
        fh.write(f"Filter: account age >= {MIN_HISTORY_DAYS} days (portfolio history)\n")
        fh.write(f"Filter: day volume >= ${MIN_DAY_VOLUME_USD:g}, week volume >= ${MIN_WEEK_VOLUME_USD:g}\n")
        fh.write("Filter: day/week ROI > 0, account value >= ${:g}\n".format(MIN_ACCOUNT_VALUE_USD))
        fh.write("Filter: 30D PnL > 0 (leaderboard month)\n")
        fh.write(
            f"Filter: ROI > {min_year_roi:.0%} "
            f"(trailing 365D if history>=365d, else since account inception)\n"
        )
        fh.write("Filter: all-time PnL > 0 (leaderboard allTime)\n")
        fh.write(f"Filter: exclude peak drawdown >= {MAX_PEAK_DRAWDOWN:.0%}\n")
        fh.write(f"Consensus: min {MIN_CONSENSUS_ACCOUNTS} accounts per symbol\n")
        fh.write(
            f"Exclude MM: median(|closedPnl|) < ${MM_MEDIAN_ABS_PNL_USD:g}"
            f" and closed_trades > {MM_MIN_CLOSED_TRADES}\n\n"
        )
        fh.write(f"Accounts: {len(qualified)}\n")
        fh.write(f"4H open records: {len(records_4h)}\n")
        fh.write(f"24H open records: {len(records_24h)}\n\n")

        fh.write("24H Consensus Targets (by account count):\n")
        fh.write("-" * 80 + "\n")
        for i, c in enumerate(consensus_24h[:30], start=1):
            fh.write(
                f"{i:>3}. {c.coin:<16} accounts={c.account_count:>3} ({c.account_pct:.1%}) "
                f"dir={format_direction_zh(c.consensus_direction):<4} net={c.net_ratio:.0%}\n"
            )

        fh.write("\nTop Accounts:\n")
        fh.write("-" * 80 + "\n")
        for i, item in enumerate(qualified[:20], start=1):
            t = item.trader
            name = t.display_name or "(anonymous)"
            fh.write(
                f"{i:>3}. {t.address} | {name} | ROI={t.year_roi:.1%} "
                f"| dayVol=${t.day_volume:,.0f} | dayROI={t.day_roi:.2%}\n"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Hyperliquid top traders (parallel info mirrors).")
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument(
        "--min-year-roi",
        type=float,
        default=MIN_YEAR_ROI,
        help="Min ROI (default: 1.5 = 150%%; trailing 365D or since inception if <365d)",
    )
    parser.add_argument("--wr-days", type=int, default=DEFAULT_WR_DAYS, help="Win-rate lookback days (default: 30)")
    parser.add_argument("--min-closed", type=int, default=5)
    parser.add_argument("--scan-limit", type=int, default=2500)
    parser.add_argument("--workers", type=int, default=24, help="Parallel API workers (default: 24)")
    parser.add_argument("--fast", action="store_true", default=True, help="Fast mode (default on)")
    parser.add_argument("--slow", action="store_true", help="Scan more candidates (slower)")
    parser.add_argument("--no-cache", action="store_true", help="Skip leaderboard file cache")
    parser.add_argument("--no-fills-cache", action="store_true", help="Disable fills disk cache")
    parser.add_argument("--refresh-fills", action="store_true", help="Ignore fills cache and re-fetch all accounts")
    parser.add_argument("--fills-cache-ttl", type=int, default=FILLS_CACHE_TTL_SEC, help="Fills cache TTL seconds")
    parser.add_argument("--goldrush-key", default="", help="GoldRush API key (or env GOLDRUSH_API_KEY)")
    parser.add_argument("--probe-mirrors", action="store_true", help="Test info mirrors and exit")
    parser.add_argument(
        "--info-urls",
        default=",".join(DEFAULT_INFO_URLS),
        help="Comma-separated Hyperliquid /info mirrors (free: api + api-ui only)",
    )
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--html-only", action="store_true", help="Generate HTML from existing CSV in output-dir")
    args = parser.parse_args()

    info_urls, info_auth = build_info_url_config(args.info_urls, args.goldrush_key or None)
    configure_info_urls(info_urls, info_auth)

    if args.probe_mirrors:
        return print_mirror_probe(extra_urls=info_urls, auth_by_url=info_auth)

    configure_wr_days(args.wr_days)
    configure_min_year_roi(args.min_year_roi)
    configure_fills_cache(
        os.path.join(args.output_dir, FILLS_CACHE_FILE),
        use=not args.no_fills_cache,
        refresh=args.refresh_fills,
        ttl_sec=args.fills_cache_ttl,
    )
    os.makedirs(args.output_dir, exist_ok=True)

    if args.html_only:
        try:
            html_from_csv(args.output_dir)
            return 0
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

    t0 = time.time()

    try:
        print(f"      Info mirrors: {len(_info_urls)} ({', '.join(_info_urls)})")
        if GOLDRUSH_INFO_URL in _info_urls:
            print("      GoldRush lane enabled (3rd mirror)")
        if _fills_cache_use:
            print(f"      Fills cache: {os.path.join(args.output_dir, FILLS_CACHE_FILE)} (TTL {args.fills_cache_ttl}s, 1Y ROI>{args.min_year_roi:.0%})")
        rows = fetch_leaderboard(args.output_dir, use_cache=not args.no_cache)
        qualified = select_top_traders(
            rows, target=args.count,
            min_closed=args.min_closed, scan_limit=args.scan_limit,
            workers=args.workers, fast=not args.slow,
        )
        if not qualified:
            print("No traders matched. Try --min-year-roi 0.5 or --scan-limit 3000")
            return 1

        print("[3/3] Building 4H / 24H opens + consensus ...")
        records_4h, records_24h = records_from_qualified(qualified)
        total = len(qualified)
        consensus_4h = build_consensus(records_4h, "4H", total)
        consensus_24h = build_consensus(records_24h, "24H", total)

        write_accounts_csv(os.path.join(args.output_dir, "top300_accounts.csv"), qualified)
        write_trades_csv(os.path.join(args.output_dir, "trades_4h.csv"), records_4h)
        write_trades_csv(os.path.join(args.output_dir, "trades_24h.csv"), records_24h)
        write_consensus_csv(os.path.join(args.output_dir, "consensus_4h.csv"), consensus_4h)
        write_consensus_csv(os.path.join(args.output_dir, "consensus_24h.csv"), consensus_24h)
        write_summary_txt(
            os.path.join(args.output_dir, "summary.txt"),
            qualified, records_4h, records_24h, consensus_4h, consensus_24h,
            min_year_roi=args.min_year_roi,
        )
        write_hl_routes(os.path.join(args.output_dir, "hl_api_routes.txt"))
        write_html_report(
            os.path.join(args.output_dir, "report.html"),
            qualified, records_4h, records_24h, consensus_4h, consensus_24h,
            min_year_roi=args.min_year_roi,
        )

        elapsed = time.time() - t0
        print(f"\nDone in {elapsed:.0f}s ({elapsed/60:.1f} min).")
        print(f"  Accounts: {len(qualified)}")
        print(f"  output/top300_accounts.csv")
        print(f"  output/consensus_24h.csv  ({len(consensus_24h)} symbols)")
        print(f"  output/report.html")
        print(f"  output/hl_api_routes.txt")
        if consensus_24h:
            top = consensus_24h[0]
            print(f"  Top consensus: {top.coin} ({top.account_count} accounts, {format_direction_zh(top.consensus_direction)})")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

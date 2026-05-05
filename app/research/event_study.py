from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Dict, List, Optional

_BASE_DIR = Path(__file__).resolve().parents[2]
_STRATEGY_CFG_PATH = _BASE_DIR / "app" / "config" / "strategy_config.json"


def _load_thresholds() -> dict:
    with open(_STRATEGY_CFG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return {
        "long": float(cfg["entry_threshold_long"]),
        "short": float(cfg["entry_threshold_short"]),
        "strong_long": float(cfg["strong_entry_threshold_long"]),
        "strong_short": float(cfg["strong_entry_threshold_short"]),
    }


def load_daily_prices_from_csv(csv_path: Path) -> List[dict]:
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(
                {
                    "price_time": row["price_time"],
                    "symbol": row["symbol"],
                    "asset_type": row.get("asset_type", "commodity"),
                    "open": float(row["open"]) if row.get("open") else None,
                    "high": float(row["high"]) if row.get("high") else None,
                    "low": float(row["low"]) if row.get("low") else None,
                    "close": float(row["close"]) if row.get("close") else None,
                    "volume": float(row["volume"]) if row.get("volume") else None,
                }
            )
    return rows


def normalize_daily_close_series(price_rows: List[dict], symbol: str) -> List[dict]:
    filtered = [r for r in price_rows if r["symbol"] == symbol and r["close"] is not None]
    filtered.sort(key=lambda x: x["price_time"])
    return [{"date": str(r["price_time"])[:10], "close": float(r["close"])} for r in filtered]


def build_close_map(series: List[dict]) -> Dict[str, float]:
    return {r["date"]: r["close"] for r in series}


def ordered_dates(series: List[dict]) -> List[str]:
    return [r["date"] for r in series]


def future_return(close_by_date: Dict[str, float], ordered: List[str], start_date: str, horizon_days: int) -> Optional[float]:
    if start_date not in close_by_date:
        return None
    try:
        idx = ordered.index(start_date)
    except ValueError:
        return None
    future_idx = idx + horizon_days
    if future_idx >= len(ordered):
        return None
    p0 = close_by_date[ordered[idx]]
    p1 = close_by_date[ordered[future_idx]]
    if p0 in (None, 0) or p1 is None:
        return None
    return (p1 / p0) - 1.0


def signal_bucket(score: float, thresholds: dict | None = None) -> str:
    th = thresholds or _load_thresholds()
    if score >= th["strong_long"]:
        return "strong_bullish"
    if score >= th["long"]:
        return "bullish"
    if score <= th["strong_short"]:
        return "strong_bearish"
    if score <= th["short"]:
        return "bearish"
    return "neutral"


def hit_direction(score: float, ret: float) -> Optional[int]:
    if score > 0:
        return 1 if ret > 0 else 0
    if score < 0:
        return 1 if ret < 0 else 0
    return None


def run_event_study(scores: List[dict], price_series: List[dict], horizons: List[int]) -> dict:
    # Accept either raw price rows with price_time/close or normalized rows with date/close.
    if price_series and "date" not in price_series[0]:
        symbol = price_series[0].get("symbol", "WTI")
        price_series = normalize_daily_close_series(price_series, symbol)
    close_map = build_close_map(price_series)
    ordered = ordered_dates(price_series)

    enriched = []
    for s in scores:
        score_date = s["score_date"]
        row = dict(s)
        row["bucket"] = signal_bucket(float(s["narrative_score"]))
        for h in horizons:
            fwd = future_return(close_map, ordered, score_date, h)
            row[f"fwd_ret_{h}d"] = fwd
            row[f"hit_{h}d"] = hit_direction(float(s["narrative_score"]), fwd) if fwd is not None else None
        enriched.append(row)

    by_bucket = defaultdict(list)
    by_topic = defaultdict(list)

    for row in enriched:
        by_bucket[row["bucket"]].append(row)
        by_topic[row["topic"]].append(row)

    bucket_summary = {}
    for bucket, rows in by_bucket.items():
        summary = {"count": len(rows)}
        for h in horizons:
            vals = [r[f"fwd_ret_{h}d"] for r in rows if r[f"fwd_ret_{h}d"] is not None]
            hits = [r[f"hit_{h}d"] for r in rows if r[f"hit_{h}d"] is not None]
            summary[f"avg_fwd_ret_{h}d"] = mean(vals) if vals else None
            summary[f"hit_rate_{h}d"] = mean(hits) if hits else None
        bucket_summary[bucket] = summary

    topic_summary = {}
    for topic, rows in by_topic.items():
        summary = {"count": len(rows)}
        for h in horizons:
            vals = [r[f"fwd_ret_{h}d"] for r in rows if r[f"fwd_ret_{h}d"] is not None]
            hits = [r[f"hit_{h}d"] for r in rows if r[f"hit_{h}d"] is not None]
            summary[f"avg_fwd_ret_{h}d"] = mean(vals) if vals else None
            summary[f"hit_rate_{h}d"] = mean(hits) if hits else None
        topic_summary[topic] = summary

    return {
        "horizons": horizons,
        "sample_size": len(enriched),
        "bucket_summary": bucket_summary,
        "topic_summary": topic_summary,
        "rows": enriched,
    }


def run_conditional_event_study(
    scores: List[dict],
    price_series: List[dict],
    regime_rows: List[dict],
    horizons: List[int],
    streak_min: int = 0,
) -> dict:
    """Bucket samples by (primary_regime, narrative_bucket).

    `regime_rows` are dicts with at least `regime_date`, `primary_regime`,
    `regime_streak`. Score-dates with no regime data, or with streak below
    `streak_min`, are dropped.

    Output mirrors run_event_study but adds a `regime` outer dimension.
    """
    if price_series and "date" not in price_series[0]:
        symbol = price_series[0].get("symbol", "WTI")
        price_series = normalize_daily_close_series(price_series, symbol)
    close_map = build_close_map(price_series)
    ordered = ordered_dates(price_series)

    regime_map = {
        str(r["regime_date"]): {
            "primary_regime": r["primary_regime"],
            "regime_streak": int(r.get("regime_streak") or 0),
        }
        for r in regime_rows
    }

    by_cell: Dict[tuple, List[dict]] = defaultdict(list)
    regime_dates: Dict[str, set] = defaultdict(set)
    skipped_no_regime = 0

    for s in scores:
        score_date = s["score_date"]
        regime_info = regime_map.get(score_date)
        if regime_info is None:
            skipped_no_regime += 1
            continue
        if regime_info["regime_streak"] < streak_min:
            continue

        regime = regime_info["primary_regime"]
        bucket = signal_bucket(float(s["narrative_score"]))
        row = dict(s)
        row["bucket"] = bucket
        row["primary_regime"] = regime
        row["regime_streak"] = regime_info["regime_streak"]
        for h in horizons:
            fwd = future_return(close_map, ordered, score_date, h)
            row[f"fwd_ret_{h}d"] = fwd
            row[f"hit_{h}d"] = hit_direction(float(s["narrative_score"]), fwd) if fwd is not None else None

        by_cell[(regime, bucket)].append(row)
        regime_dates[regime].add(score_date)

    # Cell summary: nested by_regime[regime][bucket]
    by_regime: Dict[str, dict] = {}
    for (regime, bucket), rows in by_cell.items():
        cell = {"count": len(rows)}
        for h in horizons:
            vals = [r[f"fwd_ret_{h}d"] for r in rows if r[f"fwd_ret_{h}d"] is not None]
            hits = [r[f"hit_{h}d"] for r in rows if r[f"hit_{h}d"] is not None]
            cell[f"avg_fwd_ret_{h}d"] = mean(vals) if vals else None
            cell[f"hit_rate_{h}d"] = mean(hits) if hits else None
        by_regime.setdefault(regime, {})[bucket] = cell

    regime_distribution = {r: len(d) for r, d in regime_dates.items()}

    return {
        "horizons": horizons,
        "streak_min": streak_min,
        "skipped_no_regime": skipped_no_regime,
        "regime_distribution": regime_distribution,
        "by_regime": by_regime,
    }

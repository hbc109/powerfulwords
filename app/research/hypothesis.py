"""Hypothesis tester — evaluate falsifiable trading rules against history.

Each hypothesis is a (rule, direction) pair. The rule is a callable that
returns True/False given a row dict containing both narrative and regime
fields. We then look up forward returns at horizons 1/3/5/10 trading days
and compute hit rate + avg forward return for the triggering set.

This is *not* a backtest — there's no position sizing, no slippage, no
multi-horizon entries/exits. It's a clean signal evaluator: "if I
mechanically went LONG (or SHORT) on every day this rule fires, what
would the average outcome look like?"
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from statistics import mean
from typing import Callable, List, Optional

import pandas as pd

from app.research.event_study import (
    build_close_map, future_return, normalize_daily_close_series, ordered_dates,
)


@dataclass
class Hypothesis:
    name: str
    description: str
    direction: str  # "long" or "short"
    rule: Callable[[dict], bool]


def _hit(direction: str, fwd_ret: float) -> int:
    """Did price move the way the rule predicted?"""
    if direction == "long":
        return 1 if fwd_ret > 0 else 0
    if direction == "short":
        return 1 if fwd_ret < 0 else 0
    raise ValueError(f"unsupported direction: {direction}")


def fetch_hypothesis_universe(conn: sqlite3.Connection, symbol: str,
                              commodity: str = "crude_oil") -> List[dict]:
    """Join daily_narrative_scores × daily_regimes for one symbol.
    Returns a list of dicts each containing both score and regime fields
    on the same date — the universe over which a rule iterates.
    """
    q = """
    SELECT s.score_date, s.commodity, s.topic, s.narrative_score, s.theme,
           s.event_count, s.breadth, s.persistence, s.source_divergence,
           s.official_confirmation_score, s.news_breadth_score,
           s.chatter_score, s.crowding_score,
           r.primary_regime, r.regime_streak, r.regime_tags,
           r.rsi14, r.adx14, r.bb_pctb, r.atr_ratio,
           r.macd_hist, r.volume_ratio, r.cross_product_agreement,
           r.close
    FROM daily_narrative_scores s
    JOIN daily_regimes r ON s.score_date = r.regime_date AND r.symbol = ?
    WHERE s.commodity = ?
    ORDER BY s.score_date, s.topic
    """
    cur = conn.execute(q, (symbol, commodity))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_prices(conn: sqlite3.Connection, symbol: str) -> List[dict]:
    cur = conn.execute(
        "SELECT price_time, symbol, asset_type, open, high, low, close, volume "
        "FROM market_prices WHERE symbol = ? ORDER BY price_time",
        (symbol,),
    )
    return [
        {"price_time": r[0], "symbol": r[1], "asset_type": r[2],
         "open": r[3], "high": r[4], "low": r[5], "close": r[6], "volume": r[7]}
        for r in cur.fetchall()
    ]


def evaluate_hypothesis(
    hypothesis: Hypothesis,
    universe: List[dict],
    prices: List[dict],
    horizons: List[int],
) -> dict:
    """Apply rule, compute hit rate + avg forward return at each horizon.

    The rule iterates over (date, topic) rows, but the trading unit is
    the *day*: a strategy acts once per date. We therefore dedupe to
    per-date triggers — a date fires if any topic on that date passes
    the rule. Hit rate / forward return are then computed on unique
    dates, which is the methodologically correct sample size.
    """
    series = normalize_daily_close_series(prices, prices[0]["symbol"])
    close_map = build_close_map(series)
    ordered = ordered_dates(series)

    # Per-(date, topic) triggers (kept for diagnostics)
    triggered_rows = [r for r in universe if hypothesis.rule(r)]

    # Per-date dedupe — one observation per unique date
    triggered_dates = sorted({r["score_date"] for r in triggered_rows})
    if not triggered_dates:
        return {
            "name": hypothesis.name,
            "direction": hypothesis.direction,
            "n_triggered_rows": 0,
            "unique_dates": 0,
            "by_horizon": {},
        }

    by_h: dict = {}
    for h in horizons:
        rets = []
        hits = []
        for d in triggered_dates:
            fwd = future_return(close_map, ordered, str(d), h)
            if fwd is None:
                continue
            rets.append(fwd)
            hits.append(_hit(hypothesis.direction, fwd))
        by_h[h] = {
            "count": len(rets),
            "avg_fwd_ret": mean(rets) if rets else None,
            "hit_rate": mean(hits) if hits else None,
        }

    return {
        "name": hypothesis.name,
        "description": hypothesis.description,
        "direction": hypothesis.direction,
        "n_triggered_rows": len(triggered_rows),
        "unique_dates": len(triggered_dates),
        "by_horizon": by_h,
    }

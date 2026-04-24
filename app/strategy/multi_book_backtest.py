"""Multi-book / multi-instrument backtest.

Each book defines its own instrument (outright or spread), capital,
thresholds, and scoring config (theme weights + vetoes). All books share
the same daily narrative scores. Aggregate equity is the sum of book
equities; each book is run independently.

PnL model:
- Outright: PnL_t = book_capital * prev_position * (close_t / close_{t-1} - 1)
- Spread:   PnL_t = book_capital * prev_position * (spread_t - spread_{t-1}) / max(|spread_{t-1}|, 1.0)
  The /max(...,1) normalisation gives sensible scaling whether the spread
  trades around 1.0 or 50.0; for very tight spreads near zero it caps the
  effective leverage.

Cost model: book_capital * turnover * one_way_cost_bps / 10000.

This is intentionally simple — refine when real spread P&L data exists.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from app.strategy.backtest_engine import (
    aggregate_score_by_date,
    apply_theme_vetoes,
    score_to_target_position,
)

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "app" / "config" / "multi_strategy_config.json"


def load_multi_strategy_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _close_map_for_symbol(price_rows: List[dict], symbol: str) -> Dict[str, float]:
    out = {}
    for r in price_rows:
        if r["symbol"] != symbol:
            continue
        date = str(r["price_time"])[:10]
        if r.get("close") is not None:
            out[date] = float(r["close"])
    return out


def resolve_instrument_close_series(
    price_rows: List[dict], instrument: dict
) -> List[tuple[str, float]]:
    """Return [(date, close_or_derived)] sorted by date for the given instrument.

    For spreads and cracks, only dates where ALL legs have closes are included.
    Crack: product close - crude close (assumes both quoted in $/bbl).
    """
    itype = instrument.get("type", "outright")
    if itype == "outright":
        m = _close_map_for_symbol(price_rows, instrument["symbol"])
        return sorted(m.items())
    if itype == "spread":
        long_m = _close_map_for_symbol(price_rows, instrument["long_symbol"])
        short_m = _close_map_for_symbol(price_rows, instrument["short_symbol"])
        common = sorted(set(long_m) & set(short_m))
        return [(d, long_m[d] - short_m[d]) for d in common]
    if itype == "crack":
        product_m = _close_map_for_symbol(price_rows, instrument["product_symbol"])
        crude_m = _close_map_for_symbol(price_rows, instrument["crude_symbol"])
        common = sorted(set(product_m) & set(crude_m))
        return [(d, product_m[d] - crude_m[d]) for d in common]
    raise ValueError(f"Unknown instrument type: {itype}")


# ---- P&L models ----
#
# Each instrument can specify `pnl_method`:
#   - "pct_return" (default for outright): PnL_t = book_capital * pos * (close_t/close_{t-1} - 1)
#   - "point_value" (default for spread, crack): PnL_t = pos * (price_t - price_{t-1}) * point_value
#
# point_value defaults: outright=1000, spread=100, crack=100. Override per
# instrument when you know the real contract spec (NYMEX WTI is 1000 bbl/contract
# so $1 move = $1000; CME Crack spreads are 1000 bbl/contract too, so $1 move
# in the crack = $1000 — set point_value: 1000 there).


_DEFAULT_POINT_VALUE = {"outright": 1000.0, "spread": 100.0, "crack": 100.0}


def _instrument_pnl_method(instrument: dict) -> str:
    if "pnl_method" in instrument:
        return instrument["pnl_method"]
    return "pct_return" if instrument.get("type", "outright") == "outright" else "point_value"


def compute_daily_pnl(
    instrument: dict,
    book_capital: float,
    prev_position: float,
    price_t: float,
    price_prev: float,
) -> float:
    method = _instrument_pnl_method(instrument)
    if method == "pct_return":
        if price_prev == 0:
            return 0.0
        return book_capital * prev_position * ((price_t / price_prev) - 1.0)
    if method == "point_value":
        itype = instrument.get("type", "outright")
        pv = float(instrument.get("point_value", _DEFAULT_POINT_VALUE.get(itype, 100.0)))
        return prev_position * (price_t - price_prev) * pv
    raise ValueError(f"Unknown pnl_method: {method}")


def run_book(book_cfg: dict, score_rows: List[dict], price_rows: List[dict], cost_bps: float) -> dict:
    instrument = book_cfg["instrument"]
    series = resolve_instrument_close_series(price_rows, instrument)
    if not series:
        return {
            "name": book_cfg["name"],
            "instrument": instrument,
            "book_capital": float(book_cfg["book_capital"]),
            "summary": {"final_equity": float(book_cfg["book_capital"]), "num_days": 0, "num_trades": 0},
            "equity_curve": [],
            "trades": [],
            "warnings": ["No price data for this instrument."],
        }

    scoring_cfg = book_cfg.get("scoring") or {}
    use_themes = bool(scoring_cfg.get("use_themes", False))
    group_field = "theme" if use_themes else "topic"
    weights = scoring_cfg.get("theme_weights") if use_themes else None
    vetoes = scoring_cfg.get("theme_vetoes", []) if use_themes else []

    aggregated = aggregate_score_by_date(score_rows, weights=weights, group_field=group_field)
    score_by_date = {r["score_date"]: float(r["aggregate_score"]) for r in aggregated}
    breakdown_by_date = {r["score_date"]: r["breakdown"] for r in aggregated}
    raw_breakdown_by_date = {r["score_date"]: r["raw_breakdown"] for r in aggregated}

    book_capital = float(book_cfg["book_capital"])
    cost_rate = float(cost_bps) / 10000.0

    equity = book_capital
    prev_price = None
    prev_position = 0.0
    equity_curve = []
    trades = []
    vetoed = 0

    for date, price in series:
        score = score_by_date.get(date, 0.0)
        proposed_position = score_to_target_position(score, book_cfg)

        veto_reasons = []
        if use_themes:
            todays = dict(raw_breakdown_by_date.get(date, []))
            target_position, veto_reasons = apply_theme_vetoes(proposed_position, todays, vetoes)
        else:
            target_position = proposed_position
        if veto_reasons:
            vetoed += 1

        pnl = 0.0
        if prev_price is not None:
            pnl = compute_daily_pnl(instrument, book_capital, prev_position, price, prev_price)

        turnover = abs(target_position - prev_position)
        cost = book_capital * turnover * cost_rate
        equity = equity + pnl - cost

        if turnover > 0:
            top = breakdown_by_date.get(date, [])[:3]
            trade = {
                "date": date,
                "score": round(score, 6),
                "prev_position": prev_position,
                "target_position": target_position,
                "proposed_position": proposed_position,
                "turnover": turnover,
                "transaction_cost": round(cost, 6),
                "price": round(price, 6),
                ("top_themes" if use_themes else "top_topics"): [
                    {group_field: g, "score": round(s, 6)} for g, s in top
                ],
            }
            if veto_reasons:
                trade["vetoes"] = veto_reasons
            trades.append(trade)

        equity_curve.append({
            "date": date,
            "price": round(price, 6),
            "score": round(score, 6),
            "position": target_position,
            "pnl": round(pnl, 6),
            "cost": round(cost, 6),
            "equity": round(equity, 6),
        })

        prev_price = price
        prev_position = target_position

    total_return = (equity / book_capital) - 1.0
    return {
        "name": book_cfg["name"],
        "instrument": instrument,
        "book_capital": book_capital,
        "summary": {
            "book_capital": book_capital,
            "final_equity": round(equity, 6),
            "total_return": round(total_return, 6),
            "num_days": len(equity_curve),
            "num_trades": len(trades),
            "num_vetoed_days": vetoed,
            "scoring_mode": "themes" if use_themes else "topics",
        },
        "equity_curve": equity_curve,
        "trades": trades,
    }


def aggregate_books(book_results: List[dict]) -> dict:
    """Build a portfolio-level equity curve by summing per-date book equities."""
    by_date: Dict[str, float] = defaultdict(float)
    initial_capital = sum(b["book_capital"] for b in book_results)

    for b in book_results:
        for row in b["equity_curve"]:
            by_date[row["date"]] += row["equity"]

    portfolio_curve = [
        {"date": d, "equity": round(eq, 6)}
        for d, eq in sorted(by_date.items())
    ]
    final_equity = portfolio_curve[-1]["equity"] if portfolio_curve else initial_capital
    total_return = (final_equity / initial_capital - 1.0) if initial_capital else 0.0

    return {
        "initial_capital": initial_capital,
        "final_equity": round(final_equity, 6),
        "total_return": round(total_return, 6),
        "num_days": len(portfolio_curve),
        "num_books": len(book_results),
        "num_trades": sum(b["summary"]["num_trades"] for b in book_results),
        "portfolio_curve": portfolio_curve,
    }


def run_multi_book(cfg: dict, score_rows: List[dict], price_rows: List[dict]) -> dict:
    cost_bps = float((cfg.get("global") or {}).get("one_way_cost_bps", 5.0))
    book_results = [
        run_book(book, score_rows, price_rows, cost_bps)
        for book in cfg.get("books", [])
    ]
    portfolio = aggregate_books(book_results)
    return {
        "portfolio": portfolio,
        "books": book_results,
    }

"""Daily snapshot of the composite signal as paper trades.

For each tracked symbol, computes today's composite, classifies into
LONG/SHORT/FLAT (using the same thresholds as the composite backtest),
auto-generates a one-line reasoning string from the factor breakdown,
and writes a paper_trades row. Auto-resolves any previously open
position whose direction differs.

Run manually:
    python scripts/snapshot_paper_trades.py
    python scripts/snapshot_paper_trades.py --date 2026-05-14   # backdated

Run via cron — see ops/crontab (nightly at 03:30, after the composite
backtest finishes at 03:15).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json

import pandas as pd

from app.db.database import get_connection
from app.scoring.composite import composite_score
from app.scoring.factors import positioning_factor, inventory_factor, term_structure_factor
from app.scoring.paper_trading import record_snapshot, evaluate_closes
from app.strategy.backtest_engine import score_to_target_position, aggregate_score_by_date


COMPOSITE_CFG = {
    "entry_threshold_long":         0.10,
    "entry_threshold_short":       -0.10,
    "strong_entry_threshold_long":  0.40,
    "strong_entry_threshold_short": -0.40,
    "base_position":     1.0,
    "strong_position":   2.0,
    "max_abs_position":  2.0,
}

SYMBOLS = [("WTI", "wti_outright"), ("Brent", "brent_outright")]

# Spread book — single-factor signal (term_structure z-score IS the signal),
# no regime conditioning, no composite blending. Same close-rule
# semantics as the outright book.
SPREAD_SYMBOLS = ["WTI_M1M2", "Brent_M1M2"]

# Spread positions use slightly tighter thresholds — fewer, higher-conviction
# spread trades match the retail-edge thesis on this book.
SPREAD_CFG = {
    "entry_threshold_long":          0.30,
    "entry_threshold_short":        -0.30,
    "strong_entry_threshold_long":   0.80,
    "strong_entry_threshold_short": -0.80,
    "base_position":     1.0,
    "strong_position":   2.0,
    "max_abs_position":  2.0,
}


def _load_book_cfg(name: str) -> dict:
    cfg_path = BASE_DIR / "app" / "config" / "multi_strategy_config.json"
    cfg = json.loads(cfg_path.read_text())
    for b in cfg.get("books", []):
        if b.get("name") == name:
            return b
    raise KeyError(f"Book {name!r} not in multi_strategy_config")


def _narrative_z_for_date(book_cfg: dict, theme_scores: pd.DataFrame, asof: date, window: int = 30):
    weights = (book_cfg.get("scoring") or {}).get("theme_weights")
    rows = [
        {"score_date": str(r["score_date"]), "theme": r["theme"],
         "narrative_score": float(r["narrative_score"])}
        for _, r in theme_scores.iterrows()
    ]
    agg = aggregate_score_by_date(rows, weights=weights, group_field="theme")
    if not agg:
        return None
    df = pd.DataFrame(agg).sort_values("score_date").reset_index(drop=True)
    df["score_date"] = df["score_date"].astype(str)
    df["aggregate_score"] = df["aggregate_score"].astype(float)
    asof_iso = asof.isoformat()
    if asof_iso not in df["score_date"].values:
        # Use the last date on or before asof
        before = df[df["score_date"] <= asof_iso]
        if before.empty:
            return None
        df = before
    history = df[df["score_date"] <= asof_iso].tail(window + 1)
    if len(history) < 6:
        return None
    today_val = float(history.iloc[-1]["aggregate_score"])
    prior = history.iloc[:-1]["aggregate_score"]
    mean, std = prior.mean(), prior.std()
    if std == 0 or pd.isna(std):
        return None
    return (today_val - mean) / std


def _latest_close_on_or_before(symbol: str, asof: date, conn) -> float | None:
    row = conn.execute(
        "SELECT close FROM market_prices WHERE symbol=? AND price_time <= ? ORDER BY price_time DESC LIMIT 1",
        (symbol, asof.isoformat()),
    ).fetchone()
    return float(row[0]) if row and row[0] else None


def _latest_settled_trading_day(conn) -> date | None:
    """Most recent weekday (Mon-Fri) with a settled WTI close in market_prices.

    Skips weekend rows that yfinance sometimes produces (Sunday-night reopen,
    partial fills, etc.) so we never create a trade dated to a non-trading
    day. Looks at the last 10 distinct dates to be safe.
    """
    rows = conn.execute(
        "SELECT DISTINCT price_time FROM market_prices "
        "WHERE symbol='WTI' AND close IS NOT NULL "
        "ORDER BY price_time DESC LIMIT 10"
    ).fetchall()
    for (pt,) in rows:
        try:
            d = date.fromisoformat(pt[:10])
        except ValueError:
            continue
        if d.weekday() < 5:  # 0-4 = Mon-Fri
            return d
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None,
                    help="Plan date (YYYY-MM-DD). Defaults to the most recent "
                         "settled trading day (Mon-Fri) in market_prices.")
    args = ap.parse_args()

    conn = get_connection()
    if args.date:
        plan_date = date.fromisoformat(args.date)
    else:
        # Rule: trades can only be opened on settled trading days. Use the
        # most recent weekday (Mon-Fri) for which we have a WTI close.
        # Weekend / pre-settle cron runs hit dedup on the prior day's
        # snapshot and exit cleanly without creating phantom trades.
        plan_date = _latest_settled_trading_day(conn)
        if plan_date is None:
            print("[ERROR] No settled WTI close found in market_prices — cannot pick a plan_date.")
            conn.close()
            return
        days_behind = (date.today() - plan_date).days
        if days_behind > 3:
            print(f"[WARN] Latest settled trading day is {plan_date} ({days_behind} days behind today). "
                  f"Verify fetch_prices is running.")
    theme_scores = pd.read_sql(
        "SELECT score_date, theme, narrative_score FROM daily_theme_scores WHERE commodity='crude_oil'",
        conn,
    )
    print(f"Snapshotting paper trades for {plan_date}.")

    for sym, book_name in SYMBOLS:
        regime_row = conn.execute(
            "SELECT primary_regime FROM daily_regimes WHERE symbol=? AND regime_date <= ? "
            "ORDER BY regime_date DESC LIMIT 1",
            (sym, plan_date.isoformat()),
        ).fetchone()
        regime = regime_row[0] if regime_row else None

        book_cfg = _load_book_cfg(book_name)
        narr_z = _narrative_z_for_date(book_cfg, theme_scores, plan_date)
        try:
            ts = term_structure_factor(sym, plan_date)
        except Exception:
            ts = None
        try:
            pos = positioning_factor(sym, plan_date)
        except Exception:
            pos = None
        try:
            inv = inventory_factor(sym, plan_date)
        except Exception:
            inv = None

        composite = None
        breakdown = []
        if regime and narr_z is not None:
            try:
                out = composite_score(
                    sym, regime, narr_z,
                    {"term_structure": ts, "positioning": pos, "inventory": inv},
                )
                composite = float(out["total"])
                breakdown = out.get("breakdown", [])
            except KeyError:
                composite = None

        target_position = score_to_target_position(composite, COMPOSITE_CFG) if composite is not None else 0.0
        direction = "LONG" if target_position > 0 else ("SHORT" if target_position < 0 else "FLAT")
        latest_close = _latest_close_on_or_before(sym, plan_date, conn)

        # 1) Run close evaluation FIRST — any open positions whose direction
        #    has reversed past the opposite entry threshold close at today's close.
        closed_ids = evaluate_closes(
            symbol=sym,
            asof=plan_date,
            current_composite=composite,
            exit_close=latest_close,
            entry_threshold_long=COMPOSITE_CFG["entry_threshold_long"],
            entry_threshold_short=COMPOSITE_CFG["entry_threshold_short"],
            conn=conn,
        )
        if closed_ids:
            print(f"  {sym}: closed open positions {closed_ids} on reversal")

        # 2) Then record today's snapshot if signal is active (LONG/SHORT past threshold).
        result = record_snapshot(
            plan_date=plan_date,
            symbol=sym,
            direction=direction,
            target_position=target_position,
            composite_score=composite,
            regime=regime,
            narrative_z=narr_z,
            term_structure=ts,
            positioning=pos,
            inventory=inv,
            breakdown=breakdown,
            entry_close=latest_close,
            conn=conn,
        )
        if result.get("skipped_dup"):
            msg = "DUPLICATE skipped (snapshot already exists for today)"
        elif result.get("skipped_flat"):
            msg = "FLAT — no new trade recorded"
        else:
            msg = f"opened new trade id={result['trade_id']}"
        print(f"  {sym}: {direction} {abs(target_position):.0f}x  composite={composite}  ({msg})")
        print(f"    {result['reasoning']}")

    # ---- Spread book ----
    print()
    print("Spread book (term_structure z-score is the signal):")
    for spread_sym in SPREAD_SYMBOLS:
        # Term-structure z-score is the signal (calls into existing factor function)
        try:
            ts_z = term_structure_factor(spread_sym, plan_date)
        except Exception as e:
            ts_z = None
            print(f"  {spread_sym}: term_structure_factor unavailable: {e}")
        target_position = score_to_target_position(ts_z, SPREAD_CFG) if ts_z is not None else 0.0
        direction = "LONG" if target_position > 0 else ("SHORT" if target_position < 0 else "FLAT")
        spread_close = _latest_close_on_or_before(spread_sym, plan_date, conn)

        # 1) Close any opens whose direction reversed past the opposite entry threshold
        closed_ids = evaluate_closes(
            symbol=spread_sym,
            asof=plan_date,
            current_composite=ts_z,
            exit_close=spread_close,
            entry_threshold_long=SPREAD_CFG["entry_threshold_long"],
            entry_threshold_short=SPREAD_CFG["entry_threshold_short"],
            conn=conn,
        )
        if closed_ids:
            print(f"  {spread_sym}: closed open spread positions {closed_ids} on reversal")

        # 2) Open new spread trade if signal active. Composite/breakdown fields
        #    are repurposed for the spread book: composite_score = the z-score,
        #    breakdown = single-row pseudo-breakdown.
        breakdown = [{"factor": "term_structure", "value": ts_z, "weight": 1.0,
                      "contribution": ts_z}] if ts_z is not None else []
        result = record_snapshot(
            plan_date=plan_date,
            symbol=spread_sym,
            direction=direction,
            target_position=target_position,
            composite_score=ts_z,
            regime="spread_book",   # marker so the dashboard knows
            narrative_z=None,
            term_structure=ts_z,
            positioning=None,
            inventory=None,
            breakdown=breakdown,
            entry_close=spread_close,
            conn=conn,
        )
        if result.get("skipped_dup"):
            msg = "DUPLICATE skipped"
        elif result.get("skipped_flat"):
            msg = "FLAT — no new spread trade recorded"
        else:
            msg = f"opened new spread trade id={result['trade_id']}"
        ts_str = f"{ts_z:+.3f}" if ts_z is not None else "n/a"
        spread_str = f"{spread_close:+.2f}" if spread_close is not None else "n/a"
        print(f"  {spread_sym}: {direction} {abs(target_position):.0f}x  z={ts_str}  spread={spread_str}  ({msg})")

    conn.close()


if __name__ == "__main__":
    main()

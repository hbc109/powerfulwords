"""Backtest the composite signal vs narrative-only baseline.

For each trading day in the overlap window:
  1. Compute narrative z-score, positioning_factor, inventory_factor
     (term_structure excluded — historical data is biased proxy).
  2. Compute composite_full   = composite_score(regime, narr, factors)
     and  composite_narr_only = narr_z (the baseline).
  3. Look at next-5-day and next-10-day price return on flat WTI.
  4. Record direction agreement (sign hit / miss) per signal.

Aggregate: per-regime and per-magnitude-bucket hit rates for both
signals, side-by-side. If composite shows uplift over narrative-only
in some regime, the regime weights are earning their keep.

Run:
    python scripts/backtest_composite.py
Output:
    data/processed/backtests/composite_validation.json  (full record)
    stdout summary table
"""

from __future__ import annotations

from pathlib import Path
import sys
import json
from datetime import date, timedelta
from typing import Optional

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd

from app.db.database import get_connection
from app.scoring.factors import positioning_factor, inventory_factor
from app.scoring.composite import composite_score
from app.strategy.backtest_engine import aggregate_score_by_date, load_strategy_config


SYMBOLS = ["WTI", "Brent"]
SIGNAL_THRESHOLD = 0.1   # |score| > this counts as a non-flat signal
FORWARD_HORIZONS = [5, 10]
WTI_BOOK = "wti_outright"
BRENT_BOOK = "brent_outright"


def _load_book_cfg(name: str) -> dict:
    cfg_path = BASE_DIR / "app" / "config" / "multi_strategy_config.json"
    cfg = json.loads(cfg_path.read_text())
    for b in cfg.get("books", []):
        if b.get("name") == name:
            return b
    raise KeyError(f"Book {name!r} not found in multi_strategy_config")


def _narrative_z_series(book_cfg: dict, theme_scores: pd.DataFrame, window: int = 30) -> pd.Series:
    """Reproduce _book_history_score() from the dashboard but vectorized:
    rolling 30d z-score of the book's weighted theme score, indexed by date."""
    weights = (book_cfg.get("scoring") or {}).get("theme_weights")
    rows = [
        {"score_date": str(r["score_date"]), "theme": r["theme"],
         "narrative_score": float(r["narrative_score"])}
        for _, r in theme_scores.iterrows()
    ]
    agg = aggregate_score_by_date(rows, weights=weights, group_field="theme")
    if not agg:
        return pd.Series(dtype=float)
    df = pd.DataFrame(agg).sort_values("score_date").reset_index(drop=True)
    df["score_date"] = df["score_date"].astype(str)
    df["aggregate_score"] = df["aggregate_score"].astype(float)
    rolling_mean = df["aggregate_score"].rolling(window=window, min_periods=5).mean().shift(1)
    rolling_std = df["aggregate_score"].rolling(window=window, min_periods=5).std().shift(1)
    z = (df["aggregate_score"] - rolling_mean) / rolling_std
    return pd.Series(z.values, index=df["score_date"].values, dtype=float)


def _hit(score: float, fwd_ret: float) -> Optional[bool]:
    """True/False if non-flat signal agrees with sign of fwd return; None if flat."""
    if pd.isna(score) or pd.isna(fwd_ret):
        return None
    if abs(score) <= SIGNAL_THRESHOLD:
        return None
    return (score > 0) == (fwd_ret > 0)


def backtest_symbol(symbol: str, book_name: str) -> dict:
    print(f"\n=== {symbol} ({book_name}) ===")
    conn = get_connection()

    prices = pd.read_sql(
        "SELECT price_time AS date, close FROM market_prices "
        "WHERE symbol=? AND close IS NOT NULL ORDER BY price_time",
        conn, params=(symbol,),
    )
    prices["date"] = prices["date"].astype(str).str[:10]
    prices = prices.drop_duplicates(subset=["date"]).reset_index(drop=True)
    for h in FORWARD_HORIZONS:
        prices[f"fwd_{h}d"] = prices["close"].shift(-h) / prices["close"] - 1.0

    regimes = pd.read_sql(
        "SELECT regime_date AS date, primary_regime FROM daily_regimes WHERE symbol=?",
        conn, params=(symbol,),
    )
    regimes["date"] = regimes["date"].astype(str).str[:10]
    regimes_by_date = dict(zip(regimes["date"], regimes["primary_regime"]))

    theme_scores = pd.read_sql(
        "SELECT score_date, theme, narrative_score FROM daily_theme_scores WHERE commodity='crude_oil'",
        conn,
    )
    conn.close()

    book_cfg = _load_book_cfg(book_name)
    narr_z = _narrative_z_series(book_cfg, theme_scores, window=30)

    records = []
    for _, row in prices.iterrows():
        d = row["date"]
        regime = regimes_by_date.get(d)
        if not regime:
            continue
        nz = float(narr_z.get(d, float("nan")))
        if pd.isna(nz):
            continue
        try:
            asof = date.fromisoformat(d)
            pos = positioning_factor(symbol, asof)
            inv = inventory_factor(symbol, asof)
        except Exception:
            pos, inv = None, None
        try:
            full = composite_score(symbol, regime, nz, {"positioning": pos, "inventory": inv})["total"]
        except KeyError:
            continue
        rec = {
            "date": d, "regime": regime,
            "narr_z": nz, "pos": pos, "inv": inv,
            "composite": full,
        }
        for h in FORWARD_HORIZONS:
            rec[f"fwd_{h}d"] = float(row[f"fwd_{h}d"]) if not pd.isna(row[f"fwd_{h}d"]) else None
        records.append(rec)

    df = pd.DataFrame(records)
    print(f"  Records: {len(df)}  date range: {df['date'].min()} → {df['date'].max()}")

    summary = {"symbol": symbol, "n_records": len(df), "by_regime": {}, "overall": {}}
    for h in FORWARD_HORIZONS:
        for label, score_col in [("composite", "composite"), ("narrative_only", "narr_z")]:
            hits = df.apply(lambda r: _hit(r[score_col], r[f"fwd_{h}d"]), axis=1)
            taken = hits.dropna()
            n = len(taken)
            hr = float(taken.sum() / n) if n else None
            summary["overall"].setdefault(f"fwd_{h}d", {})[label] = {"hit_rate": hr, "n_trades": n, "coverage": n / len(df) if len(df) else 0}

    for regime in sorted(df["regime"].unique()):
        sub = df[df["regime"] == regime]
        summary["by_regime"][regime] = {"n": len(sub)}
        for h in FORWARD_HORIZONS:
            for label, score_col in [("composite", "composite"), ("narrative_only", "narr_z")]:
                hits = sub.apply(lambda r: _hit(r[score_col], r[f"fwd_{h}d"]), axis=1)
                taken = hits.dropna()
                n = len(taken)
                hr = float(taken.sum() / n) if n else None
                summary["by_regime"][regime].setdefault(f"fwd_{h}d", {})[label] = {"hit_rate": hr, "n_trades": n}

    return {"summary": summary, "records": records}


def print_summary(label: str, summary: dict) -> None:
    print(f"\n--- {label} ---")
    print(f"Total records: {summary['n_records']}")
    print(f"\nOverall (across all regimes):")
    for h_key, lbls in summary["overall"].items():
        line = f"  {h_key}: "
        for lbl in ("composite", "narrative_only"):
            d = lbls[lbl]
            hr = f"{d['hit_rate']:.3f}" if d["hit_rate"] is not None else "n/a"
            line += f"{lbl}={hr} (n={d['n_trades']})  "
        print(line)
    print(f"\nBy regime (5-day fwd return):")
    print(f"  {'regime':<16} {'n':>4}  {'composite':>15}  {'narr_only':>15}  uplift")
    for regime, d in summary["by_regime"].items():
        h = d["fwd_5d"]
        c, nbase = h["composite"], h["narrative_only"]
        c_hr = c["hit_rate"]; n_hr = nbase["hit_rate"]
        uplift = (c_hr - n_hr) if (c_hr is not None and n_hr is not None) else None
        c_str = f"{c_hr:.3f} (n={c['n_trades']})" if c_hr is not None else f"n/a (n=0)"
        n_str = f"{n_hr:.3f} (n={nbase['n_trades']})" if n_hr is not None else f"n/a (n=0)"
        u_str = f"{uplift:+.3f}" if uplift is not None else "n/a"
        print(f"  {regime:<16} {d['n']:>4}  {c_str:>15}  {n_str:>15}  {u_str}")


def main() -> None:
    out_dir = BASE_DIR / "data" / "processed" / "backtests"
    out_dir.mkdir(parents=True, exist_ok=True)
    all_results = {}
    for sym, book in [("WTI", WTI_BOOK), ("Brent", BRENT_BOOK)]:
        try:
            result = backtest_symbol(sym, book)
            print_summary(f"{sym} ({book})", result["summary"])
            all_results[sym] = result
        except Exception as e:
            print(f"[ERROR] {sym}: {type(e).__name__}: {e}")

    out_path = out_dir / "composite_validation.json"
    out_path.write_text(json.dumps(
        {sym: r["summary"] for sym, r in all_results.items()},
        ensure_ascii=False, indent=2,
    ))
    print(f"\nFull summary written to {out_path}")


if __name__ == "__main__":
    main()

"""Composite-signal P&L backtest.

Sister of scripts/run_backtest.py (the baseline narrative-only backtest)
but uses the regime-conditional composite_score() instead of raw
narrative-weighted theme scores. Same PnL machinery (close-to-close
return × position, transaction cost in bps), so results are directly
comparable to the baseline.

For each symbol (WTI, Brent):
  1. Loop trading days.
  2. Compute narrative_z, positioning_factor, inventory_factor.
     (term_structure_factor excluded — historical data is biased
     deferred-spread proxy.)
  3. composite = composite_score(symbol, regime, narr_z, factors)['total']
  4. target_position = composite_to_target_position(composite)
  5. Compound equity, record trade on turnover.

Output:
  data/processed/backtests/composite_pnl_<symbol>.json
  same shape as run_backtest.py output (summary + equity_curve + trades)
  plus a per-regime breakdown for context.

Run:
    python scripts/run_composite_backtest.py
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd

from app.db.database import get_connection
from app.scoring.composite import composite_score
from app.scoring.factors import positioning_factor, inventory_factor
from app.strategy.backtest_engine import score_to_target_position, aggregate_score_by_date


# Composite scores live on z-score scale (≈ [-1.5, +1.5]); thresholds
# below are tuned for that range, not the raw narrative weighted-score
# range used by the baseline backtest.
COMPOSITE_CFG = {
    "entry_threshold_long":         0.10,
    "entry_threshold_short":       -0.10,
    "strong_entry_threshold_long":  0.40,
    "strong_entry_threshold_short": -0.40,
    "base_position":     1.0,
    "strong_position":   2.0,
    "max_abs_position":  2.0,
    "one_way_cost_bps":  5.0,
    "initial_capital":   100000.0,
    "hold_if_same_sign": True,
    "flat_if_neutral":   True,
}

SYMBOLS = [("WTI", "wti_outright"), ("Brent", "brent_outright")]


def _load_book_cfg(name: str) -> dict:
    cfg_path = BASE_DIR / "app" / "config" / "multi_strategy_config.json"
    cfg = json.loads(cfg_path.read_text())
    for b in cfg.get("books", []):
        if b.get("name") == name:
            return b
    raise KeyError(f"Book {name!r} not in multi_strategy_config")


def _narrative_z_series(book_cfg: dict, theme_scores: pd.DataFrame, window: int = 30) -> pd.Series:
    """Rolling 30d z of the book's weighted theme score, indexed by date."""
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


def backtest_symbol(symbol: str, book_name: str) -> dict:
    conn = get_connection()
    prices = pd.read_sql(
        "SELECT price_time AS date, close FROM market_prices "
        "WHERE symbol=? AND close IS NOT NULL ORDER BY price_time",
        conn, params=(symbol,),
    )
    prices["date"] = prices["date"].astype(str).str[:10]
    prices = prices.drop_duplicates(subset=["date"]).reset_index(drop=True)

    regimes_df = pd.read_sql(
        "SELECT regime_date AS date, primary_regime FROM daily_regimes WHERE symbol=?",
        conn, params=(symbol,),
    )
    regimes_df["date"] = regimes_df["date"].astype(str).str[:10]
    regime_by_date = dict(zip(regimes_df["date"], regimes_df["primary_regime"]))

    theme_scores = pd.read_sql(
        "SELECT score_date, theme, narrative_score FROM daily_theme_scores WHERE commodity='crude_oil'",
        conn,
    )
    conn.close()

    book_cfg = _load_book_cfg(book_name)
    narr_z = _narrative_z_series(book_cfg, theme_scores, window=30)

    capital = float(COMPOSITE_CFG["initial_capital"])
    one_way_cost = COMPOSITE_CFG["one_way_cost_bps"] / 10000.0
    prev_close = None
    prev_position = 0.0
    equity_curve = []
    trades = []
    by_regime: dict[str, dict] = defaultdict(lambda: {"n_trades": 0, "hits": 0, "non_flat_days": 0, "pnl_sum": 0.0})

    for _, row in prices.iterrows():
        d = row["date"]
        close_px = float(row["close"])
        regime = regime_by_date.get(d)

        composite = None
        narr = None
        pos = inv = None
        breakdown = []
        if regime is not None:
            nz = narr_z.get(d, float("nan"))
            if pd.notna(nz):
                narr = float(nz)
                try:
                    asof = date.fromisoformat(d)
                    pos = positioning_factor(symbol, asof)
                    inv = inventory_factor(symbol, asof)
                    out = composite_score(symbol, regime, narr, {"positioning": pos, "inventory": inv})
                    composite = float(out["total"])
                    breakdown = out.get("breakdown", [])
                except (KeyError, Exception):
                    composite = None

        target_position = score_to_target_position(composite, COMPOSITE_CFG) if composite is not None else 0.0

        pnl = 0.0
        if prev_close is not None and prev_close != 0:
            ret = (close_px / prev_close) - 1.0
            pnl = capital * prev_position * ret

        turnover = abs(target_position - prev_position)
        cost = capital * turnover * one_way_cost
        capital = capital + pnl - cost

        if turnover > 0:
            trades.append({
                "date": d, "regime": regime,
                "composite": round(composite, 6) if composite is not None else None,
                "narrative_z": round(narr, 4) if narr is not None else None,
                "positioning": round(pos, 4) if pos is not None else None,
                "inventory": round(inv, 4) if inv is not None else None,
                "breakdown": breakdown,  # per-factor [{factor, value, weight, contribution}]
                "prev_position": prev_position,
                "target_position": target_position,
                "turnover": turnover,
                "transaction_cost": round(cost, 6),
                "entry_close": close_px,
                # Filled in after the loop: exit_date, exit_close, holding_days, realized_pnl_pct
            })
            if regime:
                by_regime[regime]["n_trades"] += 1

        if regime and target_position != 0:
            by_regime[regime]["non_flat_days"] += 1
            by_regime[regime]["pnl_sum"] += pnl
            if pnl > 0:
                by_regime[regime]["hits"] += 1

        equity_curve.append({
            "date": d,
            "close": close_px,
            "regime": regime,
            "composite": round(composite, 6) if composite is not None else None,
            "position": target_position,
            "pnl": round(pnl, 6),
            "cost": round(cost, 6),
            "equity": round(capital, 6),
        })
        prev_close = close_px
        prev_position = target_position

    # Post-process: for each trade, compute the realized result of the
    # position taken (held until the NEXT trade flips/exits it). Records
    # exit_date, exit_close, holding_days, realized_pnl_pct (signed by
    # position direction, before transaction cost).
    for i, tr in enumerate(trades):
        if i + 1 < len(trades):
            nxt = trades[i + 1]
            tr["exit_date"] = nxt["date"]
            tr["exit_close"] = nxt["entry_close"]
        else:
            # Open at end of backtest — mark to last close
            tr["exit_date"] = equity_curve[-1]["date"] if equity_curve else None
            tr["exit_close"] = equity_curve[-1]["close"] if equity_curve else None
        if tr["entry_close"] and tr["exit_close"]:
            raw_ret = (tr["exit_close"] / tr["entry_close"]) - 1.0
            # Realized PnL of the position TAKEN by this trade
            tr["realized_pnl_pct"] = round(raw_ret * tr["target_position"], 6)
        else:
            tr["realized_pnl_pct"] = None
        try:
            d_in = date.fromisoformat(tr["date"])
            d_out = date.fromisoformat(tr["exit_date"]) if tr["exit_date"] else None
            tr["holding_days"] = (d_out - d_in).days if d_out else None
        except Exception:
            tr["holding_days"] = None

    initial_capital = float(COMPOSITE_CFG["initial_capital"])
    total_return = (capital / initial_capital) - 1.0
    daily_rets = []
    prev_eq = None
    for r in equity_curve:
        if prev_eq is not None and prev_eq != 0:
            daily_rets.append((r["equity"] / prev_eq) - 1.0)
        prev_eq = r["equity"]
    if daily_rets:
        mean_r = sum(daily_rets) / len(daily_rets)
        var_r = sum((r - mean_r) ** 2 for r in daily_rets) / len(daily_rets)
        std_r = var_r ** 0.5
        sharpe_ann = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else None
    else:
        sharpe_ann = None
    eq_series = [r["equity"] for r in equity_curve]
    if eq_series:
        peak = eq_series[0]
        max_dd = 0.0
        for v in eq_series:
            peak = max(peak, v)
            if peak > 0:
                dd = (v / peak) - 1.0
                max_dd = min(max_dd, dd)
    else:
        max_dd = None
    hit_days = [1 if r > 0 else 0 for r in daily_rets]
    positive_day_rate = sum(hit_days) / len(hit_days) if hit_days else None

    summary = {
        "symbol": symbol,
        "initial_capital": round(initial_capital, 2),
        "final_equity": round(capital, 2),
        "total_return": round(total_return, 6),
        "annualized_sharpe": round(sharpe_ann, 4) if sharpe_ann is not None else None,
        "max_drawdown": round(max_dd, 6) if max_dd is not None else None,
        "num_days": len(equity_curve),
        "num_trades": len(trades),
        "positive_day_rate": round(positive_day_rate, 6) if positive_day_rate is not None else None,
    }
    regime_summary = {}
    for r, agg in by_regime.items():
        regime_summary[r] = {
            "n_trades": agg["n_trades"],
            "non_flat_days": agg["non_flat_days"],
            "pnl_sum": round(agg["pnl_sum"], 2),
            "pnl_per_day": round(agg["pnl_sum"] / agg["non_flat_days"], 2) if agg["non_flat_days"] else None,
            "day_hit_rate": round(agg["hits"] / agg["non_flat_days"], 4) if agg["non_flat_days"] else None,
        }

    return {
        "summary": summary,
        "by_regime": regime_summary,
        "equity_curve": equity_curve,
        "trades": trades,
    }


def main() -> None:
    out_dir = BASE_DIR / "data" / "processed" / "backtests"
    out_dir.mkdir(parents=True, exist_ok=True)
    for sym, book in SYMBOLS:
        print(f"\n=== {sym} ({book}) ===")
        result = backtest_symbol(sym, book)
        s = result["summary"]
        print(f"  days={s['num_days']}  trades={s['num_trades']}  "
              f"final_equity={s['final_equity']:,.2f}  total_return={s['total_return']:+.2%}  "
              f"sharpe={s['annualized_sharpe']}  max_dd={s['max_drawdown']:+.2%}  "
              f"positive_day_rate={s['positive_day_rate']:.3f}" if s['positive_day_rate'] else f"  days={s['num_days']}")
        if result["by_regime"]:
            print("  Per regime:")
            for r, agg in sorted(result["by_regime"].items()):
                hr = f"{agg['day_hit_rate']:.3f}" if agg['day_hit_rate'] is not None else "n/a"
                ppd = f"{agg['pnl_per_day']:.2f}" if agg['pnl_per_day'] is not None else "n/a"
                print(f"    {r:<16}  trades={agg['n_trades']:>3}  active_days={agg['non_flat_days']:>3}  "
                      f"pnl_per_day={ppd:>9}  day_hit={hr}")
        out_path = out_dir / f"composite_pnl_{sym}.json"
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"  Wrote {out_path.relative_to(BASE_DIR)}")


if __name__ == "__main__":
    main()

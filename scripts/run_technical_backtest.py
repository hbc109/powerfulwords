"""Technical-only P&L backtest.

Third comparator alongside Composite (multi-factor regime-conditional)
and Baseline (narrative-only). Uses `technical_signal()` — pure
indicator-driven (regime tag + ADX trend filter + BB %B mean-revert),
no narrative, no positioning, no inventory.

Output per symbol to data/processed/backtests/technical_pnl_<sym>.json
in the same shape as the composite backtest, so the dashboard renders
it with identical UI.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd

from app.db.database import get_connection
from app.scoring.technical_signal import technical_signal
from app.strategy.backtest_engine import score_to_target_position


TECHNICAL_CFG = {
    "entry_threshold_long":          0.10,
    "entry_threshold_short":        -0.10,
    "strong_entry_threshold_long":   0.75,
    "strong_entry_threshold_short": -0.75,
    "base_position":     1.0,
    "strong_position":   2.0,
    "max_abs_position":  2.0,
    "one_way_cost_bps":  5.0,
    "initial_capital":   100000.0,
}

SYMBOLS = ["WTI", "Brent"]


def backtest_symbol(symbol: str) -> dict:
    conn = get_connection()
    prices = pd.read_sql(
        "SELECT price_time AS date, close FROM market_prices "
        "WHERE symbol=? AND close IS NOT NULL ORDER BY price_time",
        conn, params=(symbol,),
    )
    prices["date"] = prices["date"].astype(str).str[:10]
    prices = prices.drop_duplicates(subset=["date"]).reset_index(drop=True)

    capital = float(TECHNICAL_CFG["initial_capital"])
    one_way_cost = TECHNICAL_CFG["one_way_cost_bps"] / 10000.0
    prev_close = None
    prev_position = 0.0
    equity_curve = []
    trades = []
    by_regime: dict[str, dict] = defaultdict(
        lambda: {"n_trades": 0, "hits": 0, "non_flat_days": 0, "pnl_sum": 0.0})

    for _, row in prices.iterrows():
        d = row["date"]
        close_px = float(row["close"])

        try:
            sig = technical_signal(symbol, date.fromisoformat(d), conn=conn)
        except Exception:
            sig = {"total": 0.0, "regime": None, "breakdown": [], "reasoning": "error"}
        score = sig["total"]
        regime = sig.get("regime")
        target_position = score_to_target_position(score, TECHNICAL_CFG)

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
                "technical_score": round(score, 6),
                "reasoning": sig.get("reasoning"),
                "breakdown": sig.get("breakdown", []),
                "prev_position": prev_position,
                "target_position": target_position,
                "turnover": turnover,
                "transaction_cost": round(cost, 6),
                "entry_close": close_px,
            })
            if regime:
                by_regime[regime]["n_trades"] += 1

        if regime and target_position != 0:
            by_regime[regime]["non_flat_days"] += 1
            by_regime[regime]["pnl_sum"] += pnl
            if pnl > 0:
                by_regime[regime]["hits"] += 1

        equity_curve.append({
            "date": d, "close": close_px, "regime": regime,
            "technical_score": round(score, 6),
            "position": target_position,
            "pnl": round(pnl, 6),
            "cost": round(cost, 6),
            "equity": round(capital, 6),
        })
        prev_close = close_px
        prev_position = target_position

    # Compute exit fields for closed trades
    for i, tr in enumerate(trades):
        if i + 1 < len(trades):
            nxt = trades[i + 1]
            tr["exit_date"] = nxt["date"]
            tr["exit_close"] = nxt["entry_close"]
        else:
            tr["exit_date"] = equity_curve[-1]["date"] if equity_curve else None
            tr["exit_close"] = equity_curve[-1]["close"] if equity_curve else None
        if tr["entry_close"] and tr["exit_close"]:
            raw_ret = (tr["exit_close"] / tr["entry_close"]) - 1.0
            tr["realized_pnl_pct"] = round(raw_ret * tr["target_position"], 6)
        else:
            tr["realized_pnl_pct"] = None
        try:
            d_in = date.fromisoformat(tr["date"])
            d_out = date.fromisoformat(tr["exit_date"]) if tr["exit_date"] else None
            tr["holding_days"] = (d_out - d_in).days if d_out else None
        except Exception:
            tr["holding_days"] = None

    initial_capital = float(TECHNICAL_CFG["initial_capital"])
    total_return = (capital / initial_capital) - 1.0
    daily_rets = []
    prev_eq = None
    for r in equity_curve:
        if prev_eq is not None and prev_eq != 0:
            daily_rets.append((r["equity"] / prev_eq) - 1.0)
        prev_eq = r["equity"]
    if daily_rets:
        mean_r = sum(daily_rets) / len(daily_rets)
        var_r = sum((rr - mean_r) ** 2 for rr in daily_rets) / len(daily_rets)
        std_r = var_r ** 0.5
        sharpe_ann = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else None
    else:
        sharpe_ann = None
    eq_series = [r["equity"] for r in equity_curve]
    if eq_series:
        peak = eq_series[0]; max_dd = 0.0
        for v in eq_series:
            peak = max(peak, v)
            if peak > 0:
                max_dd = min(max_dd, (v / peak) - 1.0)
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

    conn.close()
    return {"summary": summary, "by_regime": regime_summary,
            "equity_curve": equity_curve, "trades": trades}


def main() -> None:
    out_dir = BASE_DIR / "data" / "processed" / "backtests"
    out_dir.mkdir(parents=True, exist_ok=True)
    for sym in SYMBOLS:
        print(f"\n=== {sym} (technical-only) ===")
        result = backtest_symbol(sym)
        s = result["summary"]
        print(f"  days={s['num_days']}  trades={s['num_trades']}  "
              f"final_equity={s['final_equity']:,.2f}  total_return={s['total_return']:+.2%}  "
              f"sharpe={s['annualized_sharpe']}  max_dd={s['max_drawdown']:+.2%}")
        if result["by_regime"]:
            print("  Per regime:")
            for r, agg in sorted(result["by_regime"].items()):
                hr = f"{agg['day_hit_rate']:.3f}" if agg['day_hit_rate'] is not None else "n/a"
                ppd = f"{agg['pnl_per_day']:.2f}" if agg['pnl_per_day'] is not None else "n/a"
                print(f"    {r:<16}  trades={agg['n_trades']:>3}  active_days={agg['non_flat_days']:>3}  "
                      f"pnl_per_day={ppd:>9}  day_hit={hr}")
        out_path = out_dir / f"technical_pnl_{sym}.json"
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"  Wrote {out_path.relative_to(BASE_DIR)}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "app" / "config" / "strategy_config.json"


def load_strategy_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def score_to_target_position(score: float, cfg: dict) -> float:
    if score >= cfg["strong_entry_threshold_long"]:
        return min(cfg["strong_position"], cfg["max_abs_position"])
    if score >= cfg["entry_threshold_long"]:
        return min(cfg["base_position"], cfg["max_abs_position"])
    if score <= cfg["strong_entry_threshold_short"]:
        return max(-cfg["strong_position"], -cfg["max_abs_position"])
    if score <= cfg["entry_threshold_short"]:
        return max(-cfg["base_position"], -cfg["max_abs_position"])
    return 0.0 if cfg.get("flat_if_neutral", True) else 0.0


def aggregate_score_by_date(score_rows: List[dict]) -> List[dict]:
    by_date: Dict[str, float] = {}
    for row in score_rows:
        d = row["score_date"]
        by_date[d] = by_date.get(d, 0.0) + float(row["narrative_score"])
    out = [{"score_date": d, "aggregate_score": s} for d, s in sorted(by_date.items())]
    return out


def build_close_map(price_rows: List[dict]) -> Dict[str, float]:
    out = {}
    for r in sorted(price_rows, key=lambda x: x["price_time"]):
        out[str(r["price_time"])[:10]] = float(r["close"])
    return out


def ordered_dates(price_rows: List[dict]) -> List[str]:
    return [str(r["price_time"])[:10] for r in sorted(price_rows, key=lambda x: x["price_time"])]


def run_daily_backtest(score_rows: List[dict], price_rows: List[dict], cfg: dict) -> dict:
    score_by_date = {r["score_date"]: float(r["aggregate_score"]) for r in aggregate_score_by_date(score_rows)}
    close_map = build_close_map(price_rows)
    dates = ordered_dates(price_rows)

    capital = float(cfg["initial_capital"])
    one_way_cost_rate = float(cfg["one_way_cost_bps"]) / 10000.0

    prev_close = None
    prev_position = 0.0
    equity_curve = []
    trades = []

    for d in dates:
        close_px = close_map[d]
        score = score_by_date.get(d, 0.0)
        target_position = score_to_target_position(score, cfg)

        pnl = 0.0
        if prev_close is not None and prev_close != 0:
            ret = (close_px / prev_close) - 1.0
            pnl = capital * prev_position * ret

        turnover = abs(target_position - prev_position)
        cost = capital * turnover * one_way_cost_rate
        capital = capital + pnl - cost

        if turnover > 0:
            trades.append(
                {
                    "date": d,
                    "score": score,
                    "prev_position": prev_position,
                    "target_position": target_position,
                    "turnover": turnover,
                    "transaction_cost": round(cost, 6),
                    "close": close_px,
                }
            )

        equity_curve.append(
            {
                "date": d,
                "close": close_px,
                "score": round(score, 6),
                "position": target_position,
                "pnl": round(pnl, 6),
                "cost": round(cost, 6),
                "equity": round(capital, 6),
            }
        )

        prev_close = close_px
        prev_position = target_position

    total_return = (capital / float(cfg["initial_capital"])) - 1.0
    daily_rets = []
    prev_eq = None
    for row in equity_curve:
        if prev_eq is not None and prev_eq != 0:
            daily_rets.append((row["equity"] / prev_eq) - 1.0)
        prev_eq = row["equity"]

    hit_days = [1 if r > 0 else 0 for r in daily_rets]
    summary = {
        "initial_capital": float(cfg["initial_capital"]),
        "final_equity": round(capital, 6),
        "total_return": round(total_return, 6),
        "num_days": len(equity_curve),
        "num_trades": len(trades),
        "positive_day_rate": round(sum(hit_days) / len(hit_days), 6) if hit_days else None,
    }

    return {
        "summary": summary,
        "equity_curve": equity_curve,
        "trades": trades,
    }

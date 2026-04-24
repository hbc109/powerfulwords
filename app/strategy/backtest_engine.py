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
    return 0.0


def aggregate_score_by_date(
    score_rows: List[dict],
    weights: Dict[str, float] | None = None,
    group_field: str = "topic",
) -> List[dict]:
    """Sum (optionally weighted) per-group scores per day, keeping the breakdown.

    Each output row includes:
      - aggregate_score: sum of (weight * narrative_score) across groups that day
      - breakdown: list of (group, weighted_score), sorted by |weighted_score|
      - raw_breakdown: list of (group, raw_score) preserving the un-weighted view
    """
    weights = weights or {}
    by_date_total: Dict[str, float] = {}
    by_date_breakdown: Dict[str, list] = {}
    by_date_raw: Dict[str, list] = {}
    for row in score_rows:
        d = row["score_date"]
        group = row[group_field]
        raw = float(row["narrative_score"])
        w = float(weights.get(group, 1.0))
        weighted = w * raw
        by_date_total[d] = by_date_total.get(d, 0.0) + weighted
        by_date_breakdown.setdefault(d, []).append((group, weighted))
        by_date_raw.setdefault(d, []).append((group, raw))
    out = []
    for d in sorted(by_date_total):
        breakdown = sorted(by_date_breakdown[d], key=lambda x: abs(x[1]), reverse=True)
        raw = sorted(by_date_raw[d], key=lambda x: abs(x[1]), reverse=True)
        out.append({
            "score_date": d,
            "aggregate_score": by_date_total[d],
            "breakdown": breakdown,
            "raw_breakdown": raw,
        })
    return out


def apply_theme_vetoes(
    target_position: float,
    theme_scores_today: Dict[str, float],
    vetoes: List[dict],
) -> tuple[float, list]:
    """Force position to 0 when any veto blocks the proposed direction.

    Returns (adjusted_position, list_of_veto_reasons_triggered).
    Veto schema: {if_theme, is: above|below, value, blocks: long|short, note?}
    """
    if not vetoes or target_position == 0:
        return target_position, []
    triggered = []
    for v in vetoes:
        theme = v.get("if_theme")
        score = theme_scores_today.get(theme)
        if score is None:
            continue
        op = v.get("is", "above")
        threshold = float(v.get("value", 0))
        cond_met = (op == "above" and score >= threshold) or (op == "below" and score <= threshold)
        if not cond_met:
            continue
        blocks = v.get("blocks", "long")
        if (blocks == "long" and target_position > 0) or (blocks == "short" and target_position < 0):
            triggered.append({
                "if_theme": theme,
                "theme_score": round(score, 6),
                "is": op,
                "value": threshold,
                "blocks": blocks,
                "note": v.get("note"),
            })
    if triggered:
        return 0.0, triggered
    return target_position, []


def build_close_map(price_rows: List[dict]) -> Dict[str, float]:
    out = {}
    for r in sorted(price_rows, key=lambda x: x["price_time"]):
        out[str(r["price_time"])[:10]] = float(r["close"])
    return out


def ordered_dates(price_rows: List[dict]) -> List[str]:
    return [str(r["price_time"])[:10] for r in sorted(price_rows, key=lambda x: x["price_time"])]


def run_daily_backtest(score_rows: List[dict], price_rows: List[dict], cfg: dict) -> dict:
    scoring_cfg = cfg.get("scoring") or {}
    use_themes = bool(scoring_cfg.get("use_themes", False))
    group_field = "theme" if use_themes else "topic"
    weights = scoring_cfg.get("theme_weights") if use_themes else None
    vetoes = scoring_cfg.get("theme_vetoes", []) if use_themes else []

    aggregated = aggregate_score_by_date(score_rows, weights=weights, group_field=group_field)
    score_by_date = {r["score_date"]: float(r["aggregate_score"]) for r in aggregated}
    breakdown_by_date = {r["score_date"]: r["breakdown"] for r in aggregated}
    raw_breakdown_by_date = {r["score_date"]: r["raw_breakdown"] for r in aggregated}

    close_map = build_close_map(price_rows)
    dates = ordered_dates(price_rows)

    capital = float(cfg["initial_capital"])
    one_way_cost_rate = float(cfg["one_way_cost_bps"]) / 10000.0

    prev_close = None
    prev_position = 0.0
    equity_curve = []
    trades = []
    vetoed_count = 0

    for d in dates:
        close_px = close_map[d]
        score = score_by_date.get(d, 0.0)
        proposed_position = score_to_target_position(score, cfg)

        veto_reasons = []
        if use_themes:
            todays_theme_scores = dict(raw_breakdown_by_date.get(d, []))
            target_position, veto_reasons = apply_theme_vetoes(
                proposed_position, todays_theme_scores, vetoes
            )
        else:
            target_position = proposed_position
        if veto_reasons:
            vetoed_count += 1

        pnl = 0.0
        if prev_close is not None and prev_close != 0:
            ret = (close_px / prev_close) - 1.0
            pnl = capital * prev_position * ret

        turnover = abs(target_position - prev_position)
        cost = capital * turnover * one_way_cost_rate
        capital = capital + pnl - cost

        if turnover > 0:
            top = breakdown_by_date.get(d, [])[:3]
            trade = {
                "date": d,
                "score": score,
                "prev_position": prev_position,
                "target_position": target_position,
                "proposed_position": proposed_position,
                "turnover": turnover,
                "transaction_cost": round(cost, 6),
                "close": close_px,
                ("top_themes" if use_themes else "top_topics"): [
                    {group_field: g, "score": round(s, 6)} for g, s in top
                ],
            }
            if veto_reasons:
                trade["vetoes"] = veto_reasons
            trades.append(trade)

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
        "num_vetoed_days": vetoed_count,
        "positive_day_rate": round(sum(hit_days) / len(hit_days), 6) if hit_days else None,
        "scoring_mode": "themes" if use_themes else "topics",
    }

    return {
        "summary": summary,
        "equity_curve": equity_curve,
        "trades": trades,
    }

"""Compute today's per-book trade recommendations.

Used by both the dashboard (Recommendations tab) and the morning
digest script. Operates on a single-day slice of theme scores; for
each book in multi_strategy_config.json it returns the direction,
target position, weighted score, top theme drivers, and any veto
that fired.
"""

from __future__ import annotations

from typing import List

from app.strategy.backtest_engine import (
    aggregate_score_by_date,
    apply_theme_vetoes,
    score_to_target_position,
)


def compute_recommendations(theme_score_rows: List[dict], multi_cfg: dict) -> List[dict]:
    """`theme_score_rows` is a list of {score_date, theme, narrative_score}
    for one date. `multi_cfg` is the loaded multi_strategy_config.

    Returns one dict per book.
    """
    if not theme_score_rows or multi_cfg is None:
        return []

    raw_today = {r["theme"]: float(r["narrative_score"]) for r in theme_score_rows}
    recs = []
    for book in multi_cfg.get("books", []):
        scoring_cfg = book.get("scoring") or {}
        weights = scoring_cfg.get("theme_weights")
        vetoes = scoring_cfg.get("theme_vetoes", [])
        agg = aggregate_score_by_date(theme_score_rows, weights=weights, group_field="theme")
        if not agg:
            continue
        weighted = float(agg[0]["aggregate_score"])
        breakdown = agg[0]["breakdown"][:3]
        proposed = score_to_target_position(weighted, book)
        target, vetoes_triggered = apply_theme_vetoes(proposed, raw_today, vetoes)
        if target > 0:
            direction = "LONG"
        elif target < 0:
            direction = "SHORT"
        else:
            direction = "FLAT"
        recs.append({
            "book": book["name"],
            "instrument": book["instrument"],
            "direction": direction,
            "target_position": target,
            "proposed_position": proposed,
            "weighted_score": round(weighted, 4),
            "top_themes": [{"theme": g, "weighted_score": round(s, 4)} for g, s in breakdown],
            "vetoes": vetoes_triggered,
        })
    return recs

"""Roll subtheme-level (`topic`) scores up to the main-theme level.

The taxonomy lives in app/config/theme_hierarchy.json. The extractor keeps
emitting subthemes (the existing `topic` column); this module just maps
each subtheme to its parent theme and aggregates per (date, theme).
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

BASE_DIR = Path(__file__).resolve().parents[2]
HIERARCHY_PATH = BASE_DIR / "app" / "config" / "theme_hierarchy.json"


def load_hierarchy() -> dict:
    with open(HIERARCHY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def build_subtheme_to_theme(hierarchy: dict | None = None) -> Dict[str, str]:
    h = hierarchy or load_hierarchy()
    out: Dict[str, str] = {}
    for theme_key, theme_def in h.get("themes", {}).items():
        for sub in theme_def.get("subthemes", []):
            out[sub] = theme_key
    return out


def topic_to_theme(topic: str, hierarchy: dict | None = None) -> str:
    h = hierarchy or load_hierarchy()
    mapping = build_subtheme_to_theme(h)
    return mapping.get(topic, h.get("fallback_theme", "other"))


def aggregate_theme_scores(score_rows: List[dict], hierarchy: dict | None = None) -> List[dict]:
    """Group daily subtheme scores into daily theme scores.

    Each output row: {score_date, commodity, theme, narrative_score,
    raw_score, event_count, breadth, persistence, source_divergence,
    subtheme_count, top_subthemes}.

    breadth/persistence/divergence are averaged across the constituent
    subthemes (weighted by event_count).
    """
    h = hierarchy or load_hierarchy()
    mapping = build_subtheme_to_theme(h)
    fallback = h.get("fallback_theme", "other")

    grouped: Dict[tuple, list] = defaultdict(list)
    for row in score_rows:
        theme = mapping.get(row["topic"], fallback)
        key = (row["score_date"], row.get("commodity", "crude_oil"), theme)
        grouped[key].append(row)

    results = []
    for (score_date, commodity, theme), rows in grouped.items():
        total_events = sum(int(r.get("event_count") or 0) for r in rows) or len(rows)

        def _w_avg(field: str) -> float:
            vals = [(float(r.get(field) or 0), int(r.get("event_count") or 1)) for r in rows]
            num = sum(v * w for v, w in vals)
            den = sum(w for _, w in vals) or 1
            return round(num / den, 6)

        sorted_subs = sorted(rows, key=lambda r: abs(float(r.get("narrative_score") or 0)), reverse=True)
        results.append({
            "score_date": score_date,
            "commodity": commodity,
            "theme": theme,
            "narrative_score": round(sum(float(r["narrative_score"]) for r in rows), 6),
            "raw_score": round(sum(float(r.get("raw_score") or 0) for r in rows), 6),
            "event_count": total_events,
            "subtheme_count": len(rows),
            "breadth": _w_avg("breadth"),
            "persistence": _w_avg("persistence"),
            "source_divergence": _w_avg("source_divergence"),
            "top_subthemes": [
                {"topic": r["topic"], "score": round(float(r["narrative_score"]), 6)}
                for r in sorted_subs[:5]
            ],
        })

    return sorted(results, key=lambda x: (x["score_date"], -abs(x["narrative_score"])))

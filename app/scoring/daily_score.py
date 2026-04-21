from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List


BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "app" / "config" / "scoring_config.json"


def load_scoring_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_date(ts: str) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return str(ts)[:10]


def compute_event_strength(event: dict, cfg: dict) -> float:
    bucket_w = cfg["bucket_weights"].get(event["source_bucket"], 0.50)
    verify_w = cfg["verification_multipliers"].get(event["verification_status"], 0.70)
    horizon_w = cfg["horizon_multipliers"].get(event["horizon"], 1.00)
    sign = cfg["direction_sign"].get(event["direction"], 0.0)

    credibility = float(event.get("credibility") or 0.5)
    novelty = float(event.get("novelty") or 0.5)
    confidence = float(event.get("confidence") or ((credibility + novelty) / 2.0))

    strength = sign * bucket_w * verify_w * horizon_w * (0.45 * credibility + 0.35 * novelty + 0.20 * confidence)

    if bool(event.get("rumor_flag")):
        strength *= (1.0 - cfg["rumor_penalty"])

    if event["verification_status"] == "officially_confirmed":
        strength *= (1.0 + cfg["official_confirmation_bonus"])

    return round(strength, 6)


def aggregate_daily_scores(events: List[dict], cfg: dict) -> List[dict]:
    grouped = defaultdict(list)
    for evt in events:
        score_date = normalize_date(evt["event_time"])
        key = (score_date, evt["commodity"], evt["topic"])
        grouped[key].append(evt)

    results = []
    for (score_date, commodity, topic), evts in grouped.items():
        event_strengths = [compute_event_strength(e, cfg) for e in evts]
        raw_score = sum(event_strengths)

        official_count = sum(1 for e in evts if e["verification_status"] == "officially_confirmed")
        chatter_count = sum(1 for e in evts if e["source_bucket"] in ("social_open", "social_private_manual"))
        news_count = sum(1 for e in evts if e["source_bucket"] == "authoritative_news")

        crowding_threshold = cfg["crowding_topic_threshold"]
        crowding_score = max(0.0, len(evts) - crowding_threshold) * cfg["crowding_penalty_per_extra_event"]

        final_score = raw_score - crowding_score

        results.append({
            "score_date": score_date,
            "commodity": commodity,
            "topic": topic,
            "narrative_score": round(final_score, 6),
            "raw_score": round(raw_score, 6),
            "event_count": len(evts),
            "official_confirmation_score": round(min(1.0, official_count / max(1, len(evts))), 6),
            "news_breadth_score": round(min(1.0, news_count / max(1, len(evts))), 6),
            "chatter_score": round(min(1.0, chatter_count / max(1, len(evts))), 6),
            "crowding_score": round(crowding_score, 6),
        })
    return sorted(results, key=lambda x: (x["score_date"], x["topic"]))

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List


BASE_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = BASE_DIR / "app" / "config" / "scoring_config.json"

OFFICIAL_BUCKETS = ("official_data", "official_reports")
INSTITUTIONAL_BUCKETS = ("institutional_public", "sellside_private", "authoritative_news")
CHATTER_BUCKETS = ("social_open", "social_private_manual")


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


def _signed(direction: str, cfg: dict) -> float:
    return cfg["direction_sign"].get(direction, 0.0)


def compute_breadth(evts: List[dict]) -> float:
    """Fraction of distinct sources carrying the topic — diminishing returns above 5."""
    distinct_sources = {e.get("source_id") or e["source_name"] for e in evts}
    return round(min(1.0, len(distinct_sources) / 5.0), 6)


def compute_persistence(evts: List[dict], history_by_topic: Dict, score_date: str, cfg: dict, half_life_days: int = 5) -> float:
    """How long this topic has been carrying the same direction.

    Looks back across prior days for the same topic and rewards consecutive
    same-sign daily scores with an exponential decay on age.
    """
    if not evts:
        return 0.0
    today_sign = sum(_signed(e["direction"], cfg) for e in evts)
    if today_sign == 0:
        return 0.0
    today_sign = 1.0 if today_sign > 0 else -1.0
    try:
        d0 = datetime.fromisoformat(score_date).date()
    except Exception:
        return 0.0
    weight = 0.0
    for offset in range(1, half_life_days * 4 + 1):
        prior = (d0 - timedelta(days=offset)).isoformat()
        prior_sign = history_by_topic.get(prior)
        if prior_sign is None:
            continue
        if prior_sign * today_sign <= 0:
            break
        weight += math.exp(-offset / half_life_days)
    return round(min(1.0, weight / 2.0), 6)


def compute_source_divergence(evts: List[dict], cfg: dict) -> float:
    """Positive when chatter/news diverges from officials on the same topic.

    Returns the absolute gap between the official-source net direction and the
    chatter/news net direction, scaled to [0,1]. A high value signals "the
    market is talking about something the institutions are not confirming".
    """
    def _net(bucket_set):
        bucket_evts = [e for e in evts if e["source_bucket"] in bucket_set]
        if not bucket_evts:
            return None
        return sum(_signed(e["direction"], cfg) for e in bucket_evts) / len(bucket_evts)

    official = _net(OFFICIAL_BUCKETS)
    non_official = _net(INSTITUTIONAL_BUCKETS + CHATTER_BUCKETS)
    if official is None or non_official is None:
        return 0.0
    return round(min(1.0, abs(official - non_official) / 2.0), 6)


def compute_persistence_multiplier(persistence: float) -> float:
    """A topic that's been building gets a small boost (caps at +25%)."""
    return 1.0 + 0.25 * persistence


def compute_breadth_multiplier(breadth: float) -> float:
    """More distinct sources = more confidence (caps at +40%)."""
    return 1.0 + 0.40 * breadth


def aggregate_daily_scores(events: List[dict], cfg: dict) -> List[dict]:
    grouped: Dict[tuple, List[dict]] = defaultdict(list)
    for evt in events:
        score_date = normalize_date(evt["event_time"])
        key = (score_date, evt["commodity"], evt["topic"])
        grouped[key].append(evt)

    # Build a per-topic history of daily net signs in chronological order so
    # persistence can look back at prior days.
    history_by_topic: Dict[tuple, Dict[str, float]] = defaultdict(dict)
    sorted_keys = sorted(grouped.keys(), key=lambda k: k[0])

    results = []
    for key in sorted_keys:
        score_date, commodity, topic = key
        evts = grouped[key]
        event_strengths = [compute_event_strength(e, cfg) for e in evts]
        raw_score = sum(event_strengths)

        breadth = compute_breadth(evts)
        persistence = compute_persistence(
            evts, history_by_topic[(commodity, topic)], score_date, cfg
        )
        divergence = compute_source_divergence(evts, cfg)

        official_count = sum(1 for e in evts if e["verification_status"] == "officially_confirmed")
        chatter_count = sum(1 for e in evts if e["source_bucket"] in CHATTER_BUCKETS)
        news_count = sum(1 for e in evts if e["source_bucket"] == "authoritative_news")

        crowding_threshold = cfg["crowding_topic_threshold"]
        crowding_score = max(0.0, len(evts) - crowding_threshold) * cfg["crowding_penalty_per_extra_event"]
        crowding_norm = min(1.0, max(0.0, len(evts) - crowding_threshold) / max(1, crowding_threshold))

        adjusted = raw_score * compute_persistence_multiplier(persistence) * compute_breadth_multiplier(breadth)
        final_score = adjusted - crowding_score

        results.append({
            "score_date": score_date,
            "commodity": commodity,
            "topic": topic,
            "narrative_score": round(final_score, 6),
            "raw_score": round(raw_score, 6),
            "event_count": len(evts),
            "breadth": breadth,
            "persistence": persistence,
            "source_divergence": divergence,
            "official_confirmation_score": round(min(1.0, official_count / max(1, len(evts))), 6),
            "news_breadth_score": round(min(1.0, news_count / max(1, len(evts))), 6),
            "chatter_score": round(min(1.0, chatter_count / max(1, len(evts))), 6),
            "crowding_score": round(crowding_score, 6),
            "crowding_norm": round(crowding_norm, 6),
        })

        # Record today's net sign so tomorrow's persistence calc can see it.
        net = sum(_signed(e["direction"], cfg) for e in evts)
        history_by_topic[(commodity, topic)][score_date] = (
            1.0 if net > 0 else (-1.0 if net < 0 else 0.0)
        )

    return sorted(results, key=lambda x: (x["score_date"], x["topic"]))

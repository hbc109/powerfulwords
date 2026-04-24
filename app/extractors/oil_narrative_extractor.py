from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from app.models.narrative import NarrativeEvent

BASE_DIR = Path(__file__).resolve().parents[2]
RULES_PATH = BASE_DIR / "app" / "config" / "oil_topic_rules.json"

def load_rules() -> dict:
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def _lower(text: str) -> str:
    return text.lower()

def _count_matches(text: str, terms: Iterable[str]) -> int:
    lowered = _lower(text)
    return sum(1 for t in terms if t.lower() in lowered)

def infer_topic(text: str, rules: dict) -> tuple[str, float, str]:
    matches = infer_all_topics(text, rules)
    if not matches:
        return "other", 0.25, "neutral"
    return matches[0]


def infer_all_topics(text: str, rules: dict) -> list[tuple[str, float, str]]:
    lowered = _lower(text)
    scores = []
    for topic, spec in rules["topic_rules"].items():
        score = sum(1 for kw in spec["keywords"] if kw.lower() in lowered)
        if score > 0:
            novelty = min(1.0, 0.45 + score * 0.12)
            scores.append((topic, novelty, spec["direction"]))
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores

def infer_direction(text: str, rules: dict, default_direction: str) -> str:
    bullish = _count_matches(text, rules["bullish_words"])
    bearish = _count_matches(text, rules["bearish_words"])
    if default_direction in ("bullish", "bearish") and bullish == bearish == 0:
        return default_direction
    if bullish > bearish:
        return "bullish"
    if bearish > bullish:
        return "bearish"
    if bullish == bearish and bullish > 0:
        return "mixed"
    return default_direction

def infer_rumor_flag(text: str, rules: dict, source_bucket: str) -> bool:
    if source_bucket == "social_private_manual":
        return True
    return _count_matches(text, rules["rumor_words"]) > 0

def infer_verification_status(text: str, rules: dict, source_bucket: str, rumor_flag: bool) -> str:
    if source_bucket in ("official_data", "official_reports"):
        return "officially_confirmed"
    if _count_matches(text, rules["official_confirm_words"]) > 0:
        return "officially_confirmed"
    if rumor_flag:
        return "unverified"
    return "partially_confirmed"

def infer_horizon(text: str, rules: dict) -> str:
    if _count_matches(text, rules["medium_term_words"]) > 0:
        return "medium_term"
    if _count_matches(text, rules["swing_words"]) > 0:
        return "swing"
    return "swing"

def infer_regions(text: str, rules: dict) -> List[str]:
    found = []
    for region, keys in rules["regions"].items():
        if _count_matches(text, keys) > 0:
            found.append(region)
    return found or ["global"]

def infer_entities(text: str, rules: dict) -> List[str]:
    lowered = _lower(text)
    return [e for e in rules["entities"] if e.lower() in lowered][:12]

def estimate_credibility(source_bucket: str, source_name: str, rumor_flag: bool) -> float:
    bucket_scores = {
        "official_data": 0.95,
        "official_reports": 0.92,
        "institutional_public": 0.84,
        "sellside_private": 0.80,
        "authoritative_news": 0.72,
        "social_open": 0.45,
        "social_private_manual": 0.30,
    }
    base = bucket_scores.get(source_bucket, 0.50)
    if rumor_flag:
        base -= 0.12
    if source_name.lower() in ("eia", "opec", "ofac"):
        base = max(base, 0.93)
    return max(0.05, min(1.0, base))

def derive_event_time(published_at: str | None) -> datetime | None:
    if not published_at:
        return None
    try:
        return datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
    except Exception:
        return None

def make_event_id(document_id: str, chunk_id: str, topic: str) -> str:
    clean_topic = re.sub(r"[^a-z0-9_]+", "", topic.lower())
    return f"evt_{document_id}_{chunk_id.split('_')[-1]}_{clean_topic}"

def extract_events_from_chunk(*, document: dict, chunk: dict, rules: dict) -> list[NarrativeEvent]:
    text = chunk["text"].strip()
    if len(text) < 80:
        return []
    event_time = derive_event_time(document.get("published_at"))
    if event_time is None:
        return []
    matches = infer_all_topics(text, rules)
    if not matches:
        return []

    rumor_flag = infer_rumor_flag(text, rules, document["source_bucket"])
    verification_status = infer_verification_status(text, rules, document["source_bucket"], rumor_flag)
    horizon = infer_horizon(text, rules)
    regions = infer_regions(text, rules)
    entities = infer_entities(text, rules)
    credibility = estimate_credibility(document["source_bucket"], document["source_name"], rumor_flag)

    events: list[NarrativeEvent] = []
    for topic, novelty, default_direction in matches:
        direction = infer_direction(text, rules, default_direction)
        events.append(NarrativeEvent(
            event_id=make_event_id(document["document_id"], chunk["chunk_id"], topic),
            event_time=event_time,
            commodity="crude_oil",
            topic=topic,
            direction=direction,
            source_bucket=document["source_bucket"],
            source_name=document["source_name"],
            source_id=document.get("source_id"),
            document_id=document["document_id"],
            chunk_id=chunk["chunk_id"],
            credibility=credibility,
            novelty=novelty,
            breadth=None,
            persistence=None,
            crowding=None,
            price_confirmation=None,
            verification_status=verification_status,
            horizon=horizon,
            rumor_flag=rumor_flag,
            confidence=min(0.95, round((credibility * 0.55 + novelty * 0.45), 4)),
            entities=entities,
            regions=regions,
            asset_candidates=["WTI", "Brent", "XLE"],
            evidence_text=text[:1200],
            evidence_spans=[],
            notes=f"baseline_rule_extractor topic={topic}",
        ))
    return events


def extract_event_from_chunk(*, document: dict, chunk: dict, rules: dict) -> NarrativeEvent | None:
    events = extract_events_from_chunk(document=document, chunk=chunk, rules=rules)
    return events[0] if events else None

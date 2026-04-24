from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.extractors.llm_providers import call_provider, has_credentials
from app.models.narrative import NarrativeEvent
from app.models.narrative_extraction import NarrativeExtraction
from app.scoring.theme_rollup import build_subtheme_to_theme, load_hierarchy

BASE_DIR = Path(__file__).resolve().parents[2]
LLM_CONFIG_PATH = BASE_DIR / 'app' / 'config' / 'llm_config.json'
PROMPT_PATH = BASE_DIR / 'app' / 'prompts' / 'narrative_extraction_prompt.md'


def load_llm_config() -> dict:
    with open(LLM_CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def configured_provider(cfg: dict | None = None) -> str:
    cfg = cfg or load_llm_config()
    return cfg.get('provider', 'anthropic')


def provider_config(cfg: dict | None = None) -> dict:
    cfg = cfg or load_llm_config()
    provider = configured_provider(cfg)
    providers_block = cfg.get('providers') or {}
    if provider in providers_block:
        return providers_block[provider]
    # Backwards-compat with the old flat config (no `providers` block).
    return {k: v for k, v in cfg.items() if k not in ('provider', 'providers', 'mode_default', 'fallback_to_rules')}


def has_llm_credentials(cfg: dict | None = None) -> bool:
    return has_credentials(configured_provider(cfg))


def load_prompt_template() -> str:
    return PROMPT_PATH.read_text(encoding='utf-8')


def derive_event_time(published_at: str | None) -> datetime | None:
    if not published_at:
        return None
    try:
        return datetime.fromisoformat(str(published_at).replace('Z', '+00:00'))
    except Exception:
        return None


def make_event_id(document_id: str, chunk_id: str, topic: str) -> str:
    clean_topic = re.sub(r'[^a-z0-9_]+', '', topic.lower())
    suffix = chunk_id.split('_')[-1]
    return f'evt_{document_id}_{suffix}_{clean_topic}'


def build_messages(document: dict, chunk: dict, prompt_template: str) -> list[dict]:
    system_prompt = (
        prompt_template
        + '\n\nUse English output values for enums. '
        + 'If the chunk is not directly actionable for crude oil narrative extraction, set should_extract=false.'
    )
    user_payload = {
        'document_context': {
            'document_id': document.get('document_id'),
            'source_id': document.get('source_id'),
            'source_bucket': document.get('source_bucket'),
            'source_name': document.get('source_name'),
            'published_at': document.get('published_at'),
        },
        'chunk_context': {
            'chunk_id': chunk.get('chunk_id'),
            'chunk_index': chunk.get('chunk_index'),
        },
        'text': chunk.get('text', ''),
    }
    return [
        {'role': 'system', 'content': system_prompt},
        {'role': 'user', 'content': json.dumps(user_payload, ensure_ascii=False)},
    ]


def convert_extraction_to_event(document: dict, chunk: dict, ext: NarrativeExtraction) -> NarrativeEvent | None:
    event_time = derive_event_time(document.get('published_at'))
    if event_time is None:
        return None
    hierarchy = load_hierarchy()
    sub_to_theme = build_subtheme_to_theme(hierarchy)
    fallback_theme = hierarchy.get('fallback_theme', 'other')
    return NarrativeEvent(
        event_id=make_event_id(document['document_id'], chunk['chunk_id'], ext.topic),
        event_time=event_time,
        commodity='crude_oil',
        theme=sub_to_theme.get(ext.topic, fallback_theme),
        topic=ext.topic,
        direction=ext.direction,
        source_bucket=document['source_bucket'],
        source_name=document['source_name'],
        source_id=document.get('source_id'),
        document_id=document['document_id'],
        chunk_id=chunk['chunk_id'],
        credibility=ext.credibility,
        novelty=ext.novelty,
        breadth=ext.breadth,
        persistence=ext.persistence,
        crowding=ext.crowding,
        price_confirmation=ext.price_confirmation,
        verification_status=ext.verification_status,
        horizon=ext.horizon,
        rumor_flag=ext.rumor_flag,
        confidence=ext.confidence,
        entities=ext.entities,
        regions=ext.regions,
        asset_candidates=ext.asset_candidates or ['WTI', 'Brent', 'XLE'],
        evidence_text=ext.evidence_text,
        evidence_spans=ext.evidence_spans,
        notes=ext.notes or 'llm_extractor',
    )


def extract_event_from_chunk_llm(document: dict, chunk: dict) -> Optional[NarrativeEvent]:
    cfg = load_llm_config()
    provider = configured_provider(cfg)
    pcfg = provider_config(cfg)
    prompt_template = load_prompt_template()
    messages = build_messages(document, chunk, prompt_template)
    ext = call_provider(provider, messages, pcfg)
    if not ext.should_extract:
        return None
    return convert_extraction_to_event(document, chunk, ext)

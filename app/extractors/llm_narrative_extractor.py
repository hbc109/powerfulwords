from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.models.narrative import NarrativeEvent
from app.models.narrative_extraction import NarrativeExtraction

BASE_DIR = Path(__file__).resolve().parents[2]
LLM_CONFIG_PATH = BASE_DIR / 'app' / 'config' / 'llm_config.json'
PROMPT_PATH = BASE_DIR / 'app' / 'prompts' / 'narrative_extraction_prompt.md'


def load_llm_config() -> dict:
    with open(LLM_CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_prompt_template() -> str:
    return PROMPT_PATH.read_text(encoding='utf-8')


def has_openai_credentials() -> bool:
    return bool(os.environ.get('OPENAI_API_KEY'))


def derive_event_time(published_at: str | None) -> datetime:
    if published_at:
        try:
            return datetime.fromisoformat(str(published_at).replace('Z', '+00:00'))
        except Exception:
            pass
    return datetime.now(timezone.utc)


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


def _call_openai_responses(messages: list[dict], cfg: dict) -> NarrativeExtraction:
    from openai import OpenAI

    client = OpenAI(timeout=cfg.get('request_timeout_seconds', 60))
    response = client.responses.parse(
        model=cfg.get('model', 'gpt-5.2'),
        input=messages,
        text_format=NarrativeExtraction,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise ValueError('LLM returned no parsed output.')
    return parsed


def convert_extraction_to_event(document: dict, chunk: dict, ext: NarrativeExtraction) -> NarrativeEvent:
    return NarrativeEvent(
        event_id=make_event_id(document['document_id'], chunk['chunk_id'], ext.topic),
        event_time=derive_event_time(document.get('published_at')),
        commodity='crude_oil',
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
    if not has_openai_credentials():
        raise RuntimeError('OPENAI_API_KEY not found.')
    prompt_template = load_prompt_template()
    messages = build_messages(document, chunk, prompt_template)
    ext = _call_openai_responses(messages, cfg)
    if not ext.should_extract:
        return None
    return convert_extraction_to_event(document, chunk, ext)

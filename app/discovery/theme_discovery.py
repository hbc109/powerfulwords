"""LLM-driven theme discovery.

Reads recent chunks from the `chunks` table (optionally filtered to docs
whose events were classified as 'other' or to all docs), sends them to
the configured LLM provider, and asks for proposed themes / subthemes
that don't fit the existing taxonomy in theme_hierarchy.json +
oil_topic_rules.json.

Output is a `ThemeDiscoveryResult` Pydantic object that the caller writes
to JSON for human review. Nothing auto-promotes into the taxonomy — the
user decides whether to add the proposed themes to their configs.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from app.extractors.llm_narrative_extractor import (
    configured_provider,
    load_llm_config,
    provider_config,
)
from app.extractors.llm_providers import PROVIDERS, env_var_for, has_credentials
from app.models.theme_discovery import ThemeDiscoveryResult

BASE_DIR = Path(__file__).resolve().parents[2]
RULES_PATH = BASE_DIR / "app" / "config" / "oil_topic_rules.json"
HIERARCHY_PATH = BASE_DIR / "app" / "config" / "theme_hierarchy.json"


SYSTEM_TEMPLATE = """You are helping maintain a crude oil narrative taxonomy.

The existing taxonomy is:

## Main themes
{themes_block}

## Subthemes (each with a default direction and keyword list)
{subthemes_block}

Your task: given a set of recent chunk excerpts, identify **new themes and
subthemes that do not fit the existing list**. Do not repeat existing ones.
If the excerpts are all well-covered by the existing taxonomy, return
empty lists and say so in `summary` and `coverage_note`.

For each proposed subtheme:
- Use a snake_case `label`.
- Map it to the best-fit existing main theme via `parent_theme`; only
  propose a brand-new main theme via `new_themes` if no existing theme
  captures it.
- Suggest 3-10 keyword phrases that would trigger this subtheme.
- Quote 1-3 short verbatim excerpts from the input as `example_evidence`.
- Set `direction_bias` to bullish, bearish, mixed, or neutral.

Be conservative: only propose a new subtheme when you see a recurring,
distinct narrative that the existing taxonomy meaningfully misses.
"""

USER_TEMPLATE = """Here are {n} recent chunk excerpts (each prefixed with [source_bucket/source_name]):

{chunks_block}

Propose new themes/subthemes using the ThemeDiscoveryResult schema."""


def _load_taxonomy() -> tuple[dict, dict]:
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        rules = json.load(f)
    with open(HIERARCHY_PATH, "r", encoding="utf-8") as f:
        hierarchy = json.load(f)
    return rules, hierarchy


def _format_themes_block(hierarchy: dict) -> str:
    lines = []
    for key, spec in hierarchy.get("themes", {}).items():
        subs = ", ".join(spec.get("subthemes", []))
        lines.append(f"- {key} ({spec.get('label', key)}): {subs}")
    return "\n".join(lines) if lines else "(none)"


def _format_subthemes_block(rules: dict) -> str:
    lines = []
    for topic, spec in rules.get("topic_rules", {}).items():
        direction = spec.get("direction", "neutral")
        sample_kws = ", ".join(spec.get("keywords", [])[:5])
        lines.append(f"- {topic} [{direction}]: {sample_kws} …")
    return "\n".join(lines) if lines else "(none)"


def fetch_recent_chunks(conn, days: int, limit: int | None = None) -> List[dict]:
    """Return recent chunks with document metadata.

    Preference order for filtering:
    - docs published in the last `days` days
    - if that yields nothing, fall back to the latest `limit` chunks overall.
    """
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    q = """
        SELECT c.chunk_id, c.text, d.source_bucket, d.source_name,
               d.published_at, d.title, d.document_id
        FROM chunks c
        JOIN documents d ON d.document_id = c.document_id
        WHERE COALESCE(d.published_at, d.ingested_at) >= ?
        ORDER BY COALESCE(d.published_at, d.ingested_at) DESC, c.chunk_index ASC
    """
    rows = conn.execute(q, (since,)).fetchall()
    if not rows and limit:
        rows = conn.execute(
            """
            SELECT c.chunk_id, c.text, d.source_bucket, d.source_name,
                   d.published_at, d.title, d.document_id
            FROM chunks c
            JOIN documents d ON d.document_id = c.document_id
            ORDER BY COALESCE(d.published_at, d.ingested_at) DESC, c.chunk_index ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    chunks = []
    for r in rows:
        chunks.append({
            "chunk_id": r[0],
            "text": r[1],
            "source_bucket": r[2],
            "source_name": r[3],
            "published_at": r[4],
            "title": r[5],
            "document_id": r[6],
        })
    if limit:
        chunks = chunks[:limit]
    return chunks


def _format_chunks_block(chunks: List[dict], max_chars_per_chunk: int = 1200) -> str:
    lines = []
    for i, c in enumerate(chunks, start=1):
        prefix = f"[{c['source_bucket']}/{c['source_name']}] {c.get('title') or ''}".strip()
        body = (c["text"] or "").strip().replace("\n", " ")
        if len(body) > max_chars_per_chunk:
            body = body[:max_chars_per_chunk].rstrip() + " …"
        lines.append(f"({i}) {prefix}\n{body}")
    return "\n\n".join(lines)


def discover_themes(chunks: List[dict]) -> ThemeDiscoveryResult:
    if not chunks:
        return ThemeDiscoveryResult(
            summary="No chunks available in the requested window; nothing to analyze.",
            coverage_note="0 chunks scanned.",
        )

    rules, hierarchy = _load_taxonomy()
    system_prompt = SYSTEM_TEMPLATE.format(
        themes_block=_format_themes_block(hierarchy),
        subthemes_block=_format_subthemes_block(rules),
    )
    user_prompt = USER_TEMPLATE.format(
        n=len(chunks),
        chunks_block=_format_chunks_block(chunks),
    )

    cfg = load_llm_config()
    provider = configured_provider(cfg)
    if not has_credentials(provider):
        raise RuntimeError(
            f"{env_var_for(provider)} is not set; cannot run theme discovery with provider={provider}."
        )
    pcfg = provider_config(cfg)

    schema = ThemeDiscoveryResult.model_json_schema()
    tool = {
        "name": "record_theme_discovery",
        "description": "Record proposed new themes and subthemes.",
        "input_schema": schema,
    }

    # Call the provider directly so we can use a different output schema.
    if provider == "anthropic":
        import anthropic

        client = anthropic.Anthropic(timeout=pcfg.get("request_timeout_seconds", 60))
        response = client.messages.create(
            model=pcfg.get("model", "claude-sonnet-4-6"),
            max_tokens=int(pcfg.get("max_output_tokens", 2048)),
            temperature=float(pcfg.get("temperature", 0.1)),
            system=system_prompt,
            tools=[tool],
            tool_choice={"type": "tool", "name": tool["name"]},
            messages=[{"role": "user", "content": user_prompt}],
        )
        for block in response.content:
            if getattr(block, "type", None) == "tool_use" and block.name == tool["name"]:
                return ThemeDiscoveryResult.model_validate(block.input)
        raise ValueError("Anthropic response contained no tool_use block.")

    if provider == "openai":
        from openai import OpenAI

        client = OpenAI(timeout=pcfg.get("request_timeout_seconds", 60))
        response = client.responses.parse(
            model=pcfg.get("model", "gpt-4o"),
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            text_format=ThemeDiscoveryResult,
        )
        parsed = response.output_parsed
        if parsed is None:
            raise ValueError("OpenAI returned no parsed output.")
        return parsed

    raise ValueError(f"Unsupported provider for discovery: {provider}")

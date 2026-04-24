"""Provider-agnostic LLM call layer.

Each function takes the system + user messages and the provider's config
block from llm_config.json, and returns a parsed NarrativeExtraction.

The dispatcher `call_provider` picks the right backend based on the
top-level "provider" key in llm_config.json. Add a new provider by
implementing `_call_<name>` and registering it in PROVIDERS.
"""

from __future__ import annotations

import json
import os
from typing import Callable

from app.models.narrative_extraction import NarrativeExtraction


def env_var_for(provider: str) -> str:
    if provider == "anthropic":
        return "ANTHROPIC_API_KEY"
    if provider == "openai":
        return "OPENAI_API_KEY"
    raise ValueError(f"Unknown provider: {provider}")


def has_credentials(provider: str) -> bool:
    return bool(os.environ.get(env_var_for(provider)))


def _system_and_user(messages: list[dict]) -> tuple[str, str]:
    system_parts = [m["content"] for m in messages if m["role"] == "system"]
    user_parts = [m["content"] for m in messages if m["role"] == "user"]
    return "\n\n".join(system_parts), "\n\n".join(user_parts)


def _call_anthropic(messages: list[dict], cfg: dict) -> NarrativeExtraction:
    import anthropic

    client = anthropic.Anthropic(timeout=cfg.get("request_timeout_seconds", 60))
    system_text, user_text = _system_and_user(messages)

    schema = NarrativeExtraction.model_json_schema()
    tool = {
        "name": "record_narrative_extraction",
        "description": "Record the structured narrative extraction for the chunk.",
        "input_schema": schema,
    }

    response = client.messages.create(
        model=cfg.get("model", "claude-sonnet-4-6"),
        max_tokens=int(cfg.get("max_output_tokens", 1024)),
        temperature=float(cfg.get("temperature", 0.1)),
        system=system_text,
        tools=[tool],
        tool_choice={"type": "tool", "name": tool["name"]},
        messages=[{"role": "user", "content": user_text}],
    )

    for block in response.content:
        if getattr(block, "type", None) == "tool_use" and block.name == tool["name"]:
            return NarrativeExtraction.model_validate(block.input)

    raise ValueError("Anthropic response contained no tool_use block.")


def _call_openai(messages: list[dict], cfg: dict) -> NarrativeExtraction:
    from openai import OpenAI

    client = OpenAI(timeout=cfg.get("request_timeout_seconds", 60))
    response = client.responses.parse(
        model=cfg.get("model", "gpt-4o"),
        input=messages,
        text_format=NarrativeExtraction,
    )
    parsed = response.output_parsed
    if parsed is None:
        raise ValueError("OpenAI returned no parsed output.")
    return parsed


PROVIDERS: dict[str, Callable[[list[dict], dict], NarrativeExtraction]] = {
    "anthropic": _call_anthropic,
    "openai": _call_openai,
}


def call_provider(provider: str, messages: list[dict], provider_cfg: dict) -> NarrativeExtraction:
    if provider not in PROVIDERS:
        raise ValueError(f"Unsupported LLM provider: {provider}. Available: {list(PROVIDERS)}")
    if not has_credentials(provider):
        raise RuntimeError(
            f"{env_var_for(provider)} is not set; cannot use provider={provider}."
        )
    return PROVIDERS[provider](messages, provider_cfg)

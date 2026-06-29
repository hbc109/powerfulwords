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
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Callable

from app.models.narrative_extraction import NarrativeExtraction


def env_var_for(provider: str) -> str:
    if provider == "anthropic":
        return "ANTHROPIC_API_KEY"
    if provider == "openai":
        return "OPENAI_API_KEY"
    if provider == "deepseek":
        return "DEEPSEEK_API_KEY"
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


def _claude_cli_path() -> str | None:
    """Locate the Claude Code CLI (cron PATH often omits ~/.local/bin)."""
    p = shutil.which("claude")
    if p:
        return p
    for c in (os.path.expanduser("~/.local/bin/claude"),
              "/usr/local/bin/claude", "/usr/bin/claude"):
        if os.path.exists(c):
            return c
    return None


def _extract_json_obj(text: str) -> dict | None:
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
    try:
        return json.loads(text)
    except Exception:
        pass
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except Exception:
            return None
    return None


def _call_claude_cli(messages: list[dict], cfg: dict) -> NarrativeExtraction:
    """Headless Claude Code backend — uses the user's `claude` login, NO API key.

    Used as the anthropic backend when ANTHROPIC_API_KEY is unset. Lets
    `--mode llm` run keyless. (Auto mode still resolves to rules without a key,
    so the bulk pipeline doesn't fan out CLI calls unless explicitly asked.)
    """
    cli = _claude_cli_path()
    if not cli:
        raise RuntimeError("claude CLI not found")
    system_text, user_text = _system_and_user(messages)
    schema = json.dumps(NarrativeExtraction.model_json_schema(), ensure_ascii=False)
    prompt = (
        system_text
        + "\n\nReturn ONLY a single JSON object (no markdown fences, no prose) "
        "that validates against this JSON schema:\n" + schema
        + "\n\n--- INPUT ---\n" + user_text
    )
    timeout = int(cfg.get("request_timeout_seconds", 120) or 120)
    proc = subprocess.run(
        [cli, "-p", prompt, "--output-format", "json"],
        capture_output=True, text=True, timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"claude CLI exit {proc.returncode}: {proc.stderr[:200]}")
    try:
        result_text = json.loads(proc.stdout).get("result", "")
    except Exception:
        result_text = proc.stdout
    data = _extract_json_obj(result_text)
    if data is None:
        raise ValueError("claude CLI returned no parseable JSON")
    return NarrativeExtraction.model_validate(data)


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


def _call_deepseek(messages: list[dict], cfg: dict) -> NarrativeExtraction:
    """DeepSeek backend — OpenAI-compatible Chat Completions + JSON mode.

    DeepSeek's function-calling is flakier than its JSON mode, so we embed the
    NarrativeExtraction schema in the system prompt and force a JSON object
    response, then validate with pydantic (same robust pattern as the CLI path).
    """
    from openai import OpenAI

    client = OpenAI(
        api_key=os.environ.get("DEEPSEEK_API_KEY"),
        base_url=cfg.get("base_url", "https://api.deepseek.com"),
        timeout=cfg.get("request_timeout_seconds", 60),
    )
    system_text, user_text = _system_and_user(messages)
    schema = json.dumps(NarrativeExtraction.model_json_schema(), ensure_ascii=False)
    system_text = (
        system_text
        + "\n\nReturn ONLY a single JSON object (no markdown fences, no prose) "
        "that validates against this JSON schema:\n" + schema
        + "\n\nAlways include every required field. If should_extract is false, "
        "still return the object with topic=\"other\", direction=\"neutral\", "
        "verification_status=\"unverified\", horizon=\"unknown\", and "
        "credibility/novelty/evidence_text as empty/zero placeholders."
    )
    # DeepSeek is the only ingester (no rule/word-match fallback), so retry
    # transient failures (network blips, 429/5xx, empty/garbled JSON) here
    # rather than dropping the chunk. Final failure re-raises -> caller skips
    # WITHOUT marking the chunk processed, so it is retried on the next run.
    attempts = int(cfg.get("retries", 3))
    last_err: Exception | None = None
    for attempt in range(attempts):
        try:
            response = client.chat.completions.create(
                model=cfg.get("model", "deepseek-chat"),
                max_tokens=int(cfg.get("max_output_tokens", 1024)),
                temperature=float(cfg.get("temperature", 0.1)),
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_text},
                    {"role": "user", "content": user_text},
                ],
            )
            content = response.choices[0].message.content or ""
            data = _extract_json_obj(content)
            if data is None:
                raise ValueError("DeepSeek returned no parseable JSON.")
            break
        except Exception as e:  # noqa: BLE001 - retry any transient failure
            last_err = e
            if attempt < attempts - 1:
                time.sleep(2 * (attempt + 1))
    else:
        raise last_err  # type: ignore[misc]
    # When DeepSeek judges a chunk non-actionable it often returns a minimal
    # {"should_extract": false} and omits the 7 required fields. The wrapper
    # discards non-actionable chunks anyway, so return a valid neutral object
    # rather than failing validation (which would force a needless rule fallback).
    if not data.get("should_extract", True):
        return NarrativeExtraction(
            should_extract=False, topic="other", direction="neutral",
            credibility=0.0, novelty=0.0, verification_status="unverified",
            horizon="unknown", evidence_text="",
        )
    return NarrativeExtraction.model_validate(data)


PROVIDERS: dict[str, Callable[[list[dict], dict], NarrativeExtraction]] = {
    "anthropic": _call_anthropic,
    "openai": _call_openai,
    "deepseek": _call_deepseek,
}


def _load_llm_config() -> dict:
    cfg_path = Path(__file__).resolve().parents[1] / "config" / "llm_config.json"
    return json.loads(cfg_path.read_text())


def active_provider_model() -> tuple[str, str]:
    """Return (provider, model) per app/config/llm_config.json (deepseek by default)."""
    cfg = _load_llm_config()
    provider = cfg.get("provider", "deepseek")
    pcfg = (cfg.get("providers") or {}).get(provider, {}) or {}
    return provider, pcfg.get("model", "")


def generate_text(
    system: str,
    user: str,
    *,
    max_tokens: int = 1024,
    temperature: float = 0.3,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    """Free-text completion through the configured provider.

    Sibling to the structured extraction path above: same llm_config.json +
    env-var conventions, but returns the model's raw prose (used by the daily
    report and the AI-judgment note). Defaults to the top-level `provider` in
    llm_config.json — currently deepseek.
    """
    cfg = _load_llm_config()
    provider = provider or cfg.get("provider", "deepseek")
    pcfg = (cfg.get("providers") or {}).get(provider, {}) or {}
    timeout = pcfg.get("request_timeout_seconds", 90)
    model = model or pcfg.get("model")

    if provider in ("deepseek", "openai"):
        from openai import OpenAI

        base_url = pcfg.get("base_url") if provider == "deepseek" else None
        client = OpenAI(
            api_key=os.environ.get(env_var_for(provider)),
            base_url=base_url or ("https://api.deepseek.com" if provider == "deepseek" else None),
            timeout=timeout,
        )
        resp = client.chat.completions.create(
            model=model or ("deepseek-chat" if provider == "deepseek" else "gpt-4o"),
            max_tokens=int(max_tokens),
            temperature=float(temperature),
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        return (resp.choices[0].message.content or "").strip()

    if provider == "anthropic":
        import anthropic

        client = anthropic.Anthropic(timeout=timeout)
        resp = client.messages.create(
            model=model or "claude-sonnet-4-6",
            max_tokens=int(max_tokens),
            temperature=float(temperature),
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return "".join(
            getattr(b, "text", "") for b in resp.content
            if getattr(b, "type", None) == "text"
        ).strip()

    raise ValueError(f"generate_text: unsupported provider {provider}")


def call_provider(provider: str, messages: list[dict], provider_cfg: dict) -> NarrativeExtraction:
    if provider not in PROVIDERS:
        raise ValueError(f"Unsupported LLM provider: {provider}. Available: {list(PROVIDERS)}")
    # Anthropic with no API key -> fall back to the Claude Code CLI (keyless).
    if provider == "anthropic" and not has_credentials("anthropic"):
        if _claude_cli_path():
            return _call_claude_cli(messages, provider_cfg)
        raise RuntimeError(
            "ANTHROPIC_API_KEY is not set and the `claude` CLI was not found; "
            "cannot use provider=anthropic."
        )
    if not has_credentials(provider):
        raise RuntimeError(
            f"{env_var_for(provider)} is not set; cannot use provider={provider}."
        )
    return PROVIDERS[provider](messages, provider_cfg)

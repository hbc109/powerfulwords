"""AI judgment overlay — does NOT touch trades.

Each day a separate process calls Claude with today's signal context
(composite scores, factor breakdown, regime, recent narrative themes,
recent paper-trade outcomes) and gets back a short prose review:
does the signal make sense, what factors disagree, what tail-risk
might the rules be missing, cross-symbol coherence check.

The review is stored in a new `ai_reviews` table and surfaced in its
own dashboard tab. Trades and the composite signal are unaffected —
this is a parallel, advisory layer for human judgment.

Anthropic Claude SDK only (per user preference). No-op if
ANTHROPIC_API_KEY isn't set.
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from app.db.database import get_connection

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS ai_reviews (
    review_id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_date TEXT UNIQUE NOT NULL,
    model TEXT,
    context_json TEXT,
    review_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ai_reviews_date ON ai_reviews(review_date DESC);
"""

DEFAULT_MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = """You are a senior crude oil markets analyst. Each evening
you receive today's output from a regime-conditional multi-factor signal
(narrative tilt, term structure, money-manager positioning, US inventory)
for WTI and Brent, plus recent narrative themes and recent realized PnL.

Your job is to write a short, honest **review note** — 150-200 words. The
trade decisions are made by the rules; you are NOT picking a direction.
You are:

1. Flagging when the signal looks coherent vs. when factors meaningfully
   disagree (and which one you'd trust more given current context).
2. Noting cross-symbol coherence (WTI vs. Brent — do they agree? if not,
   is the gap explainable?).
3. Calling out tail-risk or event-risk the rules can't see (Fed surprise,
   OPEC emergency, geopolitical break) that might invalidate today's
   signal regardless of factor reading.
4. Brief sanity check on recent paper trades: any pattern in the misses?

Format: one short paragraph, no bullets. Be direct. If the signal is
obviously reasonable and nothing material is brewing, say so in 1-2
sentences instead of padding. Do not invent facts or quote prices you
haven't been shown."""


def ensure_table(conn: Optional[sqlite3.Connection] = None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    conn.executescript(CREATE_SQL)
    conn.commit()
    if own:
        conn.close()


def _gather_context(review_date: date, conn: sqlite3.Connection) -> dict:
    """Collect the inputs the LLM will reason over for the review."""
    ctx: dict = {"review_date": review_date.isoformat()}

    # Today's paper-trade snapshots (the canonical signal record for the day)
    snaps = conn.execute(
        "SELECT symbol, direction, target_position, composite_score, regime, "
        "       narrative_z, term_structure, positioning, inventory, "
        "       breakdown_json, entry_close, reasoning "
        "FROM paper_trades WHERE plan_date=? ORDER BY symbol",
        (review_date.isoformat(),),
    ).fetchall()
    ctx["signals_today"] = []
    for r in snaps:
        try:
            breakdown = json.loads(r[9]) if r[9] else []
        except Exception:
            breakdown = []
        ctx["signals_today"].append({
            "symbol": r[0], "direction": r[1], "target_position": r[2],
            "composite": r[3], "regime": r[4],
            "narrative_z": r[5], "term_structure": r[6],
            "positioning": r[7], "inventory": r[8],
            "breakdown": breakdown, "entry_close": r[10],
            "auto_reasoning": r[11],
        })

    # Top narrative themes over the last 7 days
    theme_window_start = (review_date - timedelta(days=7)).isoformat()
    themes = conn.execute(
        "SELECT theme, SUM(narrative_score) AS total_score, COUNT(*) AS n_days "
        "FROM daily_theme_scores "
        "WHERE commodity='crude_oil' AND score_date BETWEEN ? AND ? "
        "GROUP BY theme ORDER BY ABS(SUM(narrative_score)) DESC LIMIT 8",
        (theme_window_start, review_date.isoformat()),
    ).fetchall()
    ctx["recent_themes_7d"] = [
        {"theme": t[0], "total_score": round(float(t[1] or 0), 3), "n_days_present": t[2]}
        for t in themes
    ]

    # A few recent document titles for current-events flavor
    titles = conn.execute(
        "SELECT date(published_at), source_id, title FROM documents "
        "WHERE published_at IS NOT NULL "
        "  AND date(published_at) BETWEEN ? AND ? "
        "  AND quality_tier >= 2 "
        "ORDER BY published_at DESC LIMIT 15",
        (theme_window_start, review_date.isoformat()),
    ).fetchall()
    ctx["recent_titles"] = [
        {"date": t[0], "source": t[1], "title": (t[2] or "")[:160]}
        for t in titles if t[2]
    ]

    # Last 8 closed paper trades for "how are we doing" sanity
    closed = conn.execute(
        "SELECT symbol, plan_date, exit_date, direction, target_position, "
        "       composite_score, regime, realized_pnl_pct, holding_days "
        "FROM paper_trades WHERE exit_date IS NOT NULL AND exit_date <= ? "
        "ORDER BY exit_date DESC LIMIT 8",
        (review_date.isoformat(),),
    ).fetchall()
    ctx["recent_closed_trades"] = [
        {"symbol": c[0], "plan_date": c[1], "exit_date": c[2],
         "direction": c[3], "target_position": c[4],
         "composite": c[5], "regime": c[6],
         "realized_pnl_pct": c[7], "holding_days": c[8]}
        for c in closed
    ]
    return ctx


def _format_context_for_llm(ctx: dict) -> str:
    """Turn the structured context into a compact prompt body."""
    lines = [f"Review date: {ctx['review_date']}", ""]
    lines.append("== Today's signal ==")
    if not ctx["signals_today"]:
        lines.append("(no snapshot recorded for today)")
    for s in ctx["signals_today"]:
        comp = s["composite"]
        comp_str = f"{comp:+.3f}" if isinstance(comp, (int, float)) else "n/a"
        lines.append(
            f"{s['symbol']}: {s['direction']} {abs(s['target_position'] or 0):.0f}x · "
            f"regime `{s['regime']}` · composite {comp_str}"
        )
        for f in ["narrative_z", "term_structure", "positioning", "inventory"]:
            v = s.get(f)
            if isinstance(v, (int, float)):
                lines.append(f"  - {f}: {v:+.3f}")
        if s.get("auto_reasoning"):
            lines.append(f"  rule-based reasoning: {s['auto_reasoning']}")
        if s.get("entry_close"):
            lines.append(f"  entry close: {s['entry_close']:,.2f}")
    lines.append("")
    lines.append("== Recent narrative themes (7d, top by absolute summed score) ==")
    for t in ctx["recent_themes_7d"]:
        lines.append(f"  {t['theme']:<14} total {t['total_score']:+.2f} ({t['n_days_present']}d)")
    lines.append("")
    lines.append("== Recent high-quality document titles ==")
    for t in ctx["recent_titles"][:10]:
        lines.append(f"  {t['date']}  ({t['source']})  {t['title']}")
    lines.append("")
    lines.append("== Last closed paper trades ==")
    for c in ctx["recent_closed_trades"]:
        pnl = c.get("realized_pnl_pct")
        pnl_str = f"{pnl:+.2%}" if isinstance(pnl, (int, float)) else "n/a"
        lines.append(
            f"  {c['plan_date']} → {c['exit_date']}  {c['symbol']} {c['direction']} {abs(c['target_position'] or 0):.0f}x  "
            f"regime `{c['regime']}`  realized {pnl_str}  ({c['holding_days']}d)"
        )
    return "\n".join(lines)


def generate_review(review_date: date, *, model: str = DEFAULT_MODEL) -> dict:
    """Call Claude to generate a review for `review_date`. Returns a dict:
      {"status": "ok"|"skipped"|"error", "review_text": str|None, "model": str, "context": dict, "reason": str|None}
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"status": "skipped", "reason": "ANTHROPIC_API_KEY not set",
                "review_text": None, "model": model, "context": {}}

    conn = get_connection()
    ensure_table(conn)
    ctx = _gather_context(review_date, conn)
    conn.close()

    if not ctx.get("signals_today"):
        return {"status": "skipped", "reason": "no paper-trade snapshot for this date yet",
                "review_text": None, "model": model, "context": ctx}

    try:
        import anthropic
    except ImportError:
        return {"status": "error", "reason": "anthropic SDK not installed (`pip install anthropic`)",
                "review_text": None, "model": model, "context": ctx}

    user_prompt = _format_context_for_llm(ctx)
    try:
        client = anthropic.Anthropic(timeout=45)
        resp = client.messages.create(
            model=model,
            max_tokens=600,
            temperature=0.3,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = "".join(getattr(b, "text", "") for b in resp.content if getattr(b, "type", None) == "text").strip()
        if not text:
            return {"status": "error", "reason": "empty Claude response",
                    "review_text": None, "model": model, "context": ctx}
        return {"status": "ok", "reason": None, "review_text": text,
                "model": model, "context": ctx}
    except Exception as e:
        return {"status": "error", "reason": f"{type(e).__name__}: {e}",
                "review_text": None, "model": model, "context": ctx}


def prepare_prompt(review_date: date) -> dict:
    """Assemble the system + user prompt for `review_date` without calling
    any API. Used by the dashboard's paste-flow for claude.ai subscribers
    who don't have an API key.

    Returns: {"system": str, "user": str, "context": dict, "ready": bool, "reason": Optional[str]}
    """
    conn = get_connection()
    ensure_table(conn)
    ctx = _gather_context(review_date, conn)
    conn.close()
    if not ctx.get("signals_today"):
        return {"system": SYSTEM_PROMPT, "user": "", "context": ctx,
                "ready": False,
                "reason": f"No paper-trade snapshot exists for {review_date}. "
                          f"Run `python scripts/snapshot_paper_trades.py --date {review_date}` first."}
    user_prompt = _format_context_for_llm(ctx)
    return {"system": SYSTEM_PROMPT, "user": user_prompt, "context": ctx,
            "ready": True, "reason": None}


def save_review(review_date: date, model: str, context: dict, review_text: str) -> int:
    conn = get_connection()
    ensure_table(conn)
    conn.execute(
        "INSERT OR REPLACE INTO ai_reviews (review_date, model, context_json, review_text) "
        "VALUES (?, ?, ?, ?)",
        (review_date.isoformat(), model, json.dumps(context, default=str), review_text),
    )
    conn.commit()
    rid = conn.execute("SELECT review_id FROM ai_reviews WHERE review_date=?",
                       (review_date.isoformat(),)).fetchone()[0]
    conn.close()
    return rid


def load_reviews(limit: int = 60) -> list:
    conn = get_connection()
    ensure_table(conn)
    rows = conn.execute(
        "SELECT review_date, model, review_text, context_json, created_at "
        "FROM ai_reviews ORDER BY review_date DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        try:
            ctx = json.loads(r[3]) if r[3] else {}
        except Exception:
            ctx = {}
        out.append({
            "review_date": r[0], "model": r[1], "review_text": r[2],
            "context": ctx, "created_at": r[4],
        })
    return out

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


_SIGNAL_THRESHOLD = 0.10
_STRONG_THRESHOLD = 0.40


def _direction_from_composite(c):
    if c is None:
        return "FLAT"
    if c >= _SIGNAL_THRESHOLD:
        return "LONG"
    if c <= -_SIGNAL_THRESHOLD:
        return "SHORT"
    return "FLAT"


def _position_from_composite(c):
    if c is None:
        return 0.0
    if c >= _STRONG_THRESHOLD:
        return 2.0
    if c >= _SIGNAL_THRESHOLD:
        return 1.0
    if c <= -_STRONG_THRESHOLD:
        return -2.0
    if c <= -_SIGNAL_THRESHOLD:
        return -1.0
    return 0.0


def ensure_table(conn: Optional[sqlite3.Connection] = None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    conn.executescript(CREATE_SQL)
    conn.commit()
    if own:
        conn.close()


def _gather_context(review_date: date, conn: sqlite3.Connection) -> dict:
    """Collect the inputs the LLM will reason over for the review.

    Signal data comes from TWO sources to expose any morning-vs-live drift:
      - `signals_live` — composite computed fresh right now (same as Signal tab)
      - `signals_morning_snapshot` — what paper_trades locked in at the 07:00
        cron (may be FLAT or missing on quiet days)
    Themes / titles / recent closed trades come from the DB as before.
    """
    # Import here to avoid top-level circular dependency.
    import pandas as pd
    from app.scoring.composite import composite_score
    from app.scoring.factors import positioning_factor, inventory_factor, term_structure_factor
    from app.strategy.backtest_engine import aggregate_score_by_date

    SYMBOL_BOOK_PAIRS = [("WTI", "wti_outright"), ("Brent", "brent_outright")]
    cfg_path = Path(__file__).resolve().parents[1] / "config" / "multi_strategy_config.json"

    def _book_cfg(name):
        cfg = json.loads(cfg_path.read_text())
        for b in cfg.get("books", []):
            if b.get("name") == name:
                return b
        return {}

    def _narrative_z(book_cfg, theme_scores_df, asof_d):
        weights = (book_cfg.get("scoring") or {}).get("theme_weights")
        rows = [{"score_date": str(r["score_date"]), "theme": r["theme"],
                 "narrative_score": float(r["narrative_score"])}
                for _, r in theme_scores_df.iterrows()]
        agg = aggregate_score_by_date(rows, weights=weights, group_field="theme")
        if not agg:
            return None
        df = pd.DataFrame(agg).sort_values("score_date").reset_index(drop=True)
        df["score_date"] = df["score_date"].astype(str)
        df["aggregate_score"] = df["aggregate_score"].astype(float)
        before = df[df["score_date"] <= asof_d.isoformat()].tail(31)
        if len(before) < 6:
            return None
        today_val = float(before.iloc[-1]["aggregate_score"])
        prior = before.iloc[:-1]["aggregate_score"]
        mean, std = prior.mean(), prior.std()
        if std == 0 or pd.isna(std):
            return None
        return (today_val - mean) / std

    ctx: dict = {"review_date": review_date.isoformat()}

    # --- Source A: LIVE composite computed right now (matches Signal tab) ---
    theme_scores = pd.read_sql(
        "SELECT score_date, theme, narrative_score FROM daily_theme_scores "
        "WHERE commodity='crude_oil'",
        conn,
    )
    ctx["signals_live"] = []
    for sym, book_name in SYMBOL_BOOK_PAIRS:
        regime_row = conn.execute(
            "SELECT primary_regime FROM daily_regimes WHERE symbol=? AND regime_date<=? "
            "ORDER BY regime_date DESC LIMIT 1",
            (sym, review_date.isoformat()),
        ).fetchone()
        regime = regime_row[0] if regime_row else None
        nz = _narrative_z(_book_cfg(book_name), theme_scores, review_date) if regime else None
        try: ts = term_structure_factor(sym, review_date)
        except Exception: ts = None
        try: pos = positioning_factor(sym, review_date)
        except Exception: pos = None
        try: inv = inventory_factor(sym, review_date)
        except Exception: inv = None
        composite = None
        breakdown = []
        if regime and nz is not None:
            try:
                out = composite_score(sym, regime, nz,
                                      {"term_structure": ts, "positioning": pos, "inventory": inv})
                composite = float(out["total"])
                breakdown = out.get("breakdown", [])
            except KeyError:
                pass
        ctx["signals_live"].append({
            "symbol": sym, "regime": regime, "composite": composite,
            "narrative_z": nz, "term_structure": ts, "positioning": pos, "inventory": inv,
            "breakdown": breakdown,
        })

    # --- Source B: morning paper_trades snapshot (what got LOCKED at 07:00) ---
    snaps = conn.execute(
        "SELECT symbol, plan_date, direction, target_position, composite_score, regime, "
        "       narrative_z, term_structure, positioning, inventory, "
        "       breakdown_json, entry_close, reasoning "
        "FROM paper_trades WHERE plan_date=? ORDER BY symbol",
        (review_date.isoformat(),),
    ).fetchall()
    ctx["signals_morning_snapshot"] = []
    for r in snaps:
        try:
            breakdown = json.loads(r[10]) if r[10] else []
        except Exception:
            breakdown = []
        ctx["signals_morning_snapshot"].append({
            "symbol": r[0], "plan_date": r[1], "direction": r[2], "target_position": r[3],
            "composite": r[4], "regime": r[5],
            "narrative_z": r[6], "term_structure": r[7],
            "positioning": r[8], "inventory": r[9],
            "breakdown": breakdown, "entry_close": r[11],
            "auto_reasoning": r[12],
        })

    # Back-compat: keep `signals_today` keyed to the live readings (most prompts /
    # formatters expect that key). Morning snapshot is exposed alongside.
    ctx["signals_today"] = [
        {**s, "direction": _direction_from_composite(s.get("composite")),
              "target_position": _position_from_composite(s.get("composite")),
              "entry_close": None, "auto_reasoning": None}
        for s in ctx["signals_live"]
    ]

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

    # LIVE signal — what the Signal tab shows right now
    lines.append("== Live signal (computed at review time, matches Signal tab) ==")
    live = ctx.get("signals_live") or []
    if not live:
        lines.append("(no live signal — likely missing data or regime row)")
    for s in live:
        comp = s["composite"]
        comp_str = f"{comp:+.3f}" if isinstance(comp, (int, float)) else "n/a"
        direction = _direction_from_composite(comp)
        size = abs(_position_from_composite(comp))
        lines.append(
            f"{s['symbol']}: {direction} {size:.0f}x · "
            f"regime `{s.get('regime') or '?'}` · composite {comp_str}"
        )
        for f in ["narrative_z", "term_structure", "positioning", "inventory"]:
            v = s.get(f)
            if isinstance(v, (int, float)):
                lines.append(f"  - {f}: {v:+.3f}")
    lines.append("")

    # MORNING snapshot — what paper_trades locked in at the 07:00 cron
    morning = ctx.get("signals_morning_snapshot") or []
    lines.append("== Morning paper-trade snapshot (07:00 cron lock) ==")
    if not morning:
        lines.append("(no morning snapshot — composite was FLAT at 07:00 today, so no row was opened)")
    for s in morning:
        comp = s["composite"]
        comp_str = f"{comp:+.3f}" if isinstance(comp, (int, float)) else "n/a"
        lines.append(
            f"{s['symbol']} ({s['plan_date']}): {s['direction']} {abs(s['target_position'] or 0):.0f}x · "
            f"regime `{s['regime']}` · composite {comp_str}"
        )
        if s.get("auto_reasoning"):
            lines.append(f"  rule-based reasoning: {s['auto_reasoning']}")
        if s.get("entry_close"):
            lines.append(f"  entry close locked at: {s['entry_close']:,.2f}")

    # Drift note if live and morning meaningfully diverge
    drift_lines = []
    for live_s in live:
        morn_match = next((m for m in morning if m["symbol"] == live_s["symbol"]), None)
        if morn_match and isinstance(morn_match.get("composite"), (int, float)) and isinstance(live_s.get("composite"), (int, float)):
            delta = live_s["composite"] - morn_match["composite"]
            if abs(delta) >= 0.20:
                drift_lines.append(
                    f"  ⚠ {live_s['symbol']} composite drifted {delta:+.3f} since 07:00 "
                    f"(morning {morn_match['composite']:+.3f} → live {live_s['composite']:+.3f})"
                )
    if drift_lines:
        lines.append("\n== Intraday drift (live vs morning) ==")
        lines.extend(drift_lines)
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

    if not any(s.get("composite") is not None for s in ctx.get("signals_live", [])):
        return {"status": "skipped",
                "reason": "no live composite (missing regime / narrative for this date)",
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
    if not any(s.get("composite") is not None for s in ctx.get("signals_live", [])):
        return {"system": SYSTEM_PROMPT, "user": "", "context": ctx,
                "ready": False,
                "reason": f"No live composite for {review_date}. Missing regime row, or narrative_z "
                          f"requires at least 6 days of theme scores up to this date — verify the "
                          f"hourly pipeline ran for fetch_prices / compute_regimes / score_narratives."}
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

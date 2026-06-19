"""Keyless LLM direction adjudication (cost-capped).

The rule extractor handles topic/structure; this uses Claude (via the keyless
`claude` CLI, or the SDK if a key is set) to make the *direction* call on the
chunks where the keyword rule fails — risk-topic chunks whose text contains
de-escalation / diplomacy language (ceasefire, MoU, peace deal, sanctions
relief, reopening, …). Pure-escalation chunks (attacks, outages) are left to
the rule extractor, which gets them right — so we only spend tokens where it
matters.

Durable because extraction is now incremental (old chunks aren't re-extracted),
and we record every adjudicated chunk so we never pay for it twice.

Cost control: hard per-run cap (--cap) + a usage line (chunks + est. tokens).

  python scripts/llm_adjudicate_direction.py --since 2026-06-10        # run
  python scripts/llm_adjudicate_direction.py --since 2026-06-10 --cap 50
  python scripts/llm_adjudicate_direction.py --dry-run --cap 5
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.database import get_connection
from app.extractors.llm_providers import _claude_cli_path, _extract_json_obj

WAR_TOPICS = ("geopolitical_risk", "shipping_disruption", "supply_disruption",
              "sanctions_risk", "weather_risk")

# Selection filter — chunks the rule extractor is likely to mis-sign. Broadened
# beyond ceasefire to the diplomacy / de-escalation vocabulary this cycle uses.
# (Substrings chosen to avoid false hits — e.g. "memorandum" not bare "mou".)
SELECT_LIKE = [
    "ceasefire", "cease-fire", "truce", "peace deal", "peace agreement",
    "peace accord", "memorandum", "understanding", "agreement reached",
    "reach a deal", "reached a deal", "sign a deal", "signed a deal", "deal signed",
    "accord", "pact", "sanctions relief", "sanctions waiver", "lift sanctions",
    "lifted sanctions", "easing sanctions", "reopen", "restored", "withdraw",
    "de-escalat", "deescalat", "normaliz", "detente", "rapprochement",
    "diplomat", "negotiat", "talks",
]

_INSTRUCTION = (
    "You judge the NET crude-oil price impact of a news excerpt for a markets "
    "model. Rules:\n"
    "- De-escalation of a supply/geopolitical risk is BEARISH (premium unwinds): "
    "a ceasefire/MoU/peace deal that is holding or signed, a Strait/port "
    "REOPENING, supply RESTORED, sanctions LIFTED/eased, troops withdrawing.\n"
    "- BUT if it's fragile, unsigned, failing, violated, or the physical "
    "disruption persists (Strait still blocked, barrels still offline, fighting "
    "resumes), it stays BULLISH — the premium has not actually unwound.\n"
    "- Ongoing escalation / new attacks / fresh outages are BULLISH.\n"
    "- Use 'mixed' only when genuinely two-sided.\n"
    "Return ONLY a JSON object: {\"direction\": \"bullish|bearish|mixed\", "
    "\"reason\": \"one short clause\"}."
)


def ensure_table(conn) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS llm_direction_adjudicated ("
                 "chunk_id TEXT PRIMARY KEY, direction TEXT, reason TEXT, "
                 "adjudicated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.commit()


def adjudicate(text: str, cli: str, timeout: int = 60) -> tuple[str | None, str, int, int]:
    """Return (direction, reason, est_in_tokens, est_out_tokens)."""
    prompt = _INSTRUCTION + "\n\n--- EXCERPT ---\n" + (text or "")[:2000]
    est_in = len(prompt) // 4  # ~4 chars/token
    try:
        # Pin a lighter model than the CLI default (Opus): direction is a simple
        # call, and this cuts plan/quota usage ~5x while keeping the nuance.
        proc = subprocess.run(
            [cli, "-p", prompt, "--output-format", "json", "--model", "claude-sonnet-4-6"],
            capture_output=True, text=True, timeout=timeout)
    except Exception as e:
        return None, f"cli_error:{type(e).__name__}", est_in, 0
    if proc.returncode != 0:
        return None, f"exit{proc.returncode}", est_in, 0
    try:
        result_text = json.loads(proc.stdout).get("result", "")
    except Exception:
        result_text = proc.stdout
    data = _extract_json_obj(result_text)
    if not data:
        return None, "no_json", est_in, len(result_text) // 4
    d = str(data.get("direction", "")).lower().strip()
    if d not in ("bullish", "bearish", "mixed"):
        return None, f"bad_dir:{d}", est_in, len(result_text) // 4
    return d, str(data.get("reason", ""))[:120], est_in, len(result_text) // 4


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-06-10")
    ap.add_argument("--cap", type=int, default=400, help="hard max chunks this run (cost guard)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cli = _claude_cli_path()
    if not cli:
        print("No `claude` CLI found — cannot adjudicate (set ANTHROPIC_API_KEY or install Claude Code).")
        return

    conn = get_connection()
    ensure_table(conn)
    placeholders = ",".join("?" for _ in WAR_TOPICS)
    like_clause = " OR ".join("LOWER(ch.text) LIKE ?" for _ in SELECT_LIKE)
    rows = conn.execute(
        f"""SELECT DISTINCT e.chunk_id, ch.text
            FROM narrative_events e JOIN chunks ch ON e.chunk_id = ch.chunk_id
            WHERE substr(e.event_time,1,10) >= ?
              AND e.topic IN ({placeholders})
              AND ({like_clause})
              AND e.chunk_id NOT IN (SELECT chunk_id FROM llm_direction_adjudicated)""",
        (args.since, *WAR_TOPICS, *[f"%{w}%" for w in SELECT_LIKE]),
    ).fetchall()

    total_candidates = len(rows)
    capped = rows[: args.cap]
    if total_candidates > args.cap:
        print(f"[cap] {total_candidates} candidates, processing {args.cap} this run "
              f"(rest carry to next run).")

    changed = 0
    counts = {"bullish": 0, "bearish": 0, "mixed": 0}
    tin = tout = 0
    for i, (chunk_id, text) in enumerate(capped, 1):
        direction, reason, ein, eout = adjudicate(text, cli)
        tin += ein; tout += eout
        if direction is None:
            print(f"  [{i}/{len(capped)}] {chunk_id} -> skip ({reason})")
            continue
        counts[direction] += 1
        if not args.dry_run:
            cur = conn.execute(
                f"UPDATE narrative_events SET direction=? WHERE chunk_id=? "
                f"AND topic IN ({placeholders}) AND direction!=?",
                (direction, chunk_id, *WAR_TOPICS, direction))
            changed += cur.rowcount
            conn.execute("INSERT OR REPLACE INTO llm_direction_adjudicated "
                         "(chunk_id, direction, reason) VALUES (?,?,?)",
                         (chunk_id, direction, reason))
        if i % 10 == 0:
            # Commit incrementally so progress survives interruption — and since
            # we skip already-adjudicated chunks, a re-run resumes where it left
            # off (important: the keyless CLI is ~15s/call, so runs are long).
            if not args.dry_run:
                conn.commit()
            print(f"  [{i}/{len(capped)}] … {counts}", flush=True)
    if not args.dry_run:
        conn.commit()
    conn.close()

    # ~Sonnet pricing for reference ($3/$15 per Mtok); CLI runs on the Claude
    # Code plan (no metered bill) — this is just a usage gauge.
    est_cost = tin / 1e6 * 3 + tout / 1e6 * 15
    print(f"\nVerdicts: {counts}. Events re-signed: {changed}. "
          f"Candidates remaining: {max(0, total_candidates - len(capped))}.")
    print(f"[usage] {len(capped)} calls · ~{tin:,} in + ~{tout:,} out tokens "
          f"· ≈${est_cost:.2f} at Sonnet rates")
    print("Re-run `python scripts/score_narratives.py` to refresh the narrative.")


if __name__ == "__main__":
    main()

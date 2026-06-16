"""Bounded LLM re-classification of direction for the recent de-escalation window.

The deterministic guard (app/extractors/deescalation.py) is blunt — it flips on
keywords. This pass uses Claude (via the keyless `claude` CLI) to adjudicate the
*direction* of just the ambiguous chunks: those since --since on a risk-escalation
topic whose text mentions de-escalation language. Claude can tell
"ceasefire holding, Strait reopening, supply restored" (BEARISH) from
"fragile ceasefire, Strait still blocked, fighting resumes" (still BULLISH) —
the distinction neither the keyword rule nor the deterministic guard can make.

Updates direction in place on existing events (preserves topic granularity and
event counts). Re-run `python scripts/score_narratives.py` afterwards.

  python scripts/llm_redirect_recent.py --since 2026-06-10           # run
  python scripts/llm_redirect_recent.py --since 2026-06-10 --limit 5 # smoke test
  python scripts/llm_redirect_recent.py --dry-run --limit 5
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

DEESC_LIKE = ("%ceasefire%", "%truce%", "%reopen%", "%restored%", "%peace%",
              "%sanctions lifted%", "%withdraw%", "%de-escalat%", "%normaliz%")

_INSTRUCTION = (
    "You judge the NET crude-oil price impact of a news excerpt for a markets "
    "model. Rules:\n"
    "- De-escalation of a supply/geopolitical risk is BEARISH (the risk premium "
    "unwinds): a ceasefire that is holding, a Strait/port REOPENING, supply "
    "RESTORED, sanctions LIFTED or eased, troops withdrawing, a conflict ending.\n"
    "- BUT if that de-escalation is fragile, failing, violated, or the physical "
    "disruption persists (Strait still blocked, barrels still offline, fighting "
    "resumes), it stays BULLISH — the premium has not actually unwound.\n"
    "- Ongoing escalation / new attacks / fresh outages are BULLISH.\n"
    "- Use 'mixed' only when genuinely two-sided.\n"
    "Return ONLY a JSON object: {\"direction\": \"bullish|bearish|mixed\", "
    "\"reason\": \"one short clause\"}."
)


def adjudicate(text: str, cli: str, timeout: int = 60) -> tuple[str | None, str]:
    prompt = _INSTRUCTION + "\n\n--- EXCERPT ---\n" + (text or "")[:2000]
    try:
        proc = subprocess.run([cli, "-p", prompt, "--output-format", "json"],
                              capture_output=True, text=True, timeout=timeout)
    except Exception as e:
        return None, f"cli_error:{type(e).__name__}"
    if proc.returncode != 0:
        return None, f"exit{proc.returncode}"
    try:
        result_text = json.loads(proc.stdout).get("result", "")
    except Exception:
        result_text = proc.stdout
    data = _extract_json_obj(result_text)
    if not data:
        return None, "no_json"
    d = str(data.get("direction", "")).lower().strip()
    if d not in ("bullish", "bearish", "mixed"):
        return None, f"bad_dir:{d}"
    return d, str(data.get("reason", ""))[:120]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-06-10")
    ap.add_argument("--limit", type=int, default=0, help="cap chunks (0=all)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cli = _claude_cli_path()
    if not cli:
        print("No `claude` CLI found — cannot run LLM adjudication.")
        return

    conn = get_connection()
    placeholders = ",".join("?" for _ in WAR_TOPICS)
    like_clause = " OR ".join("LOWER(ch.text) LIKE ?" for _ in DEESC_LIKE)
    rows = conn.execute(
        f"""SELECT DISTINCT e.chunk_id, ch.text
            FROM narrative_events e JOIN chunks ch ON e.chunk_id = ch.chunk_id
            WHERE substr(e.event_time,1,10) >= ?
              AND e.topic IN ({placeholders})
              AND ({like_clause})""",
        (args.since, *WAR_TOPICS, *DEESC_LIKE),
    ).fetchall()
    if args.limit:
        rows = rows[: args.limit]

    print(f"Adjudicating {len(rows)} chunks (since {args.since})…")
    changed = 0
    counts = {"bullish": 0, "bearish": 0, "mixed": 0}
    for i, (chunk_id, text) in enumerate(rows, 1):
        direction, reason = adjudicate(text, cli)
        if direction is None:
            print(f"  [{i}/{len(rows)}] {chunk_id} -> skip ({reason})")
            continue
        counts[direction] += 1
        if not args.dry_run:
            cur = conn.execute(
                f"""UPDATE narrative_events SET direction=?
                    WHERE chunk_id=? AND topic IN ({placeholders}) AND direction!=?""",
                (direction, chunk_id, *WAR_TOPICS, direction),
            )
            changed += cur.rowcount
        if i % 20 == 0 or args.dry_run:
            print(f"  [{i}/{len(rows)}] {chunk_id} -> {direction} ({reason})")
    if not args.dry_run:
        conn.commit()
    conn.close()
    print(f"\nVerdicts: {counts}. Events updated: {changed}.")
    print("Now re-run: python scripts/score_narratives.py")


if __name__ == "__main__":
    main()

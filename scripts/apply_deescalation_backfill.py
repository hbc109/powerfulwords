"""One-time backfill: re-sign existing narrative_events with the de-escalation
guard, so the live narrative score stops treating ceasefire / reopening /
supply-restored / sanctions-lifted news as bullish.

Deterministic and conservative (see app/extractors/deescalation.py) — it only
flips a *bullish* risk-topic event to bearish when de-escalation language in the
underlying chunk strictly out-weighs re-escalation/collapse language. Uses the
full chunk text (not the short evidence excerpt) for the most context.

After running, re-run `python scripts/score_narratives.py` to refresh
daily_narrative_scores.

  python scripts/apply_deescalation_backfill.py            # apply
  python scripts/apply_deescalation_backfill.py --dry-run  # report only
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.database import get_connection
from app.extractors.deescalation import resolve_direction


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--since", default=None, help="only events with event_time >= this date")
    args = ap.parse_args()

    conn = get_connection()
    where = "WHERE e.direction = 'bullish'"
    params: tuple = ()
    if args.since:
        where += " AND substr(e.event_time,1,10) >= ?"
        params = (args.since,)
    rows = conn.execute(
        f"""SELECT e.event_id, e.topic, COALESCE(ch.text, e.evidence_text)
            FROM narrative_events e
            LEFT JOIN chunks ch ON e.chunk_id = ch.chunk_id
            {where}""",
        params,
    ).fetchall()

    flips_by_topic: Counter = Counter()
    to_update: list[str] = []
    for event_id, topic, text in rows:
        new_dir, flipped = resolve_direction(text or "", topic, "bullish")
        if flipped:
            flips_by_topic[topic] += 1
            to_update.append(event_id)

    print(f"Scanned {len(rows)} bullish events; {len(to_update)} would flip to bearish.")
    for topic, n in flips_by_topic.most_common():
        print(f"  {topic:<22} {n}")

    if args.dry_run:
        print("(dry run — no changes written)")
        conn.close()
        return

    conn.executemany(
        "UPDATE narrative_events SET direction='bearish' WHERE event_id=?",
        [(e,) for e in to_update],
    )
    conn.commit()
    conn.close()
    print(f"Flipped {len(to_update)} events. Now re-run: python scripts/score_narratives.py")


if __name__ == "__main__":
    main()

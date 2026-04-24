from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json
from pathlib import Path

from app.db.database import get_connection
from app.scoring.daily_score import load_scoring_config, aggregate_daily_scores

BASE_DIR = Path(__file__).resolve().parents[1]


def fetch_events(conn):
    cur = conn.execute(
        '''
        SELECT
            event_id, event_time, commodity, topic, direction, source_bucket, source_name,
            credibility, novelty, verification_status, horizon, rumor_flag, confidence
        FROM narrative_events
        ORDER BY event_time, topic
        '''
    )
    rows = cur.fetchall()
    events = []
    for r in rows:
        events.append({
            "event_id": r[0],
            "event_time": r[1],
            "commodity": r[2],
            "topic": r[3],
            "direction": r[4],
            "source_bucket": r[5],
            "source_name": r[6],
            "credibility": r[7],
            "novelty": r[8],
            "verification_status": r[9],
            "horizon": r[10],
            "rumor_flag": bool(r[11]),
            "confidence": r[12],
        })
    return events


def upsert_daily_scores(conn, scores):
    for s in scores:
        conn.execute(
            '''
            INSERT OR REPLACE INTO daily_narrative_scores (
                score_date, commodity, topic, narrative_score, raw_score, event_count,
                breadth, persistence, source_divergence,
                official_confirmation_score, news_breadth_score, chatter_score, crowding_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                s["score_date"],
                s["commodity"],
                s["topic"],
                s["narrative_score"],
                s.get("raw_score"),
                s.get("event_count"),
                s.get("breadth"),
                s.get("persistence"),
                s.get("source_divergence"),
                s["official_confirmation_score"],
                s["news_breadth_score"],
                s["chatter_score"],
                s["crowding_score"],
            ),
        )


def main():
    conn = get_connection()
    cfg = load_scoring_config()
    events = fetch_events(conn)
    scores = aggregate_daily_scores(events, cfg)

    out_dir = BASE_DIR / "data" / "processed" / "signals"
    out_dir.mkdir(parents=True, exist_ok=True)

    for s in scores:
        name = f'{s["score_date"]}_{s["commodity"]}_{s["topic"]}.json'
        (out_dir / name).write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")

    upsert_daily_scores(conn, scores)
    conn.commit()
    conn.close()

    print(f"Scored {len(scores)} daily topic rows from {len(events)} events.")


if __name__ == "__main__":
    main()

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import json
import os
from pathlib import Path

from app.db.database import get_connection
from app.extractors.oil_narrative_extractor import load_rules, extract_events_from_chunk
from app.extractors.llm_narrative_extractor import extract_event_from_chunk_llm, load_llm_config

BASE_DIR = Path(__file__).resolve().parents[1]


def fetch_documents_and_chunks(conn):
    query = '''
    SELECT
        d.document_id,
        d.source_id,
        d.source_bucket,
        d.source_name,
        d.published_at,
        c.chunk_id,
        c.chunk_index,
        c.text
    FROM documents d
    JOIN chunks c ON d.document_id = c.document_id
    ORDER BY d.document_id, c.chunk_index
    '''
    cur = conn.execute(query)
    rows = cur.fetchall()
    items = []
    for r in rows:
        items.append(
            {
                'document_id': r[0],
                'source_id': r[1],
                'source_bucket': r[2],
                'source_name': r[3],
                'published_at': r[4],
                'chunk_id': r[5],
                'chunk_index': r[6],
                'text': r[7],
            }
        )
    return items


def insert_event(conn, evt):
    conn.execute(
        '''
        INSERT OR REPLACE INTO narrative_events (
            event_id, document_id, chunk_id, event_time, commodity, topic, direction,
            source_bucket, source_name, credibility, novelty, breadth, persistence,
            crowding, price_confirmation, verification_status, horizon, rumor_flag,
            confidence, entities_json, regions_json, asset_candidates_json,
            evidence_text, evidence_spans_json, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            evt.event_id,
            evt.document_id,
            evt.chunk_id,
            evt.event_time.isoformat(),
            evt.commodity,
            evt.topic,
            evt.direction,
            evt.source_bucket,
            evt.source_name,
            evt.credibility,
            evt.novelty,
            evt.breadth,
            evt.persistence,
            evt.crowding,
            evt.price_confirmation,
            evt.verification_status,
            evt.horizon,
            int(evt.rumor_flag),
            evt.confidence,
            json.dumps(evt.entities, ensure_ascii=False),
            json.dumps(evt.regions, ensure_ascii=False),
            json.dumps(evt.asset_candidates, ensure_ascii=False),
            evt.evidence_text,
            json.dumps(evt.evidence_spans, ensure_ascii=False),
            evt.notes,
        ),
    )


def choose_mode(requested_mode: str) -> str:
    if requested_mode in ('rule', 'llm'):
        return requested_mode
    llm_cfg = load_llm_config()
    if os.environ.get('OPENAI_API_KEY'):
        return 'llm'
    if llm_cfg.get('mode_default') == 'llm':
        return 'llm'
    return 'rule'


def extract_with_mode(document: dict, chunk: dict, mode: str, rules: dict) -> list:
    if mode == 'rule':
        return extract_events_from_chunk(document=document, chunk=chunk, rules=rules)
    if mode == 'llm':
        evt = extract_event_from_chunk_llm(document=document, chunk=chunk)
        return [evt] if evt is not None else []
    raise ValueError(f'Unsupported mode: {mode}')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['auto', 'rule', 'llm'], default='auto')
    args = parser.parse_args()

    conn = get_connection()
    rules = load_rules()
    rows = fetch_documents_and_chunks(conn)

    out_dir = BASE_DIR / 'data' / 'processed' / 'events'
    out_dir.mkdir(parents=True, exist_ok=True)

    selected_mode = choose_mode(args.mode)
    llm_cfg = load_llm_config()
    allow_fallback = bool(llm_cfg.get('fallback_to_rules', True))

    count = 0
    llm_count = 0
    rule_count = 0
    skipped = 0
    fallback_count = 0

    for row in rows:
        document = {
            'document_id': row['document_id'],
            'source_id': row['source_id'],
            'source_bucket': row['source_bucket'],
            'source_name': row['source_name'],
            'published_at': row['published_at'],
        }
        chunk = {
            'chunk_id': row['chunk_id'],
            'chunk_index': row['chunk_index'],
            'text': row['text'],
        }

        events = []
        used_mode = selected_mode

        try:
            events = extract_with_mode(document, chunk, selected_mode, rules)
        except Exception as e:
            if selected_mode == 'llm' and allow_fallback:
                events = extract_events_from_chunk(document=document, chunk=chunk, rules=rules)
                used_mode = 'rule'
                fallback_count += 1
                print(f"[FALLBACK] {chunk['chunk_id']} -> rule extractor ({e})")
            else:
                print(f"[SKIP] {chunk['chunk_id']} failed in mode={selected_mode}: {e}")
                skipped += 1
                continue

        if not events:
            skipped += 1
            continue

        for evt in events:
            insert_event(conn, evt)
            (out_dir / f'{evt.event_id}.json').write_text(
                json.dumps(evt.model_dump(mode='json'), ensure_ascii=False, indent=2),
                encoding='utf-8',
            )
            count += 1
            if used_mode == 'llm':
                llm_count += 1
            else:
                rule_count += 1

    conn.commit()
    conn.close()
    print(
        f'Extracted {count} narrative events. '
        f'mode={selected_mode}, llm={llm_count}, rule={rule_count}, fallback={fallback_count}, skipped={skipped}'
    )


if __name__ == '__main__':
    main()

import json
import sqlite3
from typing import Iterable
from app.models.document_record import DocumentRecord

def insert_source(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO sources (
            source_id, source_name, source_bucket, access_mode, cost_level, credibility_tier, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["source_id"], row["source_name"], row["source_bucket"], row["access_mode"],
            row["cost_level"], row["credibility_tier"], row.get("notes"),
        ),
    )

def insert_document(conn: sqlite3.Connection, doc: DocumentRecord) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO documents (
            document_id, source_id, source_bucket, file_path, title, source_name,
            publisher_or_channel, language, region, commodity, subtheme, access_mode,
            cost_level, rights_note, published_at, checksum, quality_tier,
            rumor_flag, verification_status, raw_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            doc.document_id, doc.source_id, doc.source_bucket, doc.file_path, doc.title, doc.source_name,
            doc.publisher_or_channel, doc.language, doc.region, doc.commodity, doc.subtheme,
            doc.access_mode, doc.cost_level, doc.rights_note,
            doc.published_at.isoformat() if doc.published_at else None,
            doc.checksum, doc.quality_tier, int(doc.rumor_flag), doc.verification_status, doc.raw_text,
        ),
    )

def repoint_document_date(conn: sqlite3.Connection, document_id: str, new_date: str) -> dict:
    """Fix a wrong publication date on an already-ingested document.

    Updates `documents.published_at`, propagates the change to every
    `narrative_events.event_time` row for this document, and renames
    the inbox file's date prefix on disk so future re-ingest reads it
    correctly. Returns a dict summarizing what changed.

    `new_date` must be ISO YYYY-MM-DD. The hh:mm:ss portion is set to
    midnight UTC to match how ingest_folder writes dates.

    Caller should re-run score_narratives.py afterwards to fold the
    corrected date into daily_narrative_scores / daily_theme_scores.
    """
    from datetime import date as _date
    from pathlib import Path
    # Validate
    _date.fromisoformat(new_date)
    new_iso = f"{new_date}T00:00:00"

    cur = conn.execute(
        "SELECT published_at, file_path FROM documents WHERE document_id = ?",
        (document_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"document_id {document_id} not found")
    old_iso, file_path = row

    n_events = conn.execute(
        "UPDATE narrative_events SET event_time = ? WHERE document_id = ?",
        (new_iso, document_id),
    ).rowcount
    conn.execute(
        "UPDATE documents SET published_at = ? WHERE document_id = ?",
        (new_iso, document_id),
    )

    renamed_to = None
    if file_path:
        p = Path(file_path)
        if p.exists():
            old_prefix = p.name[:10]  # YYYY-MM-DD
            if len(old_prefix) == 10 and old_prefix[4] == "-" and old_prefix[7] == "-":
                new_name = new_date + p.name[10:]
                new_path = p.with_name(new_name)
                if new_path != p and not new_path.exists():
                    p.rename(new_path)
                    conn.execute(
                        "UPDATE documents SET file_path = ? WHERE document_id = ?",
                        (str(new_path), document_id),
                    )
                    renamed_to = str(new_path)

    return {
        "document_id": document_id,
        "old_published_at": old_iso,
        "new_published_at": new_iso,
        "events_updated": n_events,
        "file_renamed_to": renamed_to,
    }


def insert_chunks(conn: sqlite3.Connection, document_id: str, chunks: Iterable[dict]) -> None:
    for c in chunks:
        conn.execute(
            """
            INSERT OR REPLACE INTO chunks (
                chunk_id, document_id, chunk_index, text, token_estimate, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                c["chunk_id"], document_id, c["chunk_index"], c["text"], c["token_estimate"],
                json.dumps(c.get("metadata", {}), ensure_ascii=False),
            ),
        )

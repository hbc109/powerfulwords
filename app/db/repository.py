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

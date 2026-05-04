"""Walk data/inbox/<source_bucket>/<source_id>/ and ingest every file.

No manifest required. Metadata comes from:
- source_bucket: parent folder of the file (relative to data/inbox/)
- source_id: immediate folder of the file
- published_at: leading `YYYY-MM-DD` of the filename
- title: filename without extension and date prefix
- everything else (source_name, cost_level, access_mode, credibility): looked
  up from the sources table for the source_id

Files already ingested (matching checksum -> document_id) are skipped, so
re-running is safe.
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import re
from datetime import datetime

from app.db.database import init_db, get_connection
from app.db.repository import insert_document, insert_chunks
from app.models.document_record import DocumentRecord
from app.utils import (
    chunk_text,
    clean_text,
    extract_text_from_file,
    sha256_of_file,
    write_json,
)

INBOX_ROOT = BASE_DIR / "data" / "inbox"
SUPPORTED_EXTS = {".txt", ".pdf", ".docx", ".xlsx", ".xls"}
DATE_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[_\-\s](.+)$")


def parse_filename(stem: str) -> tuple[datetime | None, str]:
    """Return (published_at, title) parsed from `YYYY-MM-DD_title`.

    If no date prefix matches, returns (None, stem) — caller decides whether
    to skip.
    """
    m = DATE_PREFIX_RE.match(stem)
    if not m:
        return None, stem
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d"), m.group(2)
    except ValueError:
        return None, stem


def lookup_source(conn, source_id: str) -> dict | None:
    cur = conn.execute(
        "SELECT source_id, source_name, source_bucket, access_mode, cost_level, credibility_tier "
        "FROM sources WHERE source_id = ?",
        (source_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "source_id": row[0],
        "source_name": row[1],
        "source_bucket": row[2],
        "access_mode": row[3],
        "cost_level": row[4],
        "credibility_tier": row[5],
    }


def make_document_id(checksum: str) -> str:
    return f"doc_{checksum[:16]}"


def existing_document(conn, document_id: str) -> bool:
    cur = conn.execute("SELECT 1 FROM documents WHERE document_id = ? LIMIT 1", (document_id,))
    return cur.fetchone() is not None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inbox", default=str(INBOX_ROOT), help="Inbox root (default: data/inbox)")
    parser.add_argument("--chunk-chars", type=int, default=1800)
    parser.add_argument("--overlap-chars", type=int, default=250)
    parser.add_argument("--commodity", default="crude_oil")
    parser.add_argument("--reingest", action="store_true", help="Re-ingest files even if checksum already exists")
    args = parser.parse_args()

    inbox_root = Path(args.inbox).resolve()
    if not inbox_root.exists():
        print(f"Inbox not found: {inbox_root}. Run scripts/setup_inbox.py first.")
        sys.exit(1)

    init_db()
    conn = get_connection()

    ingested = skipped = unknown_source = no_date = duplicate = 0

    for path in sorted(inbox_root.rglob("*")):
        if not path.is_file():
            continue
        if path.name == ".gitkeep" or path.name.startswith("."):
            continue
        if path.suffix.lower() not in SUPPORTED_EXTS:
            continue

        rel = path.relative_to(inbox_root)
        if len(rel.parts) < 3:
            print(f"[SKIP-LAYOUT] {rel} — expected <bucket>/<source_id>/<file>")
            skipped += 1
            continue

        source_bucket = rel.parts[0]
        source_id = rel.parts[1]
        src = lookup_source(conn, source_id)
        if src is None:
            print(f"[SKIP-SRC] {rel} — source_id '{source_id}' not in sources table.")
            unknown_source += 1
            continue
        if src["source_bucket"] != source_bucket:
            print(
                f"[SKIP-BUCKET] {rel} — folder bucket '{source_bucket}' "
                f"!= registry bucket '{src['source_bucket']}' for {source_id}."
            )
            skipped += 1
            continue

        published_at, title = parse_filename(path.stem)
        if published_at is None:
            print(f"[SKIP-DATE] {rel} — filename must start with YYYY-MM-DD_")
            no_date += 1
            continue

        try:
            raw_text = clean_text(extract_text_from_file(path))
        except Exception as e:
            print(f"[SKIP-READ] {rel}: {e}")
            skipped += 1
            continue

        checksum = sha256_of_file(path)
        document_id = make_document_id(checksum)
        if not args.reingest and existing_document(conn, document_id):
            duplicate += 1
            continue

        rumor_flag = source_bucket in ("social_open", "social_private_manual")
        if src["source_bucket"] in ("official_data", "official_reports"):
            verification_status = "officially_confirmed"
        elif rumor_flag:
            verification_status = "unverified"
        else:
            verification_status = "partially_confirmed"

        doc = DocumentRecord(
            document_id=document_id,
            source_id=source_id,
            source_bucket=source_bucket,
            file_path=str(path),
            title=title,
            source_name=src["source_name"],
            publisher_or_channel=src["source_name"],
            language=None,
            region=None,
            commodity=args.commodity,
            subtheme=None,
            access_mode=src["access_mode"],
            cost_level=src["cost_level"],
            rights_note=None,
            published_at=published_at,
            checksum=checksum,
            quality_tier=src["credibility_tier"],
            rumor_flag=rumor_flag,
            verification_status=verification_status,
            raw_text=raw_text,
        )
        insert_document(conn, doc)

        raw_chunks = chunk_text(raw_text, chunk_chars=args.chunk_chars, overlap_chars=args.overlap_chars)
        chunks = []
        for c in raw_chunks:
            chunk_id = f"chk_{document_id}_{c['chunk_index']:04d}"
            chunks.append({
                "chunk_id": chunk_id,
                "chunk_index": c["chunk_index"],
                "text": c["text"],
                "token_estimate": c["token_estimate"],
                "metadata": {
                    "source_bucket": source_bucket,
                    "source_name": src["source_name"],
                    "source_id": source_id,
                    "title": title,
                    "rumor_flag": rumor_flag,
                },
            })
        insert_chunks(conn, document_id, chunks)

        clean_dir = BASE_DIR / "data" / "processed" / "clean_text"
        clean_dir.mkdir(parents=True, exist_ok=True)
        (clean_dir / f"{document_id}.txt").write_text(raw_text, encoding="utf-8")

        meta = doc.model_dump(mode="json")
        meta["chunk_count"] = len(chunks)
        write_json(BASE_DIR / "data" / "processed" / "metadata" / f"{document_id}.json", meta)

        ingested += 1
        print(f"[OK] {rel} -> {document_id} ({len(chunks)} chunks)")

    conn.commit()
    conn.close()
    print(
        f"Done. ingested={ingested}, duplicates={duplicate}, "
        f"unknown_source={unknown_source}, missing_date={no_date}, other_skips={skipped}"
    )


if __name__ == "__main__":
    main()

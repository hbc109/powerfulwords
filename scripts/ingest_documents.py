from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
from datetime import datetime
from app.db.database import init_db, get_connection
from app.db.repository import insert_document, insert_chunks
from app.models.document_record import DocumentRecord
from app.utils import chunk_text, clean_text, extract_text_from_file, read_manifest_csv, sha256_of_file, write_json

BASE_DIR = Path(__file__).resolve().parents[1]

def parse_dt(value: str | None):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None

def make_document_id(checksum: str) -> str:
    return f"doc_{checksum[:16]}"

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--chunk-chars", type=int, default=1800)
    parser.add_argument("--overlap-chars", type=int, default=250)
    args = parser.parse_args()
    manifest_path = (BASE_DIR / args.manifest).resolve() if not Path(args.manifest).is_absolute() else Path(args.manifest)
    rows = read_manifest_csv(manifest_path)
    init_db()
    conn = get_connection()
    ingested = skipped = 0
    for row in rows:
        raw_file_path = row["file_path"]
        file_path = (BASE_DIR / raw_file_path).resolve() if not Path(raw_file_path).is_absolute() else Path(raw_file_path)
        if not file_path.exists():
            print(f"[SKIP] missing file: {file_path}")
            skipped += 1
            continue
        try:
            raw_text = clean_text(extract_text_from_file(file_path))
        except Exception as e:
            print(f"[SKIP] failed to read {file_path.name}: {e}")
            skipped += 1
            continue
        checksum = sha256_of_file(file_path)
        document_id = make_document_id(checksum)
        doc = DocumentRecord(
            document_id=document_id,
            source_id=row["source_id"], source_bucket=row["source_bucket"], file_path=str(file_path),
            title=row.get("title") or file_path.stem, source_name=row["source_name"],
            publisher_or_channel=row.get("publisher_or_channel") or None,
            language=row.get("language") or None, region=row.get("region") or None,
            commodity=row.get("commodity") or "crude_oil", subtheme=row.get("subtheme") or None,
            access_mode=row.get("access_mode") or None, cost_level=row.get("cost_level") or None,
            rights_note=row.get("rights_note") or None, published_at=parse_dt(row.get("published_at")),
            checksum=checksum, quality_tier=int(row["quality_tier"]) if row.get("quality_tier") else None,
            rumor_flag=str(row.get("rumor_flag", "")).lower() == "true",
            verification_status=row.get("verification_status") or "unverified", raw_text=raw_text,
        )
        insert_document(conn, doc)
        raw_chunks = chunk_text(raw_text, chunk_chars=args.chunk_chars, overlap_chars=args.overlap_chars)
        chunks = []
        for c in raw_chunks:
            chunk_id = f"chk_{document_id}_{c['chunk_index']:04d}"
            chunks.append({
                "chunk_id": chunk_id, "chunk_index": c["chunk_index"], "text": c["text"], "token_estimate": c["token_estimate"],
                "metadata": {"source_bucket": doc.source_bucket, "source_name": doc.source_name, "title": doc.title, "subtheme": doc.subtheme, "rumor_flag": doc.rumor_flag},
            })
        insert_chunks(conn, document_id, chunks)
        (BASE_DIR / "data" / "processed" / "clean_text").mkdir(parents=True, exist_ok=True)
        clean_text_path = BASE_DIR / "data" / "processed" / "clean_text" / f"{document_id}.txt"
        clean_text_path.write_text(raw_text, encoding="utf-8")
        meta = doc.model_dump(mode="json")
        meta["chunk_count"] = len(chunks)
        write_json(BASE_DIR / "data" / "processed" / "metadata" / f"{document_id}.json", meta)
        ingested += 1
        print(f"[OK] {file_path.name} -> {document_id} ({len(chunks)} chunks)")
    conn.commit()
    conn.close()
    print(f"Done. ingested={ingested}, skipped={skipped}")

if __name__ == "__main__":
    main()

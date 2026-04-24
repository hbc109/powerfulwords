"""Run LLM theme discovery over recently-ingested chunks.

Output:
  data/processed/themes/proposed_<YYYY-MM-DD>.json

Nothing is auto-promoted into the taxonomy. Review the file and edit
app/config/oil_topic_rules.json + app/config/theme_hierarchy.json by
hand if you want to adopt a proposed theme.
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import json
from datetime import date

from app.db.database import get_connection
from app.discovery.theme_discovery import discover_themes, fetch_recent_chunks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30, help="Look back N days from today")
    parser.add_argument("--limit", type=int, default=80, help="Max chunks to send to the LLM")
    args = parser.parse_args()

    conn = get_connection()
    chunks = fetch_recent_chunks(conn, days=args.days, limit=args.limit)
    conn.close()

    print(f"Loaded {len(chunks)} chunks for discovery (window={args.days}d, limit={args.limit}).")
    if not chunks:
        print("No chunks in window. Ingest something first.")
        return

    result = discover_themes(chunks)

    out_dir = BASE_DIR / "data" / "processed" / "themes"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"proposed_{date.today().isoformat()}.json"
    out_path.write_text(json.dumps(result.model_dump(), ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote: {out_path}")
    print(f"Summary: {result.summary}")
    print(f"New subthemes proposed: {len(result.new_subthemes)}")
    print(f"New themes proposed:    {len(result.new_themes)}")


if __name__ == "__main__":
    main()

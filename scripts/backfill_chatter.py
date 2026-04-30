"""One-shot historical backfill of HN + Bluesky chatter for a date range.

Reuses the queries already configured in `app/config/fetcher_config.json`
for the `bluesky` and `hackernews` fetcher types, but runs them with a
custom [start, end] window and pagination enabled. Writes to the same
inbox folders as the regular fetcher.

Usage:
  python scripts/backfill_chatter.py --start 2026-01-01 --end 2026-02-28
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

from app.fetchers.base import FetchedDocument, filename_for

CONFIG_PATH = BASE_DIR / "app" / "config" / "fetcher_config.json"
INBOX = BASE_DIR / "data" / "inbox"


def write_to_inbox(doc: FetchedDocument) -> bool:
    folder = INBOX / doc.source_bucket / doc.source_id
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / filename_for(doc)
    if target.exists():
        return False
    target.write_text(doc.text, encoding="utf-8")
    return True


def run_fetcher(spec: dict, start: date, end: date) -> list[FetchedDocument]:
    typ = spec["type"]
    params = dict(spec.get("params", {}))
    params["since"] = start
    params["until"] = end
    if typ == "hackernews":
        from app.fetchers.hackernews import fetch_query
        params.setdefault("limit", 500)
        return fetch_query(**params)
    if typ == "bluesky":
        from app.fetchers.bluesky import fetch_query
        return fetch_query(**params)
    raise ValueError(f"Backfill only supports hackernews/bluesky; got {typ}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD inclusive")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    INBOX.mkdir(parents=True, exist_ok=True)
    summary = []
    for spec in cfg.get("fetchers", []):
        if not spec.get("enabled", True):
            continue
        if spec.get("type") not in ("hackernews", "bluesky"):
            continue
        name = spec.get("name") or spec["type"]
        try:
            docs = run_fetcher(spec, start=start, end=end)
        except Exception as e:
            print(f"[ERROR] {name}: {e!r}")
            summary.append((name, "error", 0, 0))
            continue
        written = sum(1 for d in docs if write_to_inbox(d))
        skipped = len(docs) - written
        print(f"[OK] {name}: fetched {len(docs)}, wrote {written}, skipped {skipped}")
        summary.append((name, "ok", written, skipped))

    print()
    print(f"Backfill window: {start} .. {end}")
    total_written = sum(s[2] for s in summary)
    print(f"Total written to inbox: {total_written}")
    print()
    print("Next: python scripts/init_sources.py && "
          "python scripts/ingest_folder.py && "
          "python scripts/extract_narratives.py && "
          "python scripts/score_narratives.py && "
          "python scripts/run_event_study_weekly.py")


if __name__ == "__main__":
    main()

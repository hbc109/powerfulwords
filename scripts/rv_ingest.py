"""Ingest broker price sheets into the RV module (rv_quotes).

Scans the sellside manual-upload inbox, dispatches each file to its source
parser by filename pattern, and upserts the parsed differentials. Idempotent
(re-running re-parses and replaces by primary key). Run daily after dropping
the day's sheets in the inbox.

  python scripts/rv_ingest.py            # ingest all recognised sheets
  python scripts/rv_ingest.py --since 2026-06-16
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.database import get_connection
from app.rv import db as rvdb
from app.rv.parsers import sc as sc_parser
from app.rv.parsers import pvm as pvm_parser
from app.rv.parsers import mitsui as mitsui_parser

INBOX = BASE_DIR / "data" / "inbox" / "sellside_private" / "sellside_manual_upload"

# filename substring -> (source label, parser module).
DISPATCH = [
    ("sc_price_indication", "SC", sc_parser),
    ("pvmpricesheet", "PVM", pvm_parser),
    ("mitsui_asia_prices", "MITSUI", mitsui_parser),
]


def _obs_date_from_name(fname: str) -> str | None:
    stem = fname[:10]
    return stem if len(stem) == 10 and stem[4] == "-" and stem[7] == "-" else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None, help="only files with obs_date >= this")
    args = ap.parse_args()

    if not INBOX.exists():
        print(f"Inbox not found: {INBOX}")
        return

    conn = get_connection()
    rvdb.ensure_schema(conn)
    total = 0
    for f in sorted(INBOX.iterdir()):
        if not f.suffix.lower() in (".xlsx", ".xls", ".xlsm"):
            continue
        obs_date = _obs_date_from_name(f.name)
        if obs_date is None:
            continue
        if args.since and obs_date < args.since:
            continue
        for needle, source, parser in DISPATCH:
            if needle in f.name.lower():
                try:
                    rows = parser.parse(str(f), obs_date)
                except Exception as e:
                    print(f"  [ERR] {f.name}: {type(e).__name__}: {e}")
                    break
                n = rvdb.upsert_quotes(conn, rows)
                total += n
                print(f"  {source:<6} {obs_date}  {f.name[:40]:<42} -> {n} quotes")
                break
    conn.close()
    print(f"\nTotal RV quotes upserted: {total}")


if __name__ == "__main__":
    main()

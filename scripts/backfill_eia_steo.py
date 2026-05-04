"""Backfill EIA Short-Term Energy Outlook (STEO) PDFs.

The EIA publishes STEOs monthly (around the 6th-12th). PDFs live at a
predictable URL: https://www.eia.gov/outlooks/steo/archives/{mmm}{yy}.pdf
e.g. jan24.pdf, jun25.pdf. STEO is fully free.

Drops PDFs into data/inbox/official_reports/eia_steo/ with the
publication-date prefix the ingest folder expects. Uses the 15th of
the month as the canonical date (close to actual release dates and
unique per month).

Usage:
  python scripts/backfill_eia_steo.py --start 2024-01 --end 2025-12
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import requests
from datetime import date

from app.fetchers.base import USER_AGENT

INBOX = BASE_DIR / "data" / "inbox" / "official_reports" / "eia_steo"
URL_TEMPLATE = "https://www.eia.gov/outlooks/steo/archives/{mmm}{yy}.pdf"
MONTHS = ["jan", "feb", "mar", "apr", "may", "jun",
          "jul", "aug", "sep", "oct", "nov", "dec"]


def parse_yyyymm(s: str) -> tuple[int, int]:
    y, m = s.split("-")
    return int(y), int(m)


def iter_months(start_y, start_m, end_y, end_m):
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        yield y, m
        m += 1
        if m > 12:
            y += 1; m = 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM (inclusive)")
    parser.add_argument("--end", required=True, help="YYYY-MM (inclusive)")
    args = parser.parse_args()

    start_y, start_m = parse_yyyymm(args.start)
    end_y, end_m = parse_yyyymm(args.end)
    INBOX.mkdir(parents=True, exist_ok=True)

    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})

    written = skipped = errors = 0
    for y, m in iter_months(start_y, start_m, end_y, end_m):
        mmm = MONTHS[m - 1]
        yy = f"{y % 100:02d}"
        url = URL_TEMPLATE.format(mmm=mmm, yy=yy)
        pub_date = date(y, m, 15).isoformat()
        target = INBOX / f"{pub_date}_eia_steo_{mmm}{yy}.pdf"
        if target.exists():
            print(f"[SKIP] {target.name} (already on disk)")
            skipped += 1
            continue
        try:
            r = sess.get(url, timeout=30)
        except Exception as e:
            print(f"[ERR ] {y}-{m:02d}: {e!r}")
            errors += 1
            continue
        if r.status_code != 200:
            print(f"[MISS] {y}-{m:02d}: HTTP {r.status_code}  {url}")
            errors += 1
            continue
        target.write_bytes(r.content)
        print(f"[OK  ] {y}-{m:02d}: wrote {target.name} ({len(r.content):,} bytes)")
        written += 1

    print(f"\nDone. wrote={written}, skipped={skipped}, missing/err={errors}")
    print("Next: python scripts/init_sources.py && "
          "python scripts/ingest_folder.py && "
          "python scripts/extract_narratives.py && "
          "python scripts/score_narratives.py")


if __name__ == "__main__":
    main()

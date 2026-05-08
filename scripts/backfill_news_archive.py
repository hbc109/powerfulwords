"""Backfill historical news via Google News date-filtered site: queries.

Standard RSS feeds only carry the latest 20-100 items, so for any
historical news coverage we have to go through Google's index.

For each (domain × quarter) we build a query like:
    site:cnbc.com (oil OR crude OR OPEC OR brent OR WTI)
    before:2024-04-01 after:2024-01-01
and hit Google News RSS, capturing up to 100 results per quarter.
That's 12 quarters × 14 domains = 168 queries for 2023-01 → 2025-12.

Each result becomes a FetchedDocument (title + summary, follow_links
optional) and lands in `data/inbox/authoritative_news/<source_id>/`.
The standard ingest -> extract -> score path picks them up.

Usage:
  python scripts/backfill_news_archive.py --start 2023-01 --end 2025-12
  python scripts/backfill_news_archive.py --start 2025-01 --end 2025-06 --domains cnbc.com,reuters.com
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import time
from datetime import date
from urllib.parse import quote_plus

from app.fetchers.base import FetchedDocument, filename_for
from app.fetchers.rss_feed import fetch_rss


# (domain, source_id-to-store-as)
DOMAINS = [
    ("oilprice.com",      "oilprice_news"),
    ("rbnenergy.com",     "rbn_energy"),
    ("cnbc.com",          "cnbc_top_news"),
    ("reuters.com",       "google_news"),
    ("bloomberg.com",     "google_news"),
    ("ft.com",            "google_news"),
    ("worldoil.com",      "world_oil"),
    ("energyvoice.com",   "energy_voice"),
    ("zerohedge.com",     "zerohedge_energy"),
    ("aljazeera.com",     "aljazeera_all"),
    ("xinhuanet.com",     "xinhua_world"),
    ("tass.com",          "tass_economy"),
    ("presstv.ir",        "presstv_economy"),
    ("scmp.com",          "scmp_china"),
    ("asia.nikkei.com",   "nikkei_asia_business"),
]

KEYWORDS = "oil OR crude OR OPEC OR brent OR WTI OR gasoline"

INBOX = BASE_DIR / "data" / "inbox"


def parse_yyyymm(s: str) -> tuple[int, int]:
    y, m = s.split("-")
    return int(y), int(m)


def quarter_ranges(start_y: int, start_m: int, end_y: int, end_m: int):
    """Yield (after_iso, before_iso) date strings for every quarter
    intersecting [start, end]."""
    qm = ((start_m - 1) // 3) * 3 + 1
    y, m = start_y, qm
    while (y, m) <= (end_y, end_m):
        after = date(y, m, 1).isoformat()
        ny, nm = (y, m + 3) if m + 3 <= 12 else (y + 1, m + 3 - 12)
        before = date(ny, nm, 1).isoformat()
        yield after, before
        y, m = ny, nm


def write_to_inbox(doc: FetchedDocument) -> bool:
    folder = INBOX / doc.source_bucket / doc.source_id
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / filename_for(doc)
    if target.exists():
        return False
    target.write_text(doc.text, encoding="utf-8")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM inclusive")
    parser.add_argument("--end", required=True, help="YYYY-MM inclusive")
    parser.add_argument("--domains", help="Comma-separated subset of domains; default = all 14")
    parser.add_argument("--keywords", default=KEYWORDS)
    parser.add_argument("--throttle", type=float, default=2.0,
                        help="Seconds to wait between queries (Google rate limit guard)")
    parser.add_argument("--limit-per-query", type=int, default=100)
    parser.add_argument("--follow-links", action="store_true",
                        help="Fetch full article body (slower, more disk, higher block-rate)")
    args = parser.parse_args()

    start_y, start_m = parse_yyyymm(args.start)
    end_y, end_m = parse_yyyymm(args.end)

    selected_domains = (
        [(d.strip(), s) for d, s in DOMAINS if d.strip() in args.domains.split(",")]
        if args.domains else DOMAINS
    )

    quarters = list(quarter_ranges(start_y, start_m, end_y, end_m))
    print(f"Domains: {len(selected_domains)}  Quarters: {len(quarters)}  "
          f"Keywords: {args.keywords!r}")
    print()

    total_written = 0
    total_fetched = 0
    for domain, source_id in selected_domains:
        for after, before in quarters:
            query = (f"site:{domain} ({args.keywords}) "
                     f"before:{before} after:{after}")
            url = (f"https://news.google.com/rss/search?"
                   f"q={quote_plus(query)}&hl=en-US&gl=US")
            try:
                docs = fetch_rss(
                    feed_url=url,
                    source_id=source_id,
                    source_bucket="authoritative_news",
                    limit=args.limit_per_query,
                    follow_links=args.follow_links,
                    min_chars=80,
                )
            except Exception as e:
                print(f"  [ERR ] {domain:<22} {after}..{before[:7]}  {type(e).__name__}: {str(e)[:80]}")
                time.sleep(args.throttle)
                continue
            written = sum(1 for d in docs if write_to_inbox(d))
            total_written += written
            total_fetched += len(docs)
            print(f"  [OK  ] {domain:<22} {after}..{before[:7]}  fetched={len(docs):>3}  wrote={written:>3}")
            time.sleep(args.throttle)

    print()
    print(f"Total fetched: {total_fetched}")
    print(f"Total written to inbox: {total_written}")
    print()
    print("Next: python scripts/init_sources.py && "
          "python scripts/ingest_folder.py && "
          "python scripts/extract_narratives.py && "
          "python scripts/score_narratives.py && "
          "python scripts/test_strategy_hypotheses.py")


if __name__ == "__main__":
    main()

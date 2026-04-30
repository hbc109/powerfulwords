"""Run all enabled fetchers and write results into the inbox.

Each fetcher returns FetchedDocument objects; this script writes each
document into:

  data/inbox/<source_bucket>/<source_id>/<YYYY-MM-DD>_<slug>.txt

Files already on disk (same name) are skipped — re-runs are safe.
After fetching, run `python scripts/ingest_folder.py` to chunk and
store them, then `extract_narratives.py` and `score_narratives.py`
as usual.

Usage:
  python scripts/fetch_sources.py                  # all enabled
  python scripts/fetch_sources.py --only reddit_oil  # one fetcher
  python scripts/fetch_sources.py --days 7         # override lookback
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import json
from datetime import date, timedelta

from app.fetchers.base import FetchedDocument, filename_for

CONFIG_PATH = BASE_DIR / "app" / "config" / "fetcher_config.json"
INBOX = BASE_DIR / "data" / "inbox"


def load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def write_to_inbox(doc: FetchedDocument) -> bool:
    """Return True if a file was written, False if it already existed."""
    folder = INBOX / doc.source_bucket / doc.source_id
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / filename_for(doc)
    if target.exists():
        return False
    target.write_text(doc.text, encoding="utf-8")
    return True


def run_fetcher(spec: dict, since: date) -> list[FetchedDocument]:
    typ = spec["type"]
    params = dict(spec.get("params", {}))
    params.setdefault("since", since)

    if typ == "reddit":
        from app.fetchers.reddit import fetch_subreddit
        return fetch_subreddit(**params)
    if typ == "rss":
        from app.fetchers.rss_feed import fetch_rss
        return fetch_rss(**params)
    if typ == "opec_press":
        from app.fetchers.opec_press import fetch_press_releases
        return fetch_press_releases(**params)
    if typ == "iea_news":
        from app.fetchers.iea_news import fetch_iea_news
        return fetch_iea_news(**params)
    if typ == "agency_html":
        from app.fetchers.agency_html import fetch_agency
        return fetch_agency(**params)
    if typ == "stocktwits":
        from app.fetchers.stocktwits import fetch_symbol
        return fetch_symbol(**params)
    if typ == "bluesky":
        from app.fetchers.bluesky import fetch_query
        return fetch_query(**params)
    if typ == "hackernews":
        from app.fetchers.hackernews import fetch_query
        return fetch_query(**params)
    raise ValueError(f"Unknown fetcher type: {typ}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", help="Run only the named fetcher (matches the `name` field)")
    parser.add_argument("--days", type=int, help="Lookback in days; overrides config")
    args = parser.parse_args()

    cfg = load_config()
    lookback = args.days or int(cfg.get("default_lookback_days", 14))
    since = date.today() - timedelta(days=lookback)

    INBOX.mkdir(parents=True, exist_ok=True)

    summary = []
    for spec in cfg.get("fetchers", []):
        if not spec.get("enabled", True):
            continue
        if args.only and spec.get("name") != args.only:
            continue
        name = spec.get("name") or spec.get("type")
        try:
            docs = run_fetcher(spec, since=since)
        except PermissionError as e:
            print(f"[BLOCKED] {name}: {e}")
            summary.append((name, "blocked", 0, 0))
            continue
        except Exception as e:
            print(f"[ERROR]   {name}: {e!r}")
            summary.append((name, "error", 0, 0))
            continue
        written = sum(1 for d in docs if write_to_inbox(d))
        skipped = len(docs) - written
        print(f"[OK]      {name}: fetched {len(docs)}, wrote {written}, skipped {skipped}")
        summary.append((name, "ok", written, skipped))

    print()
    print("Summary:")
    for name, status, written, skipped in summary:
        print(f"  {name:>30}  {status:>8}  wrote={written}  skipped={skipped}")
    print()
    print("Next step:")
    print("  python scripts/ingest_folder.py")


if __name__ == "__main__":
    main()

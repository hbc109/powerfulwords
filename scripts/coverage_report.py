"""Calendar coverage report — find days with thin or missing documentation.

Tells you which dates in the past N days have few or no narrative
documents, so manual uploads can be targeted at the worst gaps
(highest signal-improvement per upload).

Three categories:
  EMPTY  — date has zero documents
  THIN   — date has < THIN_THRESHOLD docs (default 5)
  OK     — date has >= THIN_THRESHOLD docs

Run:
    python scripts/coverage_report.py             # last 90 days
    python scripts/coverage_report.py --days 365  # last year
    python scripts/coverage_report.py --weekdays  # weekdays only (skip Sat/Sun)
"""

from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "oil_narrative.db"
THIN_THRESHOLD = 5


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90, help="Lookback window (default 90).")
    ap.add_argument("--thin", type=int, default=THIN_THRESHOLD,
                    help=f"Threshold below which a day counts as THIN (default {THIN_THRESHOLD}).")
    ap.add_argument("--weekdays", action="store_true", help="Skip Saturdays and Sundays.")
    ap.add_argument("--show", type=int, default=30, help="How many worst days to print.")
    args = ap.parse_args()

    conn = sqlite3.connect(DB_PATH)
    today = date.today()
    start = today - timedelta(days=args.days - 1)

    # Doc counts by published_at date
    rows = conn.execute(
        "SELECT date(published_at), COUNT(*) "
        "FROM documents WHERE published_at IS NOT NULL "
        "AND date(published_at) BETWEEN ? AND ? "
        "GROUP BY date(published_at)",
        (start.isoformat(), today.isoformat()),
    ).fetchall()
    by_day = {r[0]: r[1] for r in rows}

    # Enumerate all dates in window
    all_dates = []
    d = start
    while d <= today:
        if not (args.weekdays and d.weekday() >= 5):
            all_dates.append(d)
        d += timedelta(days=1)

    empty = []
    thin = []
    ok = []
    for d in all_dates:
        n = by_day.get(d.isoformat(), 0)
        if n == 0:
            empty.append((d, 0))
        elif n < args.thin:
            thin.append((d, n))
        else:
            ok.append((d, n))

    total = len(all_dates)
    print(f"Coverage report — {start} → {today}  ({total} {'weekday' if args.weekdays else 'calendar'} days)")
    print(f"  EMPTY:  {len(empty):>4}  ({100*len(empty)/total:.0f}%)  — no documents at all")
    print(f"  THIN:   {len(thin):>4}  ({100*len(thin)/total:.0f}%)  — < {args.thin} docs")
    print(f"  OK:     {len(ok):>4}  ({100*len(ok)/total:.0f}%)  — >= {args.thin} docs")
    print()

    if empty:
        print(f"Worst {min(args.show, len(empty))} EMPTY days (most recent first):")
        empty.sort(reverse=True)
        for d, _ in empty[: args.show]:
            print(f"  {d}  ({d.strftime('%a')})  — 0 docs")
        print()

    if thin:
        print(f"Worst {min(args.show, len(thin))} THIN days (most recent first):")
        thin.sort(reverse=True)
        for d, n in thin[: args.show]:
            bucket_rows = conn.execute(
                "SELECT source_bucket, COUNT(*) FROM documents "
                "WHERE date(published_at)=? GROUP BY source_bucket",
                (d.isoformat(),),
            ).fetchall()
            buckets = ', '.join(f"{b}:{c}" for b, c in bucket_rows) or '(none)'
            print(f"  {d}  ({d.strftime('%a')})  — {n} doc(s)  buckets: {buckets}")

    conn.close()


if __name__ == "__main__":
    main()

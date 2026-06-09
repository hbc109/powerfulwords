"""One-shot backfill: set `released_at` for every market_prices row
where it's NULL, using the per-symbol lag policy in
app/scoring/release_lags.py.

Idempotent — re-running is a no-op once every row is populated. Safe to
run mid-cron because we only touch NULL rows; in-flight writes from
upsert_market_prices already set the column.

Usage:
    python3 scripts/backfill_release_dates.py [--dry-run] [--verbose]
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.database import get_connection, init_db
from app.scoring.release_lags import released_at_for, lag_days_for


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would change but don't write")
    ap.add_argument("--verbose", action="store_true",
                    help="Print per-row decisions for the first 20 rows of each symbol")
    args = ap.parse_args()

    init_db()
    conn = get_connection()

    null_rows = conn.execute(
        "SELECT price_time, symbol FROM market_prices "
        "WHERE released_at IS NULL ORDER BY symbol, price_time"
    ).fetchall()
    if not null_rows:
        print("Nothing to backfill — every market_prices row already has released_at.")
        conn.close()
        return

    print(f"Found {len(null_rows):,} rows with NULL released_at across "
          f"{len({s for _, s in null_rows})} distinct symbols.")

    per_symbol = Counter()
    per_symbol_lag: dict[str, int] = {}
    sample_printed: dict[str, int] = {}
    updates: list[tuple[str, str, str]] = []
    for price_time, symbol in null_rows:
        rel = released_at_for(symbol, str(price_time))
        per_symbol[symbol] += 1
        per_symbol_lag[symbol] = lag_days_for(symbol)
        updates.append((rel, price_time, symbol))
        if args.verbose and sample_printed.get(symbol, 0) < 3:
            sample_printed[symbol] = sample_printed.get(symbol, 0) + 1
            print(f"  {symbol:32s} price_time={price_time}  ->  released_at={rel}")

    print("\n--- summary by symbol (top 25) ---")
    print(f"{'symbol':40s} {'rows':>8s} {'lag_d':>6s}")
    for sym, n in per_symbol.most_common(25):
        print(f"{sym:40s} {n:>8d} {per_symbol_lag[sym]:>6d}")

    if args.dry_run:
        print("\n--dry-run set — no DB writes. Re-run without --dry-run to apply.")
        conn.close()
        return

    print(f"\nWriting {len(updates):,} updates...")
    conn.executemany(
        "UPDATE market_prices SET released_at = ? "
        "WHERE price_time = ? AND symbol = ?",
        updates,
    )
    conn.commit()
    remaining = conn.execute(
        "SELECT COUNT(*) FROM market_prices WHERE released_at IS NULL"
    ).fetchone()[0]
    print(f"Done. Remaining NULL released_at rows: {remaining}")
    conn.close()


if __name__ == "__main__":
    main()

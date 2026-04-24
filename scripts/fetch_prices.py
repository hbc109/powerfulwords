"""Auto-fetch front-month futures prices from Yahoo Finance and upsert
into the market_prices table.

Wraps app.fetchers.yfinance_prices.fetch_prices and writes to SQLite
in the same shape as load_prices_csv.py.
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse

from app.db.database import get_connection, init_db
from app.fetchers.yfinance_prices import fetch_prices


def upsert_prices(conn, rows: list[dict]) -> int:
    n = 0
    for r in rows:
        conn.execute(
            '''
            INSERT OR REPLACE INTO market_prices (
                price_time, symbol, asset_type, open, high, low, close, volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                r["price_time"], r["symbol"], r["asset_type"],
                r["open"], r["high"], r["low"], r["close"], r["volume"],
            ),
        )
        n += 1
    conn.commit()
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", default="3mo", help="yfinance period (e.g. 1mo, 3mo, 6mo, 1y)")
    parser.add_argument("--interval", default="1d")
    args = parser.parse_args()

    init_db()
    rows = fetch_prices(period=args.period, interval=args.interval)
    if not rows:
        print("No price rows returned. Yahoo may be rate-limiting; try again in a few minutes.")
        return
    conn = get_connection()
    n = upsert_prices(conn, rows)
    conn.close()

    by_sym = {}
    for r in rows:
        by_sym[r["symbol"]] = by_sym.get(r["symbol"], 0) + 1
    print(f"Upserted {n} price rows.")
    for sym, count in sorted(by_sym.items()):
        print(f"  {sym:>10}  {count} rows")


if __name__ == "__main__":
    main()

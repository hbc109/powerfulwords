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
from app.fetchers.term_structure import fetch_term_structure
from app.fetchers.cot_positioning import fetch_cot_positioning
from app.fetchers.eia_inventory import fetch_eia_inventory
from app.fetchers.eia_futures_settlement import fetch_eia_futures_settlement
from app.fetchers.jodi_inventory import fetch_jodi_inventory
from app.fetchers.broker_settle import fetch_broker_settle


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
    rows += fetch_term_structure(period=args.period, interval=args.interval)
    rows += fetch_cot_positioning()
    rows += fetch_eia_inventory()
    rows += fetch_eia_futures_settlement()
    rows += fetch_jodi_inventory()
    if not rows:
        print("No price rows returned. Yahoo may be rate-limiting; try again in a few minutes.")
        return
    conn = get_connection()
    n = upsert_prices(conn, rows)
    # Broker-settle parser reads from the documents table, so it must run
    # after the connection is open. Adds WTI_BROKER_SETTLE / BRENT_BROKER_SETTLE
    # rows for any trade-date cited in a 港联 or Macquarie morning brief.
    broker_rows = fetch_broker_settle(conn)
    if broker_rows:
        n += upsert_prices(conn, broker_rows)
        rows += broker_rows
    conn.close()

    by_sym = {}
    for r in rows:
        by_sym[r["symbol"]] = by_sym.get(r["symbol"], 0) + 1
    print(f"Upserted {n} price rows.")
    for sym, count in sorted(by_sym.items()):
        print(f"  {sym:>10}  {count} rows")


if __name__ == "__main__":
    main()

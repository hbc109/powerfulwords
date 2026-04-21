from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import csv
from pathlib import Path

from app.db.database import get_connection

BASE_DIR = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="CSV file with columns price_time,symbol,asset_type,open,high,low,close,volume")
    args = parser.parse_args()

    csv_path = (BASE_DIR / args.csv).resolve() if not Path(args.csv).is_absolute() else Path(args.csv)

    conn = get_connection()
    count = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            conn.execute(
                '''
                INSERT OR REPLACE INTO market_prices (
                    price_time, symbol, asset_type, open, high, low, close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    row["price_time"],
                    row["symbol"],
                    row.get("asset_type", "commodity"),
                    float(row["open"]) if row.get("open") else None,
                    float(row["high"]) if row.get("high") else None,
                    float(row["low"]) if row.get("low") else None,
                    float(row["close"]) if row.get("close") else None,
                    float(row["volume"]) if row.get("volume") else None,
                ),
            )
            count += 1

    conn.commit()
    conn.close()
    print(f"Loaded {count} price rows from {csv_path}")


if __name__ == "__main__":
    main()

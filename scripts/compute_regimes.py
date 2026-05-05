"""Compute regime tags for every symbol in market_prices and upsert
into daily_regimes.

Run after `fetch_prices.py` so the latest price feeds in.
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import pandas as pd

from app.db.database import get_connection, init_db
from app.research.regime import compute_regimes


REGIME_COLS = [
    "close", "rsi14", "adx14", "atr14", "atr_ratio",
    "bb_pctb", "sma50", "sma50_slope_5d_pct",
    "regime_tags", "primary_regime",
]


def fetch_symbols(conn) -> list[str]:
    cur = conn.execute("SELECT DISTINCT symbol FROM market_prices ORDER BY symbol")
    return [r[0] for r in cur.fetchall()]


def fetch_prices_df(conn, symbol: str) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT price_time, symbol, open, high, low, close, volume "
        "FROM market_prices WHERE symbol = ? ORDER BY price_time",
        conn, params=(symbol,),
    )
    df["date"] = df["price_time"].astype(str).str[:10]
    return df


def upsert(conn, symbol: str, regimes: pd.DataFrame) -> int:
    n = 0
    for _, r in regimes.iterrows():
        if pd.isna(r["close"]):
            continue
        conn.execute(
            f'''INSERT OR REPLACE INTO daily_regimes
                (regime_date, symbol, {", ".join(REGIME_COLS)})
                VALUES (?, ?, {", ".join(["?"] * len(REGIME_COLS))})''',
            (r["date"], symbol, *[r[c] if not pd.isna(r[c]) else None for c in REGIME_COLS]),
        )
        n += 1
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="*",
                        help="Subset of symbols to compute (default = all in market_prices)")
    args = parser.parse_args()

    init_db()
    conn = get_connection()

    symbols = args.symbols or fetch_symbols(conn)
    total = 0
    for sym in symbols:
        prices = fetch_prices_df(conn, sym)
        if prices.empty:
            print(f"[skip] {sym}: no prices")
            continue
        regimes = compute_regimes(prices)
        n = upsert(conn, sym, regimes)
        latest = regimes.iloc[-1]
        print(f"[OK]   {sym}: wrote {n} rows; latest {latest['date']} -> "
              f"{latest['primary_regime']} (tags={latest['regime_tags'] or '-'}, "
              f"RSI={latest['rsi14']:.1f}, ADX={latest['adx14']:.1f}, "
              f"%B={latest['bb_pctb']:.2f}, ATRr={latest['atr_ratio']:.2f})")
        total += n
    conn.commit()
    conn.close()
    print(f"\nTotal upserts: {total}")


if __name__ == "__main__":
    main()

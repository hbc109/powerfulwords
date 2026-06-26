"""One-off: copy the local SQLite DB (data/oil_narrative.db) into hosted
Postgres (Neon).

Run locally once, with DATABASE_URL pointing at the Neon *direct* endpoint:

    DATABASE_URL='postgresql://USER:PASS@HOST/db?sslmode=require' \
        python scripts/migrate_sqlite_to_pg.py

It (1) creates the core schema via the app's own init_db(), (2) reflects any
remaining (lazily-created) tables from SQLite, then (3) bulk-copies every table
in FK order, batched, with ON CONFLICT DO NOTHING so it is safe to re-run. Serial
sequences for paper_trades / ai_reviews are reset at the end.

The 363 MB chunks/documents text is the slow part; expect a few minutes.
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db import database, pg_adapter

# Insert parents before children so FK constraints hold; any other tables that
# exist in the source but are not listed here are appended afterwards.
ORDER = [
    "sources", "documents", "chunks", "narrative_events",
    "market_prices", "daily_regimes", "daily_narrative_scores",
    "daily_theme_scores", "rv_spreads", "rv_quotes", "paper_trades",
    "ai_reviews", "extracted_chunks", "llm_direction_adjudicated",
]
SERIAL_PK = {"paper_trades": "trade_id", "ai_reviews": "review_id"}
BATCH = 500


def ensure_table(src, dst, t):
    """Create a table on Postgres from its SQLite DDL (adapter translates
    AUTOINCREMENT etc.). init_db() already made the core tables; this fills in
    the lazily-created ones (rv_*, paper_trades, ai_reviews, extracted_chunks...)."""
    row = src.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (t,)
    ).fetchone()
    ddl = row[0]
    if "IF NOT EXISTS" not in ddl.upper():
        ddl = re.sub(r"CREATE\s+TABLE\s+", "CREATE TABLE IF NOT EXISTS ", ddl,
                     count=1, flags=re.I)
    dst.execute(ddl)


def copy_table(src, dst, t):
    info = src.execute(f"PRAGMA table_info({t})").fetchall()
    cols = [c[1] for c in info]
    pk = [c[1] for c in info if c[5]]
    conflict = f" ON CONFLICT ({', '.join(pk)}) DO NOTHING" if pk else ""
    sql = (f"INSERT INTO {t} ({', '.join(cols)}) "
           f"VALUES ({', '.join('?' for _ in cols)}){conflict}")
    cur = src.execute(f"SELECT {', '.join(cols)} FROM {t}")
    total = 0
    while True:
        batch = cur.fetchmany(BATCH)
        if not batch:
            break
        dst.executemany(sql, [tuple(r) for r in batch])
        dst.commit()
        total += len(batch)
        print(f"    {t}: {total} rows...", end="\r")
    return total


def main():
    url = os.getenv("DATABASE_URL")
    if not url:
        sys.exit("Set DATABASE_URL to the Neon connection string first.")

    print(f"Source : {database.DB_PATH}")
    print("Creating core schema via init_db()...")
    database.init_db()  # uses get_connection() -> adapter because DATABASE_URL is set

    src = sqlite3.connect(database.DB_PATH)
    dst = pg_adapter.connect(url)

    existing = [r[0] for r in src.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()]
    tables = [t for t in ORDER if t in existing] + [t for t in existing if t not in ORDER]

    for t in tables:
        ensure_table(src, dst, t)
        n = copy_table(src, dst, t)
        print(f"  {t}: {n} rows".ljust(60))

    cur = dst.cursor()
    for t, pk in SERIAL_PK.items():
        if t in existing:
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{t}', '{pk}'), "
                f"COALESCE((SELECT MAX({pk}) FROM {t}), 1))")
    dst.commit()
    print("Done. Serial sequences reset.")


if __name__ == "__main__":
    main()

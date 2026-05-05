import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "oil_narrative.db"
INIT_SQL_PATH = BASE_DIR / "sql" / "init.sql"

def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    with open(INIT_SQL_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    # Lightweight migration for additive score columns. SQLite ALTER TABLE
    # only supports ADD COLUMN, and there is no IF NOT EXISTS on column adds,
    # so probe via PRAGMA and add what's missing.
    additive_columns = {
        "daily_narrative_scores": [
            ("raw_score", "REAL"),
            ("event_count", "INTEGER"),
            ("breadth", "REAL"),
            ("persistence", "REAL"),
            ("source_divergence", "REAL"),
            ("theme", "TEXT"),
        ],
        "narrative_events": [
            ("theme", "TEXT"),
        ],
        "daily_regimes": [
            ("regime_streak", "INTEGER"),
        ],
    }
    for table, cols in additive_columns.items():
        cur = conn.execute(f"PRAGMA table_info({table})")
        existing_cols = {row[1] for row in cur.fetchall()}
        for col, decl in cols:
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    print(f"Initialized database at: {DB_PATH}")

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
    cur = conn.execute("PRAGMA table_info(daily_narrative_scores)")
    existing_cols = {row[1] for row in cur.fetchall()}
    for col, decl in [
        ("raw_score", "REAL"),
        ("event_count", "INTEGER"),
        ("breadth", "REAL"),
        ("persistence", "REAL"),
        ("source_divergence", "REAL"),
    ]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE daily_narrative_scores ADD COLUMN {col} {decl}")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
    print(f"Initialized database at: {DB_PATH}")

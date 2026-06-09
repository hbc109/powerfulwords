import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "oil_narrative.db"
INIT_SQL_PATH = BASE_DIR / "sql" / "init.sql"

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    # busy_timeout > Python-level timeout is belt-and-suspenders: the dashboard
    # upload pipeline holds a write lock for minutes while extract_narratives
    # batches rows, and the hourly cron's init_sources.py used to fail
    # immediately on "database is locked" with the SQLite default 0-timeout.
    # 60s window lets the loser wait for the winner's commit instead of dying.
    conn.execute("PRAGMA busy_timeout=60000;")
    return conn

def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    # WAL mode lets dashboard reads run concurrently with cron writes —
    # otherwise heavy ingest grabs an exclusive lock and blocks queries.
    # Idempotent; safe to run every init.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")  # fewer fsyncs, still durable on WAL
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
            ("macd_line", "REAL"),
            ("macd_hist", "REAL"),
            ("volume_ratio", "REAL"),
            ("cross_product_agreement", "REAL"),
        ],
        # `released_at` is when this row's data became publicly known,
        # distinct from `price_time` which is the period the data
        # describes (e.g. EIA week-ending Fri vs Wed publication, COT
        # "as of Tue" vs Fri publication). All backtest / factor queries
        # that want "available as of asof" filter on `released_at`.
        # See app/scoring/release_lags.py for the per-symbol lag policy.
        "market_prices": [
            ("released_at", "TIMESTAMP"),
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

def upsert_market_prices(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Single write path for market_prices. Auto-computes released_at via
    the per-symbol lag policy so every backtest can rely on it to mean
    "when this row became known". Caller still owns `conn.commit()`.

    Each row must have: price_time, symbol, asset_type, open, high, low,
    close, volume. `released_at` is derived; passing it is ignored."""
    from app.scoring.release_lags import released_at_for
    n = 0
    for r in rows:
        released_at = released_at_for(r["symbol"], str(r["price_time"]))
        conn.execute(
            """
            INSERT OR REPLACE INTO market_prices (
                price_time, symbol, asset_type, open, high, low, close, volume,
                released_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                r["price_time"], r["symbol"], r["asset_type"],
                r.get("open"), r.get("high"), r.get("low"),
                r.get("close"), r.get("volume"),
                released_at,
            ),
        )
        n += 1
    return n


if __name__ == "__main__":
    init_db()
    print(f"Initialized database at: {DB_PATH}")

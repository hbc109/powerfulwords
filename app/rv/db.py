"""Relative-value module — self-contained storage.

Lives in the same oil_narrative.db file (so RV can read the narrative/price
tables as a loose overlay) but owns its own `rv_*` tables and schema. Nothing
in the flat-price narrative pipeline depends on these tables — RV is an
independent subsystem that could be lifted out into its own db/service later.

Tables
------
rv_quotes   : one row per (obs_date, source, spread, tenor) — the daily broker
              snapshot. `tenor` is a constant-maturity label (M1, M2, …) so a
              spread's series stays comparable across days for z-scoring;
              `contract` keeps the absolute month/strip label (Aug-26, Q3-26).
rv_spreads  : metadata per spread (category, description, what a higher value
              means) — for the evaluator and dashboard.
"""

from __future__ import annotations

from app.db.database import get_connection

RV_SCHEMA = """
CREATE TABLE IF NOT EXISTS rv_quotes (
    obs_date    TEXT NOT NULL,      -- date the sheet is for (YYYY-MM-DD)
    source      TEXT NOT NULL,      -- 'SC' | 'PVM' | 'MITSUI'
    category    TEXT,               -- crude_outright | crude_diff | crude_timespread | crack | ...
    spread      TEXT NOT NULL,      -- canonical name: 'WTI-Brent', 'Brent', 'Brent cal', ...
    tenor       TEXT NOT NULL,      -- constant-maturity: 'M1','M2',… or strip label 'Q3-26'
    contract    TEXT,               -- absolute label: 'Aug-26','Q3-26'
    value       REAL,
    unit        TEXT,               -- '$/bbl' | '$/mt' | 'c/bbl'
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (obs_date, source, spread, tenor)
);
CREATE INDEX IF NOT EXISTS idx_rv_quotes_series ON rv_quotes(source, spread, tenor, obs_date);

CREATE TABLE IF NOT EXISTS rv_spreads (
    spread        TEXT PRIMARY KEY,
    category      TEXT,
    description   TEXT,
    higher_means  TEXT             -- what a higher value implies (for trade-idea text)
);
"""


def ensure_schema(conn) -> None:
    conn.executescript(RV_SCHEMA)
    conn.commit()


def upsert_quotes(conn, rows: list[dict]) -> int:
    ensure_schema(conn)
    n = 0
    for r in rows:
        conn.execute(
            """INSERT OR REPLACE INTO rv_quotes
                 (obs_date, source, category, spread, tenor, contract, value, unit)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (r["obs_date"], r["source"], r.get("category"), r["spread"],
             r["tenor"], r.get("contract"), r["value"], r.get("unit")),
        )
        n += 1
    conn.commit()
    return n


def series(conn, source: str, spread: str, tenor: str) -> list[tuple]:
    """Time series (obs_date, value) for one spread/tenor — for z-scoring."""
    return conn.execute(
        "SELECT obs_date, value FROM rv_quotes WHERE source=? AND spread=? AND tenor=? "
        "ORDER BY obs_date",
        (source, spread, tenor),
    ).fetchall()


def latest_obs_date(conn) -> str | None:
    r = conn.execute("SELECT MAX(obs_date) FROM rv_quotes").fetchone()
    return r[0] if r else None

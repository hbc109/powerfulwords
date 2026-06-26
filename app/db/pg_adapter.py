"""Tiny psycopg3 adapter that mimics the sqlite3 Connection/Cursor API.

The app talks to SQLite through `database.get_connection()` (and a few direct
`sqlite3.connect` calls in the dashboard, rerouted to the factory). When
DATABASE_URL is set (Streamlit Cloud + GitHub Actions), the factory returns one
of these adapters instead, so existing call sites keep working against Postgres
(Neon) without edits. Translation happens at the cursor, the single chokepoint
shared by `conn.execute(...)`, `conn.executescript(...)` and `pandas.read_sql`:

  - ?            -> %s   (and literal %  -> %%, only when params are bound)
  - INSERT OR REPLACE / INSERT OR IGNORE -> INSERT ... ON CONFLICT (keys) ...
  - PRAGMA journal_mode/synchronous/busy_timeout -> no-op
  - PRAGMA table_info(t) -> information_schema query (name in column [1])
  - INTEGER PRIMARY KEY AUTOINCREMENT -> SERIAL PRIMARY KEY  (DDL)
  - lastrowid  -> synthesized via RETURNING for plain serial-PK inserts

Local dev is untouched: no DATABASE_URL -> plain sqlite3, this module unused.
"""
from __future__ import annotations

import re

# Tables whose INSERT OR REPLACE / INSERT OR IGNORE must map onto a conflict key.
CONFLICT_KEYS = {
    "sources": ("source_id",),
    "documents": ("document_id",),
    "chunks": ("chunk_id",),
    "market_prices": ("price_time", "symbol"),
    "rv_quotes": ("obs_date", "source", "spread", "tenor"),
    "narrative_events": ("event_id",),
    "daily_narrative_scores": ("score_date", "commodity", "topic"),
    "daily_theme_scores": ("score_date", "commodity", "theme"),
    "daily_regimes": ("regime_date", "symbol"),
    "llm_direction_adjudicated": ("chunk_id",),
    "ai_reviews": ("review_date",),
    "extracted_chunks": ("chunk_id",),
}

# Tables with an auto-increment PK, so a plain INSERT can report lastrowid.
SERIAL_PK = {
    "paper_trades": "trade_id",
    "ai_reviews": "review_id",
}

_NOOP = object()

_PRAGMA_TABLE_INFO = re.compile(r"^\s*PRAGMA\s+table_info\(\s*['\"]?(\w+)['\"]?\s*\)", re.I)
_PRAGMA_ANY = re.compile(r"^\s*PRAGMA\b", re.I)
_INSERT_REPLACE = re.compile(r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]*)\)", re.I | re.S)
_INSERT_IGNORE = re.compile(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+(\w+)", re.I)
_INSERT_INTO = re.compile(r"^\s*INSERT\s+INTO\s+(\w+)", re.I)
_AUTOINC = re.compile(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", re.I)


def _conflict_clause(table, cols):
    keys = CONFLICT_KEYS.get(table)
    if not keys:
        return ""  # unknown table: leave as plain INSERT
    updates = [c for c in cols if c not in keys]
    if updates:
        sets = ", ".join(f"{c}=EXCLUDED.{c}" for c in updates)
        return f" ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {sets}"
    return f" ON CONFLICT ({', '.join(keys)}) DO NOTHING"


def _translate_sql(sql):
    """SQLite SQL -> Postgres SQL. Returns _NOOP for statements to skip.
    Does NOT touch ? / % (that is param-dependent, handled in execute)."""
    if _PRAGMA_ANY.match(sql):
        m = _PRAGMA_TABLE_INFO.match(sql)
        if m:
            t = m.group(1)
            return ("SELECT ordinal_position-1 AS cid, column_name AS name, "
                    "data_type AS type FROM information_schema.columns "
                    f"WHERE table_name='{t}' ORDER BY ordinal_position")
        return _NOOP

    m = _INSERT_REPLACE.match(sql)
    if m:
        table, collist = m.group(1), m.group(2)
        cols = [c.strip() for c in collist.split(",")]
        body = _INSERT_REPLACE.sub(f"INSERT INTO {table} ({collist})", sql, count=1)
        return body.rstrip().rstrip(";") + _conflict_clause(table, cols)

    m = _INSERT_IGNORE.match(sql)
    if m:
        table = m.group(1)
        body = _INSERT_IGNORE.sub(f"INSERT INTO {table}", sql, count=1)
        keys = CONFLICT_KEYS.get(table)
        if keys:
            body = body.rstrip().rstrip(";") + f" ON CONFLICT ({', '.join(keys)}) DO NOTHING"
        return body

    if _AUTOINC.search(sql):
        return _AUTOINC.sub("SERIAL PRIMARY KEY", sql)

    return sql


def _bind(sql):
    """Make a translated statement safe for psycopg's %s paramstyle."""
    return sql.replace("%", "%%").replace("?", "%s")


def _split_statements(script):
    out = []
    for raw in script.split(";"):
        meaningful = [ln for ln in raw.splitlines()
                      if ln.strip() and not ln.strip().startswith("--")]
        if meaningful:
            out.append(raw.strip())
    return out


class Cursor:
    def __init__(self, pgcur):
        self._cur = pgcur
        self.lastrowid = None

    def execute(self, sql, params=None):
        translated = _translate_sql(sql)
        if translated is _NOOP:
            self.lastrowid = None
            return self
        bind = (params is not None and len(params) > 0) or "?" in translated
        sql2 = _bind(translated) if bind else translated

        returning = None
        m = _INSERT_INTO.match(sql2)
        if m and "RETURNING" not in sql2.upper() and "ON CONFLICT" not in sql2.upper():
            pk = SERIAL_PK.get(m.group(1))
            if pk:
                sql2 = sql2.rstrip().rstrip(";") + f" RETURNING {pk}"
                returning = pk

        self._cur.execute(sql2, tuple(params) if params else None)

        self.lastrowid = None
        if returning is not None:
            try:
                row = self._cur.fetchone()
                self.lastrowid = row[0] if row else None
            except Exception:
                pass
        return self

    def executemany(self, sql, seq_of_params):
        translated = _translate_sql(sql)
        if translated is _NOOP:
            return self
        self._cur.executemany(_bind(translated), [tuple(p) for p in seq_of_params])
        self.lastrowid = None
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def fetchmany(self, size=None):
        return self._cur.fetchmany(size) if size is not None else self._cur.fetchmany()

    def __iter__(self):
        return iter(self._cur)

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        self._cur.close()


class Connection:
    def __init__(self, pgconn):
        self._conn = pgconn

    def cursor(self):
        return Cursor(self._conn.cursor())

    def execute(self, sql, params=None):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql, seq_of_params):
        cur = self.cursor()
        cur.executemany(sql, seq_of_params)
        return cur

    def executescript(self, script):
        for stmt in _split_statements(script):
            self.execute(stmt)
        return self

    def commit(self):
        try:
            self._conn.commit()
        except Exception:
            pass

    def rollback(self):
        try:
            self._conn.rollback()
        except Exception:
            pass

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False


def connect(url):
    import psycopg
    return Connection(psycopg.connect(url, autocommit=True))

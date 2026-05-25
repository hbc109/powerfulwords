"""Paper-trading ledger.

Records the composite signal as a "planned trade" each day, auto-generates
a short reasoning string from the factor breakdown, and resolves positions
when the next day's plan flips direction. Used to track real-time
out-of-sample performance of the composite signal.

Storage: SQLite table `paper_trades` in the same DB as everything else.
Created on first use (idempotent).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from app.db.database import get_connection

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS paper_trades (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,           -- LONG, SHORT, FLAT
    target_position REAL NOT NULL,
    composite_score REAL,
    regime TEXT,
    narrative_z REAL,
    term_structure REAL,
    positioning REAL,
    inventory REAL,
    breakdown_json TEXT,
    entry_close REAL,
    reasoning TEXT,
    notes TEXT,
    exit_date TEXT,
    exit_close REAL,
    realized_pnl_pct REAL,
    holding_days INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_paper_trades_symbol_date ON paper_trades(symbol, plan_date);
CREATE INDEX IF NOT EXISTS idx_paper_trades_open ON paper_trades(symbol, exit_date);
"""

# Idempotent column adds for fields added after the original schema.
_OPTIONAL_COLUMNS = [
    ("vetoes_json", "TEXT"),    # JSON array of Tier-1 veto reason strings; annotation only
]


def _ensure_optional_columns(conn: sqlite3.Connection) -> None:
    """ALTER TABLE ADD COLUMN for each post-schema column if missing. Idempotent."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(paper_trades)").fetchall()}
    for col, decl in _OPTIONAL_COLUMNS:
        if col not in cols:
            conn.execute(f"ALTER TABLE paper_trades ADD COLUMN {col} {decl}")
    conn.commit()


def ensure_table(conn: Optional[sqlite3.Connection] = None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    conn.executescript(CREATE_SQL)
    _ensure_optional_columns(conn)
    conn.commit()
    if own:
        conn.close()


def generate_reasoning(direction: str, regime: str, breakdown: list, signal_threshold: float = 0.1) -> str:
    """Auto-generate a one-line human-readable explanation from the composite breakdown.

    Examples:
      "LONG (range): driven by inventory +0.18, narrative +0.10; positioning gated."
      "FLAT (trend_up): composite below ±0.10 threshold; narrative weakly +0.04, inventory weakly -0.02."
      "SHORT (shock): inventory -0.30 dominates; narrative +0.05 disagrees but outweighed."
    """
    if not breakdown:
        return f"{direction} ({regime}): no factor breakdown available."

    sorted_b = sorted(breakdown, key=lambda r: abs(r.get("contribution", 0.0)), reverse=True)
    nonzero = [r for r in sorted_b if abs(r.get("contribution", 0.0)) > 1e-6]

    if direction == "FLAT":
        if not nonzero:
            return f"FLAT ({regime}): all factors gated out / unavailable."
        lead = ", ".join(f"{r['factor']} {'+' if r['contribution'] >= 0 else ''}{r['contribution']:.2f}" for r in nonzero[:3])
        return f"FLAT ({regime}): composite below ±{signal_threshold:.2f} threshold; mix is {lead}."

    # Direction is LONG or SHORT — identify drivers (same sign as direction) and disagreements (opposite)
    sign = 1 if direction == "LONG" else -1
    drivers = [r for r in nonzero if r["contribution"] * sign > 0]
    disagreers = [r for r in nonzero if r["contribution"] * sign < 0]

    drivers_str = ", ".join(f"{r['factor']} {'+' if r['contribution'] >= 0 else ''}{r['contribution']:.2f}" for r in drivers[:2])
    parts = [f"{direction} ({regime}): driven by {drivers_str}" if drivers else f"{direction} ({regime}): no clear driver"]
    if disagreers:
        opp = ", ".join(f"{r['factor']} {'+' if r['contribution'] >= 0 else ''}{r['contribution']:.2f}" for r in disagreers[:2])
        parts.append(f"despite {opp}")
    # Mention gated positioning only if it's not in the breakdown at all
    if not any(r["factor"] == "positioning" for r in breakdown):
        parts.append("positioning gated")
    return "; ".join(parts) + "."


def get_open_position(conn: sqlite3.Connection, symbol: str) -> Optional[sqlite3.Row]:
    cur = conn.execute(
        "SELECT * FROM paper_trades WHERE symbol=? AND exit_date IS NULL ORDER BY plan_date DESC LIMIT 1",
        (symbol,),
    )
    cur.row_factory = sqlite3.Row
    return cur.fetchone()


def get_all_open_positions(symbol: Optional[str] = None,
                           conn: Optional[sqlite3.Connection] = None) -> list:
    """All open trades, optionally filtered by symbol. Sorted oldest-first."""
    own = conn is None
    if own:
        conn = get_connection()
    ensure_table(conn)
    sql = "SELECT * FROM paper_trades WHERE exit_date IS NULL"
    params: list = []
    if symbol:
        sql += " AND symbol=?"
        params.append(symbol)
    sql += " ORDER BY plan_date ASC, trade_id ASC"
    cur = conn.execute(sql, params)
    cur.row_factory = sqlite3.Row
    rows = [dict(r) for r in cur.fetchall()]
    if own:
        conn.close()
    for r in rows:
        if r.get("breakdown_json"):
            try:
                r["breakdown"] = json.loads(r["breakdown_json"])
            except Exception:
                r["breakdown"] = None
        if r.get("vetoes_json"):
            try:
                r["vetoes"] = json.loads(r["vetoes_json"])
            except Exception:
                r["vetoes"] = None
    return rows


def evaluate_closes(
    symbol: str,
    asof: date,
    current_composite: Optional[float],
    exit_close: Optional[float],
    *,
    entry_threshold_long: float = 0.10,
    entry_threshold_short: float = -0.10,
    conn: Optional[sqlite3.Connection] = None,
) -> list:
    """Close all open trades for `symbol` whose direction has reversed past
    the opposite entry threshold given today's composite.

    Rule (option A — "reversal past opposite entry threshold"):
      - Open LONG → closed if current_composite <= entry_threshold_short (default -0.10)
      - Open SHORT → closed if current_composite >= entry_threshold_long (default +0.10)
      - Open FLAT → never auto-closed (FLAT is "no trade", not a directional view)

    Returns list of closed trade_ids.
    """
    own = conn is None
    if own:
        conn = get_connection()
    ensure_table(conn)

    if current_composite is None or exit_close is None:
        if own:
            conn.close()
        return []

    cur = conn.execute(
        "SELECT trade_id, plan_date, direction, target_position, entry_close "
        "FROM paper_trades WHERE symbol=? AND exit_date IS NULL",
        (symbol,),
    )
    cur.row_factory = sqlite3.Row
    open_trades = cur.fetchall()

    closed_ids: list = []
    asof_iso = asof.isoformat()
    for tr in open_trades:
        d = tr["direction"]
        should_close = False
        if d == "LONG" and current_composite <= entry_threshold_short:
            should_close = True
        elif d == "SHORT" and current_composite >= entry_threshold_long:
            should_close = True
        if not should_close:
            continue
        entry_px = tr["entry_close"]
        if entry_px and exit_close:
            raw_ret = (exit_close / entry_px) - 1.0
            pnl_pct = raw_ret * float(tr["target_position"])
        else:
            pnl_pct = None
        try:
            d_in = date.fromisoformat(tr["plan_date"])
            holding = (asof - d_in).days
        except Exception:
            holding = None
        conn.execute(
            "UPDATE paper_trades SET exit_date=?, exit_close=?, realized_pnl_pct=?, holding_days=? "
            "WHERE trade_id=?",
            (asof_iso, exit_close, pnl_pct, holding, tr["trade_id"]),
        )
        closed_ids.append(tr["trade_id"])

    if closed_ids:
        conn.commit()
    if own:
        conn.close()
    return closed_ids


def compute_mtm_pct(entry_close: Optional[float], current_close: Optional[float],
                    target_position: float) -> Optional[float]:
    """Unrealized PnL pct for an open position, marked at current_close."""
    if not entry_close or not current_close:
        return None
    return (current_close / entry_close - 1.0) * float(target_position)


def record_snapshot(
    plan_date: date,
    symbol: str,
    direction: str,
    target_position: float,
    composite_score: Optional[float],
    regime: Optional[str],
    narrative_z: Optional[float],
    term_structure: Optional[float],
    positioning: Optional[float],
    inventory: Optional[float],
    breakdown: Optional[list],
    entry_close: Optional[float],
    notes: Optional[str] = None,
    vetoes: Optional[list] = None,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> dict:
    """Record one day's snapshot for one symbol. Auto-resolves any open
    position whose direction differs from this one.

    Returns: {"trade_id": int, "resolved": Optional[trade_id], "reasoning": str}
    """
    own = conn is None
    if own:
        conn = get_connection()
    ensure_table(conn)

    plan_date_iso = plan_date.isoformat()
    reasoning = generate_reasoning(direction, regime or "?", breakdown or [])

    # Skip if we already have a snapshot for this (symbol, plan_date)
    existing = conn.execute(
        "SELECT trade_id FROM paper_trades WHERE symbol=? AND plan_date=?",
        (symbol, plan_date_iso),
    ).fetchone()
    if existing:
        if own:
            conn.close()
        return {"trade_id": existing[0], "resolved": None, "reasoning": reasoning,
                "skipped_dup": True, "skipped_flat": False}

    # Skip FLAT signals — FLAT is "no trade today", not a position. We don't
    # pollute the ledger with FLAT entries. Closes of existing positions are
    # handled separately by evaluate_closes().
    if direction == "FLAT" or target_position == 0:
        if own:
            conn.close()
        return {"trade_id": None, "resolved": None, "reasoning": reasoning,
                "skipped_dup": False, "skipped_flat": True}

    conn.execute(
        """INSERT INTO paper_trades (
            plan_date, symbol, direction, target_position,
            composite_score, regime, narrative_z, term_structure, positioning, inventory,
            breakdown_json, entry_close, reasoning, notes, vetoes_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            plan_date_iso, symbol, direction, target_position,
            composite_score, regime, narrative_z, term_structure, positioning, inventory,
            json.dumps(breakdown) if breakdown is not None else None,
            entry_close, reasoning, notes,
            json.dumps(vetoes) if vetoes else None,
        ),
    )
    trade_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    if own:
        conn.close()
    return {"trade_id": trade_id, "resolved": None, "reasoning": reasoning,
            "skipped_dup": False, "skipped_flat": False}


def load_trades(symbol: Optional[str] = None, limit: Optional[int] = None,
                conn: Optional[sqlite3.Connection] = None) -> list:
    own = conn is None
    if own:
        conn = get_connection()
    ensure_table(conn)
    sql = "SELECT * FROM paper_trades"
    params = []
    if symbol:
        sql += " WHERE symbol=?"
        params.append(symbol)
    sql += " ORDER BY plan_date DESC, trade_id DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur = conn.execute(sql, params)
    cur.row_factory = sqlite3.Row
    rows = [dict(r) for r in cur.fetchall()]
    if own:
        conn.close()
    for r in rows:
        if r.get("breakdown_json"):
            try:
                r["breakdown"] = json.loads(r["breakdown_json"])
            except Exception:
                r["breakdown"] = None
        if r.get("vetoes_json"):
            try:
                r["vetoes"] = json.loads(r["vetoes_json"])
            except Exception:
                r["vetoes"] = None
    return rows

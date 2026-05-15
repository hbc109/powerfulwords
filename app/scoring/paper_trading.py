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


def ensure_table(conn: Optional[sqlite3.Connection] = None) -> None:
    own = conn is None
    if own:
        conn = get_connection()
    conn.executescript(CREATE_SQL)
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
        return {"trade_id": existing[0], "resolved": None, "reasoning": reasoning, "skipped_dup": True}

    # Auto-resolve any open position for this symbol if direction changed
    open_row = get_open_position(conn, symbol)
    resolved_id = None
    if open_row is not None:
        prev_dir = open_row["direction"]
        if prev_dir != direction:
            entry_px = open_row["entry_close"]
            if entry_px and entry_close:
                raw_ret = (entry_close / entry_px) - 1.0
                pnl_pct = raw_ret * float(open_row["target_position"])
            else:
                pnl_pct = None
            try:
                d_in = date.fromisoformat(open_row["plan_date"])
                holding = (plan_date - d_in).days
            except Exception:
                holding = None
            conn.execute(
                "UPDATE paper_trades SET exit_date=?, exit_close=?, realized_pnl_pct=?, holding_days=? "
                "WHERE trade_id=?",
                (plan_date_iso, entry_close, pnl_pct, holding, open_row["trade_id"]),
            )
            resolved_id = open_row["trade_id"]

    conn.execute(
        """INSERT INTO paper_trades (
            plan_date, symbol, direction, target_position,
            composite_score, regime, narrative_z, term_structure, positioning, inventory,
            breakdown_json, entry_close, reasoning, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            plan_date_iso, symbol, direction, target_position,
            composite_score, regime, narrative_z, term_structure, positioning, inventory,
            json.dumps(breakdown) if breakdown is not None else None,
            entry_close, reasoning, notes,
        ),
    )
    trade_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    if own:
        conn.close()
    return {"trade_id": trade_id, "resolved": resolved_id, "reasoning": reasoning, "skipped_dup": False}


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
    return rows

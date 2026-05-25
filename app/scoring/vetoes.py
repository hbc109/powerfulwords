"""Tier-1 vetoes — annotation-only flags for paper trades.

Three vetoes ship together (all annotation only — they do NOT block
the trade, they just record a reason on the row so we can later
compare hit rate of vetoed vs non-vetoed trades):

  1. magnitude: |composite| < 0.40 → "weak signal"
  2. cross_factor_disagreement: any factor has |z| > 2 AGAINST the
     composite direction → "factor X disagrees strongly"
  3. high_volatility: atr_ratio > 1.30 on the symbol's regime row →
     "elevated vol, signal noise high"

Each veto returns either None (no veto) or a short reason string.
`evaluate_vetoes` collects all that fire into a list.
"""

from __future__ import annotations

import sqlite3
from datetime import date
from typing import Optional

MAGNITUDE_THRESHOLD = 0.40
CROSS_FACTOR_THRESHOLD = 2.0
VOL_RATIO_THRESHOLD = 1.30


def magnitude_veto(composite: Optional[float]) -> Optional[str]:
    """Fire if signal is weaker than `MAGNITUDE_THRESHOLD`."""
    if composite is None:
        return None
    if abs(composite) < MAGNITUDE_THRESHOLD:
        return f"magnitude: |{composite:+.3f}| < {MAGNITUDE_THRESHOLD:.2f} (weak signal)"
    return None


def cross_factor_veto(composite: Optional[float], breakdown: Optional[list]) -> Optional[str]:
    """Fire if any factor has |z| > CROSS_FACTOR_THRESHOLD AGAINST the
    composite direction. Catches "good signal at bad moment" cases."""
    if composite is None or not breakdown:
        return None
    sign = 1 if composite > 0 else -1
    disagreers = []
    for r in breakdown:
        try:
            z = float(r.get("value"))
        except (TypeError, ValueError):
            continue
        # Factor disagrees if its z has opposite sign AND magnitude > threshold
        if (z * sign) < 0 and abs(z) > CROSS_FACTOR_THRESHOLD:
            disagreers.append(f"{r.get('factor', '?')} z={z:+.2f}")
    if not disagreers:
        return None
    return f"cross_factor: {', '.join(disagreers)} (against {'LONG' if sign > 0 else 'SHORT'} signal)"


def _parent_for_vol(symbol: str) -> str:
    """Map a spread symbol back to its parent for vol lookup
    (the spread's vol is driven by the underlying outright's regime)."""
    if symbol.endswith("_M1M2"):
        return symbol[: -len("_M1M2")]
    return symbol


def vol_veto(symbol: str, asof: date, conn: Optional[sqlite3.Connection] = None) -> Optional[str]:
    """Fire if atr_ratio > VOL_RATIO_THRESHOLD on the symbol's most
    recent regime row on or before `asof`."""
    own = conn is None
    if own:
        from app.db.database import get_connection
        conn = get_connection()
    parent = _parent_for_vol(symbol)
    row = conn.execute(
        "SELECT regime_date, atr_ratio FROM daily_regimes "
        "WHERE symbol=? AND regime_date<=? "
        "ORDER BY regime_date DESC LIMIT 1",
        (parent, asof.isoformat()),
    ).fetchone()
    if own:
        conn.close()
    if not row or row[1] is None:
        return None
    atr_ratio = float(row[1])
    if atr_ratio > VOL_RATIO_THRESHOLD:
        return f"vol: atr_ratio={atr_ratio:.2f} > {VOL_RATIO_THRESHOLD:.2f} (elevated vol, signal whipsaw risk)"
    return None


def evaluate_vetoes(
    symbol: str,
    asof: date,
    composite: Optional[float],
    breakdown: Optional[list],
    conn: Optional[sqlite3.Connection] = None,
) -> list[str]:
    """Run all Tier-1 vetoes for `(symbol, asof)` and return the
    list of reasons that fired. Empty list = no vetoes."""
    reasons: list[str] = []
    for veto_fn in (
        lambda: magnitude_veto(composite),
        lambda: cross_factor_veto(composite, breakdown),
        lambda: vol_veto(symbol, asof, conn=conn),
    ):
        r = veto_fn()
        if r:
            reasons.append(r)
    return reasons

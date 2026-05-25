"""Pure technical-indicator trading signal.

Third comparator alongside Composite (narrative + factors + regime
weights) and Baseline (narrative only). This module produces a signal
using ONLY the technical indicators already computed by the regime
classifier (ADX, RSI, MACD, BB %B, ATR ratio) — no narrative, no
positioning, no inventory.

Design philosophy: simplest mapping that uses the regime classifier's
own output as a tradable signal, with intensity / quality filters:

  trend_up   AND adx14 > 25  → LONG  (trend follower, only when trend is strong)
  trend_down AND adx14 > 25  → SHORT
  stretched_up AND bb_pctb > 1.0 → SHORT (mean-revert fade)
  stretched_down AND bb_pctb < 0   → LONG  (mean-revert fade)
  shock / range / weak-ADX trend → FLAT

Returns a composite-style score in roughly [-1.0, +1.0]:

  +1.0 = clean trend-follow LONG
  -1.0 = clean trend-follow SHORT
  +0.5 = mean-revert LONG (stretched down)
  -0.5 = mean-revert SHORT (stretched up)
   0.0 = no signal
"""

from __future__ import annotations

import sqlite3
from datetime import date
from typing import Optional

from app.db.database import get_connection

ADX_TREND_THRESHOLD = 25.0


def _regime_row(symbol: str, asof: date, conn: sqlite3.Connection) -> Optional[dict]:
    row = conn.execute(
        "SELECT regime_date, primary_regime, regime_tags, adx14, rsi14, bb_pctb, "
        "       atr_ratio, macd_hist, close "
        "FROM daily_regimes WHERE symbol=? AND regime_date<=? "
        "ORDER BY regime_date DESC LIMIT 1",
        (symbol, asof.isoformat()),
    ).fetchone()
    if not row:
        return None
    return {
        "regime_date": row[0], "primary_regime": row[1], "regime_tags": row[2] or "",
        "adx14": row[3], "rsi14": row[4], "bb_pctb": row[5],
        "atr_ratio": row[6], "macd_hist": row[7], "close": row[8],
    }


def technical_signal(
    symbol: str,
    asof: date,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> dict:
    """Return the technical signal as a dict mirroring composite_score's
    output shape:
      {
        "total": float,           # signal value in roughly [-1.0, +1.0]
        "regime": str,            # primary_regime used
        "breakdown": [             # which indicators fired
          {"factor": str, "value": float, "weight": float, "contribution": float},
          ...
        ],
        "reasoning": str,         # short human explanation
      }
    """
    own = conn is None
    if own:
        conn = get_connection()
    r = _regime_row(symbol, asof, conn)
    if own:
        conn.close()

    if r is None:
        return {"total": 0.0, "regime": None, "breakdown": [],
                "reasoning": "no regime row available"}

    regime = r["primary_regime"]
    adx = float(r.get("adx14") or 0.0)
    bb = r.get("bb_pctb")
    bb_val = float(bb) if bb is not None else None
    macd = r.get("macd_hist")
    macd_val = float(macd) if macd is not None else 0.0
    rsi = float(r.get("rsi14") or 50.0)

    total = 0.0
    breakdown: list = []
    parts: list[str] = []

    if regime == "trend_up":
        if adx > ADX_TREND_THRESHOLD:
            total += 1.0
            breakdown.append({"factor": "trend_follow",
                              "value": 1.0, "weight": 1.0, "contribution": +1.0})
            parts.append(f"LONG trend_up (ADX={adx:.1f} > {ADX_TREND_THRESHOLD:.0f})")
        else:
            parts.append(f"trend_up but ADX={adx:.1f} weak — FLAT")
    elif regime == "trend_down":
        if adx > ADX_TREND_THRESHOLD:
            total -= 1.0
            breakdown.append({"factor": "trend_follow",
                              "value": -1.0, "weight": 1.0, "contribution": -1.0})
            parts.append(f"SHORT trend_down (ADX={adx:.1f} > {ADX_TREND_THRESHOLD:.0f})")
        else:
            parts.append(f"trend_down but ADX={adx:.1f} weak — FLAT")
    elif regime == "stretched_up":
        # Mean-revert fade: short the over-stretched move
        if bb_val is not None and bb_val > 1.0:
            total -= 0.5
            breakdown.append({"factor": "mean_revert",
                              "value": -0.5, "weight": 1.0, "contribution": -0.5})
            parts.append(f"SHORT stretched_up (BB %B={bb_val:.2f} > 1.0, fade)")
        else:
            parts.append(f"stretched_up but BB %B={bb_val} not extreme — FLAT")
    elif regime == "stretched_down":
        if bb_val is not None and bb_val < 0:
            total += 0.5
            breakdown.append({"factor": "mean_revert",
                              "value": 0.5, "weight": 1.0, "contribution": +0.5})
            parts.append(f"LONG stretched_down (BB %B={bb_val:.2f} < 0, fade)")
        else:
            parts.append(f"stretched_down but BB %B={bb_val} not extreme — FLAT")
    elif regime == "shock":
        parts.append("shock regime — FLAT (vol too high)")
    elif regime == "range":
        parts.append("range regime — FLAT (no clear direction)")
    else:
        parts.append(f"regime={regime} — FLAT")

    return {
        "total": total,
        "regime": regime,
        "breakdown": breakdown,
        "reasoning": "; ".join(parts),
    }

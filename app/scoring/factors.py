"""Per-factor scorers used by the composite signal.

Each function takes (symbol, asof) and returns a float roughly in
[-2, 2] (z-score scale), or None if there is not enough data.

The composite layer (app/scoring/composite.py) reads each factor and
combines them with regime-conditional weights from
app/config/strategy_config.json.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from app.db.database import get_connection


SUPPORTED_TERM_STRUCTURE_SYMBOLS = ("WTI", "Brent")
SUPPORTED_POSITIONING_SYMBOLS = ("WTI", "Brent")


def positioning_factor(
    symbol: str,
    asof: date,
    *,
    lookback_weeks: int = 52,
    min_obs: int = 26,
    extreme_threshold: float = 1.0,
) -> Optional[float]:
    """Contrarian COT money-manager positioning factor with extreme gate.

    Reads weekly MM net% (long - short)/OI from synthetic symbol
    `<symbol>_COT_MM_NETPCT` and computes the z-score over the last
    `lookback_weeks`.

    The contrarian fade only fires past `extreme_threshold` (σ units).
    Within the deadband the factor contributes nothing; past it the
    magnitude grows linearly with how far past the gate the z is.

      |z| ≤ threshold    → 0
      |z| > threshold    → -sign(z) * (|z| - threshold)

    So at threshold=1.0σ: z=+0.8 → 0; z=+1.5 → -0.5; z=+2.0 → -1.0.
    Returns None if fewer than `min_obs` weekly observations are
    available in the window.
    """
    if symbol not in SUPPORTED_POSITIONING_SYMBOLS:
        raise NotImplementedError(
            f"positioning_factor supports {SUPPORTED_POSITIONING_SYMBOLS}, got {symbol!r}"
        )

    pos_sym = f"{symbol}_COT_MM_NETPCT"
    start = (asof - timedelta(weeks=lookback_weeks)).isoformat()
    end = asof.isoformat()

    conn = get_connection()
    cur = conn.execute(
        """
        SELECT price_time, close
        FROM market_prices
        WHERE symbol = ?
          AND price_time BETWEEN ? AND ?
          AND close IS NOT NULL
        ORDER BY price_time
        """,
        (pos_sym, start, end),
    )
    rows = cur.fetchall()
    conn.close()

    if len(rows) < min_obs:
        return None

    values = [r[1] for r in rows]
    latest = values[-1]
    n = len(values)
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    std = var ** 0.5
    if std == 0:
        return 0.0
    z = (latest - mean) / std
    if abs(z) <= extreme_threshold:
        return 0.0
    sign = -1.0 if z > 0 else 1.0
    return sign * (abs(z) - extreme_threshold)


def term_structure_factor(
    symbol: str,
    asof: date,
    *,
    lookback_days: int = 90,
    min_obs: int = 30,
) -> Optional[float]:
    """Z-score of the front spread (M1 - M2) over `lookback_days`.

    Backwardation (M1 > M2) is bullish for flat price; the spread's
    natural sign already matches that convention, so no flip needed.
    Returns None if fewer than `min_obs` daily observations are
    available in the window. Supports WTI and Brent.
    """
    if symbol not in SUPPORTED_TERM_STRUCTURE_SYMBOLS:
        raise NotImplementedError(
            f"term_structure_factor supports {SUPPORTED_TERM_STRUCTURE_SYMBOLS}, got {symbol!r}"
        )

    m1_sym = f"{symbol}_M1"
    m2_sym = f"{symbol}_M2"
    start = (asof - timedelta(days=lookback_days)).isoformat()
    end = asof.isoformat()

    conn = get_connection()
    cur = conn.execute(
        """
        SELECT m1.price_time, (m1.close - m2.close) AS spread
        FROM market_prices m1
        JOIN market_prices m2 ON m1.price_time = m2.price_time
        WHERE m1.symbol = ? AND m2.symbol = ?
          AND m1.price_time BETWEEN ? AND ?
          AND m1.close IS NOT NULL AND m2.close IS NOT NULL
        ORDER BY m1.price_time
        """,
        (m1_sym, m2_sym, start, end),
    )
    rows = cur.fetchall()
    conn.close()

    if len(rows) < min_obs:
        return None

    spreads = [r[1] for r in rows]
    latest = spreads[-1]
    n = len(spreads)
    mean = sum(spreads) / n
    var = sum((s - mean) ** 2 for s in spreads) / n
    std = var ** 0.5
    if std == 0:
        return 0.0
    return (latest - mean) / std

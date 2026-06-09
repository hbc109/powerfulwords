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


SUPPORTED_TERM_STRUCTURE_SYMBOLS = ("WTI", "Brent", "WTI_M1M2", "Brent_M1M2")


def _parent_for_term_structure(symbol: str) -> str:
    """Map a spread symbol (WTI_M1M2) back to its parent (WTI) so we look
    up the right M1/M2 series. Outright symbols pass through."""
    if symbol.endswith("_M1M2"):
        return symbol[: -len("_M1M2")]
    return symbol
SUPPORTED_POSITIONING_SYMBOLS = ("WTI", "Brent")
SUPPORTED_INVENTORY_SYMBOLS = ("WTI", "Brent")

INVENTORY_SERIES = (
    "EIA_CRUDE_STOCKS",
    "EIA_CUSHING_STOCKS",
    "EIA_GASOLINE_STOCKS",
    "EIA_DISTILLATE_STOCKS",
    "JODI_OECD_CRUDE_STOCKS",
)


def inventory_factor(
    symbol: str,
    asof: date,
    *,
    lookback_years: int = 5,
    week_window_days: int = 7,
    min_peers: int = 3,
) -> Optional[float]:
    """Seasonal-deviation z-score across EIA US petroleum stocks.

    For each EIA series, compares the latest reading on/before `asof`
    to the same week-of-year average over the trailing `lookback_years`,
    then z-scores against the seasonal std. Sign-flipped so that **high
    stocks vs seasonal = bearish factor** (negative value).

    The four series (crude, Cushing, gasoline, distillate) are
    equal-weight averaged into a single factor. Same factor is used
    for WTI and Brent — US inventory drives global oil flows.

    Returns None if no series has enough seasonal peers (`min_peers`)
    or if the database has no recent EIA data.
    """
    if symbol not in SUPPORTED_INVENTORY_SYMBOLS:
        raise NotImplementedError(
            f"inventory_factor supports {SUPPORTED_INVENTORY_SYMBOLS}, got {symbol!r}"
        )

    conn = get_connection()
    z_scores: list[float] = []
    cutoff = (asof - timedelta(days=lookback_years * 365 + 30)).isoformat()

    for series in INVENTORY_SERIES:
        # released_at (not price_time) gates "what was known at asof".
        # EIA WPSR's price_time is the Fri week-end; release lands the
        # following Wed. Without this filter the backtest was peeking
        # at numbers 5 days before they were public.
        latest = conn.execute(
            "SELECT price_time, close FROM market_prices "
            "WHERE symbol=? AND released_at <= ? AND close IS NOT NULL "
            "ORDER BY price_time DESC LIMIT 1",
            (series, asof.isoformat()),
        ).fetchone()
        if not latest:
            continue
        latest_date = date.fromisoformat(latest[0])
        latest_value = latest[1]
        target_doy = latest_date.timetuple().tm_yday

        rows = conn.execute(
            "SELECT price_time, close FROM market_prices "
            "WHERE symbol=? AND price_time < ? AND price_time >= ? AND close IS NOT NULL",
            (series, latest_date.isoformat(), cutoff),
        ).fetchall()
        peers = []
        for d_str, val in rows:
            doy = date.fromisoformat(d_str).timetuple().tm_yday
            diff = abs(doy - target_doy)
            diff = min(diff, 365 - diff)
            if diff <= week_window_days:
                peers.append(val)
        if len(peers) < min_peers:
            continue
        n = len(peers)
        mean = sum(peers) / n
        var = sum((v - mean) ** 2 for v in peers) / n
        std = var ** 0.5
        if std == 0:
            continue
        z_scores.append((latest_value - mean) / std)

    conn.close()
    if not z_scores:
        return None
    raw_z = sum(z_scores) / len(z_scores)
    return -raw_z  # high stocks vs seasonal = bearish


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
    # released_at upper bound gates "known at asof". COT is published Fri
    # for the previous Tue's as-of date — a 3-day lag. Lower bound stays
    # on price_time so the lookback window remains a 52w trailing window
    # of observation periods (sliding by release date would distort the
    # mean/std distributions).
    cur = conn.execute(
        """
        SELECT price_time, close
        FROM market_prices
        WHERE symbol = ?
          AND price_time >= ?
          AND released_at <= ?
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

    parent = _parent_for_term_structure(symbol)
    m1_sym = f"{parent}_M1"
    m2_sym = f"{parent}_M2"
    start = (asof - timedelta(days=lookback_days)).isoformat()
    end = asof.isoformat()

    conn = get_connection()
    # Front-month settles have lag=0 so released_at == price_time here,
    # but using released_at on the upper bound makes the lookahead-guard
    # uniform across factors — same pattern as inventory and positioning.
    cur = conn.execute(
        """
        SELECT m1.price_time, (m1.close - m2.close) AS spread
        FROM market_prices m1
        JOIN market_prices m2 ON m1.price_time = m2.price_time
        WHERE m1.symbol = ? AND m2.symbol = ?
          AND m1.price_time >= ?
          AND m1.released_at <= ?
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

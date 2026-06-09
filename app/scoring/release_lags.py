"""Per-series publication-lag policy for `market_prices`.

The audit (see commit history) found that every backtest in the repo
was overstating performance because factor queries filtered on
`price_time <= asof`, but `price_time` is "the period the data describes"
(EIA week-ending Friday, COT 'as of Tuesday'), NOT "when the data
became known". `released_at` decouples the two.

This module is the single source of truth for how long after `price_time`
each series becomes publicly available. Used by:

  - `app/db/database.py:upsert_market_price()` — sets `released_at` on
    every write going forward.
  - `scripts/backfill_release_dates.py` — populates `released_at` for
    existing rows where it is NULL.

Lookups are by symbol-prefix pattern. The first match wins, so order
matters: put more specific prefixes first. Lags are in days, applied to
`price_time` to produce `released_at`.

Numbers are conservative — they reflect the *latest* the data normally
arrives, not the earliest. Better to under-credit a recent signal than
to leak future information into a backtest.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Tuple


# (symbol_prefix, lag_days, why) — first match wins; order matters.
LAG_POLICY: Tuple[Tuple[str, int, str], ...] = (
    # EIA weekly petroleum status report — week ending Friday, releases
    # the following Wed 10:30 ET = +5 days from the Friday week-end.
    ("EIA_CRUDE_STOCKS",      5, "EIA WPSR — Fri week-end → Wed release"),
    ("EIA_CUSHING_STOCKS",    5, "EIA WPSR"),
    ("EIA_GASOLINE_STOCKS",   5, "EIA WPSR"),
    ("EIA_DISTILLATE_STOCKS", 5, "EIA WPSR"),
    # EIA STEO is monthly; published mid-month for the prior month.
    ("EIA_STEO_",             14, "EIA STEO — ~2-week lag from period-end"),
    # EIA daily spot prices have a ~1 business-day reporting lag.
    ("WTI_EIA_SPOT",          1, "EIA daily spot — T+1"),
    ("BRENT_EIA_SPOT",        1, "EIA daily spot — T+1"),
    # EIA futures settlement is published daily after the prior session.
    ("WTI_EIA_SETTLE",        1, "EIA futures settle — T+1"),
    ("BRENT_EIA_SETTLE",      1, "EIA futures settle — T+1"),
    # JODI is monthly OECD inventory; ~6-8 week lag from period end.
    # Use 50 days as a conservative midpoint of the public-release window.
    ("JODI_",                 50, "JODI monthly — ~6-8wk lag, take 50d"),
    # CFTC Commitments of Traders — "as of Tuesday", released the
    # following Friday at 15:30 ET.
    ("WTI_COT_",              3, "COT — Tue as-of → Fri release"),
    ("BRENT_COT_",            3, "COT — Tue as-of → Fri release"),
    # Broker morning brief — the broker writes the daily brief overnight
    # and publishes the next morning Asia time. Trade-date stored on the
    # row is the prior business day's settle; the brief is available the
    # following day.
    ("WTI_BROKER_SETTLE",     1, "Broker brief — published T+1 morning"),
    ("BRENT_BROKER_SETTLE",   1, "Broker brief — published T+1 morning"),
    # Per-month outright futures (used by term_structure) are quoted live;
    # the daily bar is the session close, available same day.
    ("WTI_M",                 0, "Futures session close — same day"),
    ("BRENT_M",               0, "Futures session close — same day"),
    # Front-month yfinance ticker (CL=F, BZ=F). Daily close, same-day.
    ("WTI",                   0, "yfinance daily close — same day"),
    ("Brent",                 0, "yfinance daily close — same day"),
)


def lag_days_for(symbol: str) -> int:
    """Return the publication lag in days for `symbol`. Match is case-
    insensitive because the DB carries mixed casings ("Brent_COT_..."
    vs "WTI_COT_..."). Falls back to 0 (same-day) for unknown symbols —
    safe default for price-style data; a missing lagged series would
    *over*-credit (silently no lookahead), still the right side to err on."""
    s = symbol.lower()
    for prefix, lag, _why in LAG_POLICY:
        if s.startswith(prefix.lower()):
            return lag
    return 0


def released_at_for(symbol: str, price_time: str) -> str:
    """Compute `released_at` for a row given its `price_time` string.
    Accepts ISO date or full ISO datetime; returns the same shape with
    `lag_days_for(symbol)` days added."""
    pt = price_time.strip()
    if not pt:
        return pt
    if "T" in pt or " " in pt and len(pt) > 10:
        try:
            dt = datetime.fromisoformat(pt.replace("Z", "+00:00"))
        except ValueError:
            d = date.fromisoformat(pt[:10])
            return (d + timedelta(days=lag_days_for(symbol))).isoformat()
        return (dt + timedelta(days=lag_days_for(symbol))).isoformat()
    d = date.fromisoformat(pt[:10])
    return (d + timedelta(days=lag_days_for(symbol))).isoformat()

"""Auto-fetch crude + product front-month futures prices from Yahoo Finance.

Maps Yahoo symbols to the symbols the rest of the system uses:

  CL=F  -> WTI         (NYMEX WTI front month)
  BZ=F  -> Brent       (ICE Brent front month)
  RB=F  -> RBOB_BBL    (NYMEX RBOB gasoline; Yahoo quotes $/gal -> we x42 to get $/bbl)
  HO=F  -> ULSD_BBL    (NYMEX ULSD/heating oil; Yahoo quotes $/gal -> x42 to $/bbl)

Yahoo's data is unofficial / best-effort; rate-limits and outages happen.
For production reliability swap in EIA v2 / your broker feed.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, List

import yfinance as yf


# (yahoo_symbol, internal_symbol, asset_type, unit_multiplier)
DEFAULT_INSTRUMENTS = [
    ("CL=F", "WTI", "commodity", 1.0),
    ("BZ=F", "Brent", "commodity", 1.0),
    ("RB=F", "RBOB_BBL", "product", 42.0),   # $/gal -> $/bbl
    ("HO=F", "ULSD_BBL", "product", 42.0),
]


def fetch_prices(
    instruments: Iterable[tuple] = DEFAULT_INSTRUMENTS,
    period: str = "3mo",
    interval: str = "1d",
) -> List[dict]:
    """Return a list of price rows in the same shape as load_prices_csv.py
    expects: {price_time, symbol, asset_type, open, high, low, close, volume}.
    """
    rows: List[dict] = []
    for ysym, sym, asset_type, mult in instruments:
        ticker = yf.Ticker(ysym)
        hist = ticker.history(period=period, interval=interval, auto_adjust=False)
        if hist is None or hist.empty:
            print(f"[WARN] No data returned for {ysym} ({sym}).")
            continue
        for ts, row in hist.iterrows():
            try:
                d = ts.date()
            except AttributeError:
                d = datetime.fromisoformat(str(ts)).date()
            rows.append({
                "price_time": d.isoformat(),
                "symbol": sym,
                "asset_type": asset_type,
                "open": float(row["Open"]) * mult if row["Open"] else None,
                "high": float(row["High"]) * mult if row["High"] else None,
                "low": float(row["Low"]) * mult if row["Low"] else None,
                "close": float(row["Close"]) * mult if row["Close"] else None,
                "volume": float(row["Volume"]) if row["Volume"] else None,
            })
    return rows

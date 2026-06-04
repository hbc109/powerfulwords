"""WTI + Brent term-structure fetcher.

Pulls multiple delivery-month contracts from Yahoo Finance and emits each
as a separate synthetic symbol (WTI_M1..M6, Brent_M1..M6). Spreads are
computed on demand by app/scoring/factors.py — we keep raw legs in DB.

Yahoo ticker convention: <PREFIX><MonthCode><YY>.NYM
  Month codes: F G H J K M N Q U V X Z (Jan..Dec)
  WTI:   CL<code><yy>.NYM   (NYMEX WTI)
  Brent: BZ<code><yy>.NYM   (NYMEX-listed Brent)

Roll heuristics (approximate, good enough for daily-close spreads):
  WTI    expires ~3 business days before the 25th of month X-1, so the
         front delivers in month X+1 normally, X+2 if today > day 20.
  Brent  expires last business day of month X-2, so the front delivers
         in month X+2 normally, X+3 if today > day 25.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

import yfinance as yf


MONTH_CODES = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

PREFIX = {"WTI": "CL", "Brent": "BZ"}
ROLL_DAY = {"WTI": 20, "Brent": 25}
FRONT_OFFSET = {"WTI": 1, "Brent": 2}  # delivery-month offset before roll


def front_month_index(commodity: str, today: date) -> int:
    """Absolute month index (year*12 + month-1, 0-indexed) of the front contract."""
    base = today.year * 12 + (today.month - 1)
    return base + FRONT_OFFSET[commodity] + (1 if today.day > ROLL_DAY[commodity] else 0)


def yahoo_ticker(commodity: str, month_idx: int) -> str:
    year, month = divmod(month_idx, 12)
    return f"{PREFIX[commodity]}{MONTH_CODES[month]}{year % 100:02d}.NYM"


def fetch_term_structure(
    commodities: List[str] = ["WTI", "Brent"],
    months: List[int] = [1, 2, 3, 6],
    period: str = "6mo",
    interval: str = "1d",
    asof: Optional[date] = None,
) -> List[dict]:
    """Pull delivery-month contracts as of `asof`. One row per (date, symbol).
    `months` are 1-indexed offsets from the front (M1 = front).
    """
    asof = asof or date.today()
    rows: List[dict] = []
    for commodity in commodities:
        if commodity not in PREFIX:
            print(f"[WARN] Skipping unknown commodity {commodity!r}.")
            continue
        front = front_month_index(commodity, asof)
        for m in months:
            # M1 uses the continuous front-month ticker (CL=F / BZ=F) — same
            # source the daily report uses for the headline settlement, so the
            # M1-M2 spread is anchored to the *broker-cited* settle. The
            # explicit-month NYMEX codes (CLN26.NYM, BZQ26.NYM, ...) are thinly
            # traded on Yahoo and frequently lag the real ICE/CME settle by
            # $0.50-$1.50, which would corrupt the spread. M2+ legitimately
            # need explicit-month codes (no continuous "M2" series exists).
            if m == 1:
                ysym = {"WTI": "CL=F", "Brent": "BZ=F"}[commodity]
            else:
                ysym = yahoo_ticker(commodity, front + (m - 1))
            sym = f"{commodity}_M{m}"
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
                    "asset_type": "term_structure",
                    "open": float(row["Open"]) if row["Open"] else None,
                    "high": float(row["High"]) if row["High"] else None,
                    "low": float(row["Low"]) if row["Low"] else None,
                    "close": float(row["Close"]) if row["Close"] else None,
                    "volume": float(row["Volume"]) if row["Volume"] else None,
                })

    # Derive M1-M2 spread series for each commodity (synthetic "tradable" symbol
    # used by the spread book in paper_trades). One row per (date, commodity).
    by_date_sym: dict[tuple, float] = {
        (r["price_time"], r["symbol"]): r["close"]
        for r in rows if r.get("close") is not None
    }
    spread_rows: List[dict] = []
    for commodity in commodities:
        if commodity not in PREFIX:
            continue
        m1_sym, m2_sym = f"{commodity}_M1", f"{commodity}_M2"
        dates = sorted({k[0] for k in by_date_sym.keys() if k[1] == m1_sym})
        for d in dates:
            m1c = by_date_sym.get((d, m1_sym))
            m2c = by_date_sym.get((d, m2_sym))
            if m1c is None or m2c is None:
                continue
            spread = m1c - m2c
            spread_rows.append({
                "price_time": d,
                "symbol": f"{commodity}_M1M2",
                "asset_type": "spread",
                "open": spread, "high": spread, "low": spread, "close": spread,
                "volume": None,
            })
    return rows + spread_rows

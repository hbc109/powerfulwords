"""Parse broker docs (港联 morning brief, Macquarie Energy Newsletter) for
the official daily WTI / Brent settle, and emit rows for the
WTI_BROKER_SETTLE / BRENT_BROKER_SETTLE synthetic symbols.

Why this fetcher exists:
yfinance's daily Close for CL=F / BZ=F is the end-of-Globex-session
electronic close, not the NYMEX / ICE pit-window settlement that
brokers and mark-to-market statements cite. On settled days yfinance
usually catches up within 24-48h, but on the most recent trade day
the gap can be $1-$2 — the user spotted Brent 6/3 yfinance close
$96.52 vs broker-cited official settle $97.81. We trust the broker
docs the user uploads daily as the authoritative source for the
most recent settle.

Supported formats:
  港联新闻早餐 (Chinese): "周三 7月WTI涨2.26报96.02美元/桶，涨幅2.41%；
                          8月布伦特涨1.81报97.81美元/桶，涨幅1.89%"
  Macquarie (English):  "Brent climbed $1.81 to settle at 97.81 USD/BBL,
                         while WTI gained $2.26 to ... 96.02"

The trade-date is published_at − 1 business day (morning briefs
recap the previous session). Holidays are not handled — broker docs
just don't cite settles on holidays so the parser yields nothing.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple


_GANGLIAN_PAT = re.compile(
    r"周[一二三四五六日]\s*\d+月WTI[涨跌]\s*\d+\.?\d*\s*报\s*(\d+\.?\d+)\s*美元/桶"
    r".*?\d+月布伦特[涨跌]\s*\d+\.?\d*\s*报\s*(\d+\.?\d+)\s*美元/桶",
    re.S,
)
_MACQUARIE_PAT = re.compile(
    r"Brent\s+(?:climbed|fell|gained|lost|rose|dropped)\s+\$?[\d.]+\s+"
    r"to\s+(?:settle\s+at\s+)?([\d.]+)\s+USD/BBL"
    r".*?WTI\s+(?:climbed|fell|gained|lost|rose|dropped)\s+\$?[\d.]+\s+"
    r"to\s+(?:settle\s+at\s+)?([\d.]+)",
    re.S | re.I,
)


def _prev_business_day(d: date) -> date:
    """Walk back to the previous Mon-Fri. Holidays aren't tracked."""
    d2 = d - timedelta(days=1)
    while d2.weekday() >= 5:  # 5=Sat, 6=Sun
        d2 -= timedelta(days=1)
    return d2


def parse_settle(text: str) -> Optional[Tuple[float, float]]:
    """Return (wti_close, brent_close) if either pattern matches, else None.

    港联 yields WTI first, Brent second. Macquarie has Brent first in the
    sentence but the regex captures groups (brent, wti) — we flip back to
    a uniform (wti, brent) tuple.
    """
    m = _GANGLIAN_PAT.search(text)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            pass
    m = _MACQUARIE_PAT.search(text)
    if m:
        try:
            brent, wti = float(m.group(1)), float(m.group(2))
            return wti, brent
        except ValueError:
            pass
    return None


def fetch_broker_settle(conn: sqlite3.Connection, days_back: int = 30) -> List[dict]:
    """Scan documents published in the last `days_back` days, parse settle
    citations, return rows in fetch_prices() shape. One pair per trade-date —
    if multiple docs cite the same trade-date the most recently published wins
    (so a Macquarie update overrides an earlier 港联 of the same day).
    """
    start = (date.today() - timedelta(days=days_back)).isoformat()
    rows = conn.execute(
        """
        SELECT title, source_name, published_at, raw_text
        FROM documents
        WHERE date(published_at) >= ?
          AND raw_text IS NOT NULL
          AND (raw_text LIKE '%港联新闻早餐%' OR raw_text LIKE '%Macquarie%')
        ORDER BY published_at ASC
        """,
        (start,),
    ).fetchall()

    by_trade_date: dict[str, Tuple[float, float, str]] = {}
    for title, source_name, published_at, raw in rows:
        parsed = parse_settle(raw or "")
        if not parsed:
            continue
        try:
            pub_date = date.fromisoformat(published_at[:10])
        except (ValueError, TypeError):
            continue
        trade_date = _prev_business_day(pub_date).isoformat()
        wti, brent = parsed
        # Last-write-wins because we sorted ASC — later iteration overrides earlier.
        by_trade_date[trade_date] = (wti, brent, source_name or "broker")

    out: List[dict] = []
    for td, (wti, brent, src) in by_trade_date.items():
        for sym, val in [("WTI_BROKER_SETTLE", wti), ("BRENT_BROKER_SETTLE", brent)]:
            out.append({
                "price_time": td,
                "symbol": sym,
                "asset_type": "commodity",
                "open": val, "high": val, "low": val, "close": val,
                "volume": None,
            })
    return out

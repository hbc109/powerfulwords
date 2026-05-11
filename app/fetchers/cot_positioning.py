"""CFTC Commitments of Traders (COT) positioning fetcher.

Pulls weekly Money-Manager net length for crude oil from the CFTC
public Socrata API and emits each as a synthetic symbol in the
market_prices table:

  WTI_COT_MM_NETPCT     close = (MM long - MM short) / OI * 100
  Brent_COT_MM_NETPCT   volume = open interest

Sign convention is contrarian-ready: the raw value is signed long-positive,
but app/scoring/factors.positioning_factor flips it so that crowded longs
read as a *bearish* factor (extreme positioning fades on average).

Source dataset: CFTC Disaggregated Futures and Options Combined
  https://publicreporting.cftc.gov/resource/kh3c-gbw2.json

Market identifiers (verified live as of 2026-05):
  WTI    'CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE'  (NYMEX legacy
         entry stopped reporting; CFTC tracks WTI under ICE Europe)
  Brent  'BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE'    (NYMEX-listed
         financially-settled Brent — the BZ contract)
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import List

CFTC_URL = "https://publicreporting.cftc.gov/resource/kh3c-gbw2.json"

MARKET_NAMES = {
    "WTI":   "CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE",
    "Brent": "BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE",
}


def _fetch_one(market_name: str, weeks: int) -> List[dict]:
    """Pull the last `weeks` weekly rows for one market identifier."""
    q = {
        "$where": f"market_and_exchange_names = '{market_name}'",
        "$select": ("report_date_as_yyyy_mm_dd, "
                    "m_money_positions_long_all, "
                    "m_money_positions_short_all, "
                    "open_interest_all"),
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": str(weeks),
    }
    url = CFTC_URL + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": "powerfulwords/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_cot_positioning(
    commodities: List[str] = ["WTI", "Brent"],
    weeks: int = 200,
) -> List[dict]:
    """Return rows in the same shape as fetch_prices(): one dict per
    (report_date, synthetic_symbol). Default `weeks=200` gives ~4 years
    of weekly data — enough for a 52-week z-score with comfortable buffer.
    """
    rows: List[dict] = []
    for commodity in commodities:
        name = MARKET_NAMES.get(commodity)
        if name is None:
            print(f"[WARN] No COT market identifier for {commodity!r}, skipping.")
            continue
        try:
            raw = _fetch_one(name, weeks)
        except Exception as e:
            print(f"[WARN] CFTC fetch failed for {commodity}: {e}")
            continue
        if not raw:
            print(f"[WARN] No COT rows returned for {commodity}.")
            continue
        sym = f"{commodity}_COT_MM_NETPCT"
        for r in raw:
            try:
                long_ = int(r["m_money_positions_long_all"])
                short_ = int(r["m_money_positions_short_all"])
                oi = int(r["open_interest_all"])
            except (KeyError, ValueError, TypeError):
                continue
            if oi <= 0:
                continue
            net_pct = 100.0 * (long_ - short_) / oi
            rows.append({
                "price_time": r["report_date_as_yyyy_mm_dd"][:10],
                "symbol": sym,
                "asset_type": "positioning",
                "open": net_pct,
                "high": net_pct,
                "low": net_pct,
                "close": net_pct,
                "volume": float(oi),
            })
    return rows

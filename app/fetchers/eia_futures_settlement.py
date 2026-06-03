"""EIA Open Data — official daily spot prices for WTI + Brent.

Two daily series, both EIA-official, used as a cross-check against
yfinance's CL=F / BZ=F daily close in the daily report:

  WTI_EIA_SPOT      EIA series RWTC — Cushing OK WTI Spot Price FOB.
                    Typically within $0.10 of NYMEX CL front-month
                    settlement. T+1 lag.

  BRENT_EIA_SPOT    EIA series RBRTE — Europe Brent Spot FOB.
                    Typically within $0.10-0.30 of ICE Brent front
                    settlement. T+1 lag.

Why not pull the actual NYMEX/ICE settlements directly:
  - EIA's RCLC1 (WTI front-month futures settlement) was discontinued
    around April 2024 — series endPeriod is 2024-04-05.
  - CME's public settlement JSON endpoint blocks automated requests
    (HTTP 403 with anti-scraping notice).
  - ICE has no free public daily futures settlement API.

So the consumer (app/scoring/daily_report.py) uses yfinance's daily
close on the LAST SETTLED bar (price_time < today) as the official
settlement — yfinance aligns daily futures candles to exchange
settlement once the session closes. The EIA spot series here is the
cross-check that catches any yfinance anomaly.

Source: https://www.eia.gov/opendata/  (free, requires API key)
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import date, timedelta
from typing import List, Optional


EIA_SPT_URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"


def _fetch(series_id: str, api_key: str, start: str) -> List[dict]:
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": "0",
        "length": "5000",
        "start": start,
    }
    full = EIA_SPT_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"User-Agent": "powerfulwords/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read())
    return body.get("response", {}).get("data", []) or []


def fetch_eia_futures_settlement(
    years_back: int = 1,
    api_key: Optional[str] = None,
) -> List[dict]:
    """Return rows for WTI_EIA_SPOT (RWTC) + BRENT_EIA_SPOT (RBRTE).

    Shape matches fetch_prices() so the same upsert path works.
    Function name kept for the existing import in scripts/fetch_prices.py;
    the series themselves are spot prices that proxy the exchange settle.
    """
    api_key = api_key or os.environ.get("EIA_API_KEY")
    if not api_key:
        print("[WARN] EIA_API_KEY not set — skipping EIA spot benchmark fetch.")
        return []

    start_str = (date.today() - timedelta(days=int(years_back * 365.25))).isoformat()

    series = [
        ("RWTC",  "WTI_EIA_SPOT",   "commodity"),  # WTI Cushing spot — proxies NYMEX CL settle within ~$0.10
        ("RBRTE", "BRENT_EIA_SPOT", "commodity"),  # Brent Europe spot — proxies ICE B settle within ~$0.10-0.30
    ]
    rows: List[dict] = []
    for series_id, sym, asset_type in series:
        try:
            raw = _fetch(series_id, api_key, start_str)
        except Exception as e:
            print(f"[WARN] EIA spot fetch failed for {series_id} ({sym}): {e}")
            continue
        if not raw:
            print(f"[WARN] No EIA rows returned for {series_id} ({sym}).")
            continue
        for r in raw:
            try:
                v = float(r["value"])
            except (KeyError, TypeError, ValueError):
                continue
            rows.append({
                "price_time": r["period"][:10],
                "symbol": sym,
                "asset_type": asset_type,
                "open": v,
                "high": v,
                "low": v,
                "close": v,
                "volume": None,
            })
    return rows

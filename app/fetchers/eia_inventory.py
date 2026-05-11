"""EIA Weekly Petroleum Status Report — inventory fetcher.

Pulls weekly US petroleum stocks via the EIA Open Data API v2 and emits
each series as a synthetic symbol in the market_prices table:

  EIA_CRUDE_STOCKS       US crude oil ending stocks excl. SPR (k bbl)
  EIA_CUSHING_STOCKS     Crude oil stocks at Cushing OK (k bbl)
  EIA_GASOLINE_STOCKS    Total motor gasoline ending stocks (k bbl)
  EIA_DISTILLATE_STOCKS  Total distillate ending stocks (k bbl)

`close` holds the level in thousand barrels; `volume` is left empty.
The factor (`app/scoring/factors.inventory_factor`) computes the
seasonal-deviation z-score from these levels.

Data published Wednesdays ~10:30am ET (Thursday after holidays).

Source: https://www.eia.gov/opendata/  (free, requires API key)
Register: https://www.eia.gov/opendata/register.php
Set env var: EIA_API_KEY=<your_key>
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import date, timedelta
from typing import List, Optional

EIA_BASE = "https://api.eia.gov/v2/petroleum/stoc/wstk/data/"

# (eia_series_id, our synthetic symbol)
SERIES = [
    ("WCESTUS1",                "EIA_CRUDE_STOCKS"),       # Weekly US Crude oil ending stocks excl SPR
    ("W_EPC0_SAX_YCUOK_MBBL",   "EIA_CUSHING_STOCKS"),     # Cushing OK crude
    ("WGTSTUS1",                "EIA_GASOLINE_STOCKS"),    # Total gasoline
    ("WDISTUS1",                "EIA_DISTILLATE_STOCKS"),  # Total distillate
]


def _fetch_one(series_id: str, api_key: str, start: Optional[str]) -> List[dict]:
    """Pull the series from EIA v2 API. Returns raw list of {period, value} dicts."""
    params = {
        "api_key": api_key,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": "0",
        "length": "5000",
    }
    if start:
        params["start"] = start
    url = EIA_BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "powerfulwords/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read())
    return body.get("response", {}).get("data", []) or []


def fetch_eia_inventory(
    years_back: int = 6,
    api_key: Optional[str] = None,
) -> List[dict]:
    """Return rows in the same shape as fetch_prices(): one dict per
    (period, synthetic_symbol). `years_back` defaults to 6 — enough for
    a 5-year seasonal baseline plus the current year.
    """
    api_key = api_key or os.environ.get("EIA_API_KEY")
    if not api_key:
        print("[WARN] EIA_API_KEY not set — skipping EIA inventory fetch. "
              "Get a free key at https://www.eia.gov/opendata/register.php")
        return []

    start_str = (date.today() - timedelta(days=int(years_back * 365.25))).isoformat()

    rows: List[dict] = []
    for series_id, sym in SERIES:
        try:
            raw = _fetch_one(series_id, api_key, start_str)
        except Exception as e:
            print(f"[WARN] EIA fetch failed for {series_id} ({sym}): {e}")
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
                "asset_type": "inventory",
                "open": v,
                "high": v,
                "low": v,
                "close": v,
                "volume": None,
            })
    return rows

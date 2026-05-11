"""JODI-Oil monthly inventory fetcher (OECD aggregate).

Pulls annual primary CSV files from jodidata.org and emits a single
synthetic monthly series for OECD crude stocks:

  JODI_OECD_CRUDE_STOCKS  Sum of CRUDEOIL closing stock levels (CLOSTLV)
                          across a basket of major OECD reporters,
                          monthly, in thousand barrels (KBBL).

JODI primary only carries crude-side products (CRUDEOIL, NGL, OTHERCRUDE,
TOTCRUDE) — there's no gasoline / distillate / jet there. Refined-product
coverage would require the SECONDARY dataset; for now we only pull crude
because EIA already covers US products well, and the value-add of JODI
is international crude context (Europe + Asia).

Cadence: monthly, lagged ~6-8 weeks (e.g. March data lands in May).
The factor function takes the latest available reading on/before asof,
so the monthly cadence is handled naturally.

Source: https://www.jodidata.org/oil/database/data-downloads.aspx
URL pattern: /_resources/files/downloads/oil-data/annual-csv/primary/{year}.csv
Free, no API key required.
"""

from __future__ import annotations

import urllib.request
from datetime import date
from typing import List

JODI_BASE = "https://www.jodidata.org/_resources/files/downloads/oil-data/annual-csv/primary"

# Major OECD oil consumers that consistently report monthly crude stocks.
# Selection criteria: largest OECD reporters by demand × reporting reliability.
OECD_BASKET = (
    "US",  # United States
    "JP",  # Japan
    "DE",  # Germany
    "FR",  # France
    "GB",  # United Kingdom
    "IT",  # Italy
    "ES",  # Spain
    "NL",  # Netherlands
    "KR",  # South Korea
    "CA",  # Canada
    "AU",  # Australia
)


def _fetch_year(year: int) -> str:
    """Pull one annual primary CSV. Current year file is named differently."""
    today_year = date.today().year
    if year == today_year:
        url = f"{JODI_BASE}/primaryyear{year}.csv"
    else:
        url = f"{JODI_BASE}/{year}.csv"
    req = urllib.request.Request(url, headers={"User-Agent": "powerfulwords/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_jodi_inventory(years_back: int = 6) -> List[dict]:
    """Pull last `years_back` years of JODI primary data, sum the OECD
    basket's CRUDEOIL closing stocks per month, and return rows in the
    same shape as fetch_prices(). One row per month for the synthetic
    symbol JODI_OECD_CRUDE_STOCKS.
    """
    today_year = date.today().year
    years = range(today_year - years_back + 1, today_year + 1)

    # month_total[YYYY-MM] = sum of OECD basket closing stocks (KBBL) for that month
    month_total: dict[str, float] = {}

    for y in years:
        try:
            text = _fetch_year(y)
        except Exception as e:
            print(f"[WARN] JODI fetch failed for {y}: {e}")
            continue
        for line in text.splitlines()[1:]:  # skip header
            parts = line.split(",")
            if len(parts) < 6:
                continue
            ref_area, period, product, flow, unit, val = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
            if (
                ref_area not in OECD_BASKET
                or product != "CRUDEOIL"
                or flow != "CLOSTLV"
                or unit != "KBBL"
            ):
                continue
            try:
                v = float(val)
            except (ValueError, TypeError):
                continue  # "-" or "x" missing markers
            month_total[period] = month_total.get(period, 0.0) + v

    rows: List[dict] = []
    for period, total in sorted(month_total.items()):
        # period is YYYY-MM; pin price_time to the first of the month
        pt = f"{period}-01" if len(period) == 7 else period[:10]
        rows.append({
            "price_time": pt,
            "symbol": "JODI_OECD_CRUDE_STOCKS",
            "asset_type": "inventory",
            "open": total,
            "high": total,
            "low": total,
            "close": total,
            "volume": None,
        })
    if not rows:
        print("[WARN] No JODI rows extracted — basket / filter may be wrong.")
    return rows

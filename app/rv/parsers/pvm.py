"""Parser for the PVM 'FAR EAST PRICE' sheet (Sheet1).

Layout: a month x instrument matrix. Column meaning is fixed by a stacked
header in rows ~3-6; each forward month is a row anchored in col 0 as e.g.
JUN'26, JUL'26, … (constant-maturity M1, M2, … in order). We map the clean
**left block** crude columns by position and sanity-check them against the
label row, which is the pragmatic, stable approach for a fixed broker template.

v1 = crude only, and only the left block (WTI / Brent / Dubai / Dated complex
incl. the Brent/Dubai EFS = the EW). The right-block Murban/Oman columns
(21-31) carry an ambiguous unit/convention on this template, so they are
deliberately deferred until confirmed rather than ingested as possibly-wrong
values.
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd

SHEET = "Sheet1"
_MONTH_RE = re.compile(r"^[A-Z]{3}'\d{2}$")          # JUN'26
_LABEL_ROW = 5                                       # the clearest header row

# (col index, canonical spread, category, expected label substring on row 5)
# NOTE: forward outrights are the SWAP columns (col2 WTI-swap, col6 Brent-swap),
# NOT col1/col5 (which carry a stale ICE/cash print ~$7 off the swap). Verified
# by arithmetic: col2 - col6 == col3 (the WTI/Brent swap diff).
_COLS = [
    (2,  "WTI",              "crude_outright",  "WTI"),
    (6,  "Brent",            "crude_outright",  "BRENT"),
    (8,  "Dated Brent",      "crude_outright",  "DTD"),
    (13, "Dubai",            "crude_outright",  "DUBAI"),
    (3,  "WTI-Brent",        "crude_diff",      "WTI/BRENT"),
    (4,  "WTI-Dubai",        "crude_diff",      "WTI/DUB"),
    (11, "Dated-Dubai",      "crude_diff",      "Dtd/Dub"),
    (12, "Brent-Dubai(EFS)", "crude_diff",      "EFS"),
    (9,  "Brent DFL",        "crude_struct",    "DFL"),
]


def _verify_columns(df) -> list[tuple]:
    """Keep only columns whose label row still matches the expected text, so a
    template column-shift degrades gracefully instead of mislabelling."""
    if df.shape[0] <= _LABEL_ROW:
        return _COLS
    good = []
    for col, spread, cat, expect in _COLS:
        if col < df.shape[1]:
            lbl = df.iat[_LABEL_ROW, col]
            if pd.notna(lbl) and expect.lower() in str(lbl).strip().lower():
                good.append((col, spread, cat, expect))
    return good or _COLS  # if the check fails wholesale, fall back to positions


def parse(path: str, obs_date: str) -> list[dict]:
    df = pd.ExcelFile(path).parse(SHEET, header=None)
    cols = _verify_columns(df)
    rows: list[dict] = []
    m = 0
    for r in range(df.shape[0]):
        c0 = df.iat[r, 0]
        if pd.isna(c0) or not _MONTH_RE.match(str(c0).strip()):
            continue
        m += 1
        tenor = f"M{m}"
        contract = str(c0).strip()
        if df.shape[1] > 20:
            c20 = df.iat[r, 20]
            if isinstance(c20, (pd.Timestamp, datetime)):
                contract = pd.Timestamp(c20).strftime("%b-%y")
            elif pd.notna(c20):
                contract = str(c20).strip()
        for col, spread, cat, _ in cols:
            v = df.iat[r, col]
            if pd.isna(v):
                continue
            try:
                v = float(v)
            except (TypeError, ValueError):
                continue
            rows.append({
                "obs_date": obs_date, "source": "PVM", "category": cat,
                "spread": spread, "tenor": tenor, "contract": contract,
                "value": round(v, 4), "unit": "$/bbl",
            })
    return rows

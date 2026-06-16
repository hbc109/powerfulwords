"""Parser for the Mitsui 'Indications' sheet (Asian swap closings).

Layout: a flat grid with three header rows (exchange / product / unit) and then
data rows. Each instrument occupies a value column followed by a '+/-' change
column. Data rows are a month-end date (→ constant-maturity M1, M2,… in row
order), a quarter strip (Q4-26), or a calendar year (2026/2027/2028).

Mitsui has no crude DIFFs (PVM is the crude reference) — its crude value here is
the outright levels plus the **JCC** benchmark. We also capture its Asian
**product** outrights (kero, gasoil, fuel 180/380, naphtha) now, tagged
`product_outright`, so product history starts banking ahead of the cracks phase.
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd

SHEET = "Indications"
_STRIP_RE = re.compile(r"^(Q[1-4]-\d{2}|\d{4})$", re.IGNORECASE)

# (value col, canonical spread, unit, category)
_COLS = [
    (1,  "WTI",          "$/bbl", "crude_outright"),
    (3,  "Brent",        "$/bbl", "crude_outright"),
    (5,  "Dubai",        "$/bbl", "crude_outright"),
    (17, "JCC",          "$/bbl", "crude_outright"),
    (7,  "Sing Kero",    "$/bbl", "product_outright"),
    (13, "Sing Gasoil",  "$/bbl", "product_outright"),
    (15, "Sing Naphtha", "$/bbl", "product_outright"),
    (9,  "Sing 380cst",  "$/mt",  "product_outright"),
    (11, "Sing 180cst",  "$/mt",  "product_outright"),
]


def parse(path: str, obs_date: str) -> list[dict]:
    df = pd.ExcelFile(path).parse(SHEET, header=None)
    rows: list[dict] = []
    m = 0
    for r in range(df.shape[0]):
        c0 = df.iat[r, 0]
        if pd.isna(c0):
            continue
        if isinstance(c0, (pd.Timestamp, datetime)):
            m += 1
            tenor = f"M{m}"
            contract = pd.Timestamp(c0).strftime("%b-%y")
        elif _STRIP_RE.match(str(c0).strip()):
            tenor = contract = str(c0).strip()
        else:
            continue  # label / disclaimer / spot row
        for col, name, unit, cat in _COLS:
            if col >= df.shape[1]:
                continue
            v = df.iat[r, col]
            if pd.isna(v):
                continue
            try:
                v = float(v)
            except (TypeError, ValueError):
                continue
            rows.append({
                "obs_date": obs_date, "source": "MITSUI", "category": cat,
                "spread": name, "tenor": tenor, "contract": contract,
                "value": round(v, 4), "unit": unit,
            })
    return rows

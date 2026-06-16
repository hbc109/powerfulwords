"""Parser for the SC / Sumitomo 'Price Indication' sheet.

Layout (reverse-engineered from the 2026-06-16 upload): a single sheet with
three labelled blocks, each anchored by a label in column 0 and sharing the
same product columns across the row:

    Flat Price    | Brent | Dubai | Dated Brent | WTI | <products…>
    Arb-Cracks    | Brent-Dubai | Dated Dubai | WTI-Brent | WTI-Dubai | <cracks…>
    Time Spreads  | Brent | Dubai | Dated Brent | WTI | <products…>

Each block's data rows are either a month-end date (→ constant-maturity M1, M2,…
in order) or a strip label (3Q26, 4Q26, 1Q27). This parser is column-NAME
driven (not position), so it survives column reordering. v1 = crude only.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

SHEET = "Price Indication"

# Crude columns we keep, per block. Names must match the sheet's header text.
_CRUDE_OUTRIGHTS = {"Brent", "Dubai", "Dated Brent", "WTI"}
_CRUDE_DIFFS = {"Brent-Dubai", "Dated Dubai", "WTI-Brent", "WTI-Dubai"}

# (anchor label, category, allowed column names, spread-name suffix)
_BLOCKS = [
    ("Flat Price", "crude_outright", _CRUDE_OUTRIGHTS, ""),
    ("Arb-Cracks", "crude_diff", _CRUDE_DIFFS, ""),
    ("Time Spreads", "crude_timespread", _CRUDE_OUTRIGHTS, " cal"),
]
_ANCHOR_LABELS = {b[0].lower() for b in _BLOCKS}


def _norm(v) -> str:
    return str(v).strip()


def parse(path: str, obs_date: str) -> list[dict]:
    df = pd.ExcelFile(path).parse(SHEET, header=None)
    nrows = df.shape[0]

    # locate each block's anchor row
    anchors: dict[str, int] = {}
    for r in range(nrows):
        v = df.iat[r, 0]
        if pd.notna(v) and _norm(v).lower() in _ANCHOR_LABELS:
            for label, *_ in _BLOCKS:
                if _norm(v).lower() == label.lower():
                    anchors[label] = r

    rows: list[dict] = []
    anchor_rows = sorted(anchors.values())
    for label, category, allowed, suffix in _BLOCKS:
        if label not in anchors:
            continue
        hr = anchors[label]
        # header: product names on the anchor row, cols 1..N
        header = {col: _norm(df.iat[hr, col])
                  for col in range(1, df.shape[1])
                  if pd.notna(df.iat[hr, col]) and _norm(df.iat[hr, col]) in allowed}
        if not header:
            continue
        stop = next((a for a in anchor_rows if a > hr), nrows)

        m_idx = 0
        for r in range(hr + 1, stop):
            c0 = df.iat[r, 0]
            if pd.isna(c0):
                continue
            if isinstance(c0, (pd.Timestamp, datetime)):
                m_idx += 1
                tenor = f"M{m_idx}"
                contract = pd.Timestamp(c0).strftime("%b-%y")
            else:
                lbl = _norm(c0)
                if lbl.lower() in _ANCHOR_LABELS:
                    continue
                tenor = contract = lbl  # strip: 3Q26, 4Q26, …

            for col, name in header.items():
                val = df.iat[r, col]
                if pd.isna(val):
                    continue
                try:
                    val = float(val)
                except (TypeError, ValueError):
                    continue
                rows.append({
                    "obs_date": obs_date, "source": "SC", "category": category,
                    "spread": f"{name}{suffix}", "tenor": tenor, "contract": contract,
                    "value": round(val, 4), "unit": "$/bbl",
                })
    return rows

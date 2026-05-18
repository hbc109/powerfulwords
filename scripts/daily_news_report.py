"""Simple plain-prose daily oil news report.

Two sections: today's headlines (with short excerpts) and latest
EIA + JODI inventory numbers. No scores, no signal direction.

Output: data/processed/digests/daily_news_<YYYY-MM-DD>.md
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db.database import get_connection

OUT_DIR = BASE_DIR / "data" / "processed" / "digests"


def _excerpt(text: str, n: int = 240) -> str:
    if not text:
        return ""
    t = " ".join(text.split())
    return t[:n] + ("…" if len(t) > n else "")


def _fmt_kbbl(n) -> str:
    try:
        return f"{int(round(float(n))):,} kbbl"
    except Exception:
        return "—"


def _latest_two(conn, symbol: str, asof: date):
    """Most recent two prices on or before asof for WoW change."""
    rows = conn.execute(
        "SELECT price_time, close FROM market_prices WHERE symbol=? AND price_time<=? "
        "AND close IS NOT NULL ORDER BY price_time DESC LIMIT 2",
        (symbol, asof.isoformat()),
    ).fetchall()
    return rows


def render(asof: date, conn: sqlite3.Connection) -> str:
    parts = [f"# Daily Oil Report — {asof.isoformat()}\n"]

    # Headlines
    headlines = conn.execute(
        "SELECT source_id, title, raw_text FROM documents "
        "WHERE date(published_at)=? AND title IS NOT NULL "
        "ORDER BY quality_tier DESC, length(raw_text) DESC LIMIT 15",
        (asof.isoformat(),),
    ).fetchall()
    parts.append("## Headlines\n")
    if not headlines:
        parts.append("_No documents published today._\n")
    else:
        for sid, title, raw in headlines:
            parts.append(f"- **{title}** _({sid})_")
            ex = _excerpt(raw)
            if ex:
                parts.append(f"  > {ex}")
        parts.append("")

    # Inventory
    parts.append("## Inventory (latest available)\n")
    series = [
        ("EIA_CRUDE_STOCKS",     "US crude (excl SPR)"),
        ("EIA_CUSHING_STOCKS",   "Cushing crude"),
        ("EIA_GASOLINE_STOCKS",  "US gasoline"),
        ("EIA_DISTILLATE_STOCKS","US distillate"),
        ("JODI_OECD_CRUDE_STOCKS","OECD basket crude (JODI, monthly)"),
    ]
    any_inv = False
    for sym, label in series:
        rows = _latest_two(conn, sym, asof)
        if not rows:
            continue
        any_inv = True
        latest_d, latest_v = rows[0]
        if len(rows) > 1 and rows[1][1] is not None:
            chg = latest_v - rows[1][1]
            chg_str = f" ({'+' if chg >= 0 else ''}{int(round(chg)):,} kbbl vs prior)"
        else:
            chg_str = ""
        parts.append(f"- **{label}** ({latest_d[:10]}): {_fmt_kbbl(latest_v)}{chg_str}")
    if not any_inv:
        parts.append("_No inventory data available._")
    parts.append("")
    return "\n".join(parts)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    asof = date.fromisoformat(args.date) if args.date else date.today()
    conn = get_connection()
    md = render(asof, conn)
    conn.close()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"daily_news_{asof.isoformat()}.md"
    out.write_text(md, encoding="utf-8")
    print(f"Wrote {out.relative_to(BASE_DIR)} ({len(md):,} chars)")


if __name__ == "__main__":
    main()

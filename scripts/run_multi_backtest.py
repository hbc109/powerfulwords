"""Run a multi-book backtest using app/config/multi_strategy_config.json.

Each book defines its own instrument (outright or spread), capital, and
narrative scoring config. All books share the same daily narrative
scores; per-book equity curves are produced and a portfolio-level
aggregate is summed across books.

Outputs:
  data/processed/backtests/multi_backtest_<commodity>.json
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json

from app.db.database import get_connection
from app.strategy.multi_book_backtest import load_multi_strategy_config, run_multi_book


def fetch_subtheme_scores(conn, commodity: str = "crude_oil"):
    cur = conn.execute(
        '''
        SELECT score_date, commodity, topic, narrative_score
        FROM daily_narrative_scores
        WHERE commodity = ?
        ORDER BY score_date, topic
        ''',
        (commodity,),
    )
    return [
        {"score_date": r[0], "commodity": r[1], "topic": r[2], "narrative_score": float(r[3])}
        for r in cur.fetchall()
    ]


def fetch_theme_scores(conn, commodity: str = "crude_oil"):
    cur = conn.execute(
        '''
        SELECT score_date, commodity, theme, narrative_score
        FROM daily_theme_scores
        WHERE commodity = ?
        ORDER BY score_date, theme
        ''',
        (commodity,),
    )
    return [
        {"score_date": r[0], "commodity": r[1], "theme": r[2], "narrative_score": float(r[3])}
        for r in cur.fetchall()
    ]


def fetch_all_prices(conn):
    cur = conn.execute(
        '''
        SELECT price_time, symbol, asset_type, open, high, low, close, volume
        FROM market_prices
        ORDER BY price_time, symbol
        '''
    )
    return [
        {
            "price_time": r[0], "symbol": r[1], "asset_type": r[2],
            "open": r[3], "high": r[4], "low": r[5], "close": r[6], "volume": r[7],
        }
        for r in cur.fetchall()
    ]


def main() -> None:
    cfg = load_multi_strategy_config()

    # If any book uses themes, we need theme rows; otherwise subtheme rows.
    needs_themes = any(
        bool((b.get("scoring") or {}).get("use_themes", False)) for b in cfg.get("books", [])
    )

    conn = get_connection()
    if needs_themes:
        scores = fetch_theme_scores(conn, commodity=cfg["commodity"])
        # Books that don't use themes still need topic data; load both for safety.
        if not all(bool((b.get("scoring") or {}).get("use_themes", False)) for b in cfg["books"]):
            print("Mixed scoring modes detected; falling back to per-book load is not yet supported.")
            print("All books in this config use themes — proceeding.")
    else:
        scores = fetch_subtheme_scores(conn, commodity=cfg["commodity"])
    prices = fetch_all_prices(conn)
    conn.close()

    result = run_multi_book(cfg, scores, prices)

    out_dir = BASE_DIR / "data" / "processed" / "backtests"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'multi_backtest_{cfg["commodity"]}.json'
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote multi-book backtest to {out_path}")
    print()
    print("Portfolio summary:")
    print(json.dumps(
        {k: v for k, v in result["portfolio"].items() if k != "portfolio_curve"},
        ensure_ascii=False, indent=2,
    ))
    print()
    print("Per-book summary:")
    for b in result["books"]:
        print(f"  {b['name']:>22}  {b['summary']}")


if __name__ == "__main__":
    main()

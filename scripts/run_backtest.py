from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json
from pathlib import Path

from app.db.database import get_connection
from app.strategy.backtest_engine import load_strategy_config, run_daily_backtest

BASE_DIR = Path(__file__).resolve().parents[1]


def fetch_scores(conn, commodity: str = "crude_oil"):
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
        {
            "score_date": r[0],
            "commodity": r[1],
            "topic": r[2],
            "narrative_score": float(r[3]),
        }
        for r in cur.fetchall()
    ]


def fetch_prices(conn, symbol: str):
    cur = conn.execute(
        '''
        SELECT price_time, symbol, asset_type, open, high, low, close, volume
        FROM market_prices
        WHERE symbol = ?
        ORDER BY price_time
        ''',
        (symbol,),
    )
    return [
        {
            "price_time": r[0],
            "symbol": r[1],
            "asset_type": r[2],
            "open": r[3],
            "high": r[4],
            "low": r[5],
            "close": r[6],
            "volume": r[7],
        }
        for r in cur.fetchall()
    ]


def main():
    cfg = load_strategy_config()
    conn = get_connection()
    scores = fetch_scores(conn, commodity=cfg["commodity"])
    prices = fetch_prices(conn, symbol=cfg["symbol"])
    conn.close()

    result = run_daily_backtest(scores, prices, cfg)

    out_dir = BASE_DIR / "data" / "processed" / "backtests"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f'backtest_{cfg["commodity"]}_{cfg["symbol"]}.json'
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote backtest to {out_path}")
    print("Summary:")
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

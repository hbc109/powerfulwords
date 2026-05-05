from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import argparse
import json
from pathlib import Path

from app.db.database import get_connection
from app.research.event_study import run_event_study, run_conditional_event_study

BASE_DIR = Path(__file__).resolve().parents[1]


def fetch_scores(conn, commodity: str = "crude_oil"):
    cur = conn.execute(
        '''
        SELECT score_date, commodity, topic, narrative_score, official_confirmation_score,
               news_breadth_score, chatter_score, crowding_score
        FROM daily_narrative_scores
        WHERE commodity = ?
        ORDER BY score_date, topic
        ''',
        (commodity,),
    )
    rows = []
    for r in cur.fetchall():
        rows.append(
            {
                "score_date": r[0],
                "commodity": r[1],
                "topic": r[2],
                "narrative_score": float(r[3]),
                "official_confirmation_score": r[4],
                "news_breadth_score": r[5],
                "chatter_score": r[6],
                "crowding_score": r[7],
            }
        )
    return rows


def fetch_regimes(conn, symbol: str):
    cur = conn.execute(
        '''
        SELECT regime_date, primary_regime, regime_streak
        FROM daily_regimes WHERE symbol = ? ORDER BY regime_date
        ''',
        (symbol,),
    )
    return [
        {"regime_date": r[0], "primary_regime": r[1], "regime_streak": r[2]}
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
    rows = []
    for r in cur.fetchall():
        rows.append(
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
        )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="WTI")
    parser.add_argument("--commodity", default="crude_oil")
    parser.add_argument("--horizons", default="1,3,5,10")
    args = parser.parse_args()

    horizons = [int(x.strip()) for x in args.horizons.split(",") if x.strip()]

    conn = get_connection()
    scores = fetch_scores(conn, commodity=args.commodity)
    prices = fetch_prices(conn, symbol=args.symbol)
    regimes = fetch_regimes(conn, symbol=args.symbol)
    conn.close()

    result = run_event_study(scores, prices, horizons)
    if regimes:
        result["conditional"] = run_conditional_event_study(
            scores, prices, regimes, horizons
        )
        result["conditional_streak_3plus"] = run_conditional_event_study(
            scores, prices, regimes, horizons, streak_min=3
        )

    out_dir = BASE_DIR / "data" / "processed" / "research"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"event_study_{args.commodity}_{args.symbol}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote event study to {out_path}")
    print(f"Samples: {result['sample_size']}")
    print("Bucket summary:")
    for k, v in result["bucket_summary"].items():
        print(k, v)
    if "conditional" in result:
        print(f"\nRegime distribution: {result['conditional']['regime_distribution']}")


if __name__ == "__main__":
    main()

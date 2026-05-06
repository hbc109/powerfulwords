"""Test the initial three strategy hypotheses against history.

H1  fade        bullish narrative  +  stretched_up regime         -> SHORT
H2  trend       bullish narrative  +  trend_up + breadth>=0.67    -> LONG
H3  bear-conf   bearish narrative  +  trend_down + breadth>=0.67  -> SHORT

Run for WTI / Brent / RBOB / ULSD. Output a ranked summary so we can
see which hypotheses survive at acceptable sample sizes.
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json

from app.db.database import get_connection
from app.research.event_study import _load_thresholds
from app.research.hypothesis import (
    Hypothesis, evaluate_hypothesis, fetch_hypothesis_universe, fetch_prices,
)

SYMBOLS = ["WTI", "Brent", "RBOB_BBL", "ULSD_BBL"]
HORIZONS = [1, 3, 5, 10]
TH = _load_thresholds()


def make_hypotheses() -> list[Hypothesis]:
    return [
        Hypothesis(
            name="H1_fade_bullish_at_stretched",
            description=(
                "Bullish narrative score in a stretched_up price regime. "
                "Hypothesis: chatter clusters at tops; fade for 5d."
            ),
            direction="short",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and r["primary_regime"] == "stretched_up"
            ),
        ),
        Hypothesis(
            name="H2_trend_confirm_bullish",
            description=(
                "Bullish narrative + trend_up regime + cross-product "
                "agreement >= 0.67. News drift in confirmed trend."
            ),
            direction="long",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and r["primary_regime"] == "trend_up"
                and (r["cross_product_agreement"] or 0) >= 0.67
            ),
        ),
        Hypothesis(
            name="H3_bear_conviction",
            description=(
                "Bearish narrative + trend_down regime + cross-product "
                "agreement >= 0.67. Consensus bear in confirmed downtrend."
            ),
            direction="short",
            rule=lambda r: (
                float(r["narrative_score"] or 0) <= TH["short"]
                and r["primary_regime"] == "trend_down"
                and (r["cross_product_agreement"] or 0) >= 0.67
            ),
        ),
    ]


def fmt_row(name: str, sym: str, n_dates: int, h_5d: dict | None) -> str:
    if not h_5d or h_5d.get("count") == 0:
        return f"  {sym:<10}  unique_dates={n_dates:>4}  5d: insufficient forward data"
    cnt = h_5d["count"]
    hit = h_5d["hit_rate"]
    ret = h_5d["avg_fwd_ret"]
    flag = " <- low N (treat as suggestive)" if cnt < 30 else ""
    return (
        f"  {sym:<10}  unique_dates={n_dates:>4}  "
        f"5d:  hit={hit:>5.0%}  avg_ret={ret:+.2%}{flag}"
    )


def main() -> None:
    conn = get_connection()
    hypotheses = make_hypotheses()
    print(f"Thresholds (from strategy_config.json): "
          f"long={TH['long']:+.2f}  short={TH['short']:+.2f}\n")
    all_results = []
    for h in hypotheses:
        print(f"\n=== {h.name} ===")
        print(f"  rule:      {h.description}")
        print(f"  direction: {h.direction}")
        print()
        for sym in SYMBOLS:
            universe = fetch_hypothesis_universe(conn, sym)
            prices = fetch_prices(conn, sym)
            if not universe or not prices:
                print(f"  {sym}: no universe / prices")
                continue
            res = evaluate_hypothesis(h, universe, prices, HORIZONS)
            res["symbol"] = sym
            all_results.append(res)
            print(fmt_row(h.name, sym, res["unique_dates"],
                          res["by_horizon"].get(5)))
    conn.close()

    # Persist for the dashboard
    out_dir = BASE_DIR / "data" / "processed" / "research"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "strategy_hypotheses.json"
    out_path.write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()

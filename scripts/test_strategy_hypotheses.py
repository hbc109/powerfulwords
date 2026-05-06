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
    """Initial hypothesis menu covering the major archetypes:
       sentiment fade, trend confirmation (news drift), volume confirm,
       source divergence (chatter-leads-officials), breadth divergence,
       range-regime mean reversion.
    """
    return [
        # ---- Sentiment fade (mean reversion at extremes) ----
        Hypothesis(
            name="H1_fade_bullish_at_stretched",
            description="Bullish narrative + stretched_up regime → fade. Chatter clusters at tops.",
            direction="short",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and r["primary_regime"] == "stretched_up"
            ),
        ),
        Hypothesis(
            name="H1b_fade_bearish_at_stretched_down",
            description="Bearish narrative + stretched_down regime → fade long. Mirror of H1; chatter clusters at bottoms.",
            direction="long",
            rule=lambda r: (
                float(r["narrative_score"] or 0) <= TH["short"]
                and r["primary_regime"] == "stretched_down"
            ),
        ),

        # ---- Trend confirmation (news drift) ----
        Hypothesis(
            name="H2_trend_confirm_bullish",
            description="Bullish narrative + trend_up + xprod ≥ 0.67. News drift in confirmed uptrend.",
            direction="long",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and r["primary_regime"] == "trend_up"
                and (r["cross_product_agreement"] or 0) >= 0.67
            ),
        ),
        Hypothesis(
            name="H3_bear_conviction",
            description="Bearish narrative + trend_down + xprod ≥ 0.67. Consensus bear in confirmed downtrend.",
            direction="short",
            rule=lambda r: (
                float(r["narrative_score"] or 0) <= TH["short"]
                and r["primary_regime"] == "trend_down"
                and (r["cross_product_agreement"] or 0) >= 0.67
            ),
        ),

        # ---- Volume confirmation ----
        Hypothesis(
            name="H4_volume_confirm_bullish",
            description="Bullish narrative + trend_up + volume ≥ 1.5×20d. Real participation behind the move.",
            direction="long",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and r["primary_regime"] == "trend_up"
                and (r["volume_ratio"] or 0) >= 1.5
            ),
        ),
        Hypothesis(
            name="H5_low_volume_rally_fade",
            description="Bullish narrative + trend_up + volume < 0.7×20d. Weak rally; fade.",
            direction="short",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and r["primary_regime"] == "trend_up"
                and 0 < (r["volume_ratio"] or 0) < 0.7
            ),
        ),

        # ---- Source divergence (chatter leads officials) ----
        Hypothesis(
            name="H6_chatter_leads_bullish",
            description=(
                "Bullish narrative + source_divergence > 0.4 + chatter_score > 0.5. "
                "Chatter loud, officials silent — chatter often leads."
            ),
            direction="long",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and (r["source_divergence"] or 0) > 0.4
                and (r["chatter_score"] or 0) > 0.5
            ),
        ),
        Hypothesis(
            name="H7_chatter_leads_bearish",
            description=(
                "Bearish narrative + source_divergence > 0.4 + chatter_score > 0.5. "
                "Mirror of H6 on the bearish side."
            ),
            direction="short",
            rule=lambda r: (
                float(r["narrative_score"] or 0) <= TH["short"]
                and (r["source_divergence"] or 0) > 0.4
                and (r["chatter_score"] or 0) > 0.5
            ),
        ),

        # ---- Breadth divergence ----
        Hypothesis(
            name="H8_lone_outlier_fade",
            description=(
                "Bullish narrative + symbol is lone outlier in regime "
                "(xprod ≤ 0.33). Likely false breakout; fade."
            ),
            direction="short",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and (r["cross_product_agreement"] is not None
                     and r["cross_product_agreement"] <= 0.33)
            ),
        ),
        Hypothesis(
            name="H9_full_breadth_confirm",
            description="Bullish narrative + full cross-product agreement (xprod = 1.0). Consensus uptrend across the complex.",
            direction="long",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and (r["cross_product_agreement"] or 0) >= 1.0
            ),
        ),

        # ---- Range-regime mean reversion ----
        Hypothesis(
            name="H10_range_overbought_fade",
            description=(
                "Bullish narrative + range regime + RSI > 65. "
                "In chop with no trend, extreme RSI mean-reverts."
            ),
            direction="short",
            rule=lambda r: (
                float(r["narrative_score"] or 0) >= TH["long"]
                and r["primary_regime"] == "range"
                and (r["rsi14"] or 0) > 65
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

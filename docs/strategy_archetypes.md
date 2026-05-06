# Strategy Archetypes — Reference

Trading does not have proven theories the way physics has proven laws. This
file lists the major **archetypes** that carry varying degrees of academic
and practical support, so we can place any strategy we design in context.

We're currently building a **news drift + sentiment fade conditioned on
regime** combination strategy. The other archetypes are kept here as a
menu for future experiments.

| Archetype | Core idea | Empirical support | Where it works | Where it fails |
|---|---|---|---|---|
| **Momentum** | Winners keep winning over 1-12 month windows | Strong (Jegadeesh-Titman 1993, AQR factor research) — works across most asset classes | Trending regimes | Regime breaks; mean-reverting environments |
| **Mean reversion at extremes** | Overbought/oversold returns to mean | Solid in chop, well-known retail strategy | Range / consolidation regimes | Strong trends (gets crushed) |
| **News drift / PEAD** | Markets underreact to news; prices drift 2-5d | Strong for equities (Bernard-Thomas, post-earnings announcement drift). Weaker for commodities | Right after a news catalyst | Already-priced news; long horizon |
| **Sentiment fade** | When sentiment is most extreme, fade it | Moderate (de Bondt-Thaler 1985, Baker-Wurgler) — notoriously hard to time | Sentiment extremes (positioning, narrative) | Mid-cycle phases |
| **Cross-sectional value** | Buy cheap, sell expensive within a basket | Strong for equities (Fama-French) — weaker premium for last 15 years | Long-horizon equity baskets | Single-asset / commodity strategies |
| **Carry / term structure** | Earn time-decay or yield differentials | Strong theoretical grounding (factor models) — premia time-varying and noisy | Stable regimes; relative-value | Crisis periods (carry crashes) |
| **Risk premium / vol selling** | Sell insurance, harvest premium | Long-stretch profitable, well-documented | Calm markets | Tail events (catastrophic blowups) |
| **Information arbitrage** | React to news/data faster than market reprices | High-frequency or institutional only — speed-dependent | When the firm has true latency or processing edge | Already-fast markets |
| **Liquidity provision** | Get paid for stepping in when others are forced out | Empirically robust for market makers and HFTs | Fragmented markets, illiquid corners | Crisis (funding squeeze) |

## Where our system fits

The narrative + regime pipeline is closest to a **news drift + sentiment
fade + regime conditioning** hybrid:

- **News drift** — markets underreact to information; bullish reports
  before a 2-5 day price drift up.
- **Sentiment fade** — extreme bullish chatter clusters at tops; fade it.
- **Regime conditioning** — which of the two effects dominates depends
  on the price regime at the time. In a clean trend, drift dominates. At
  stretched extremes with positioning crowded, fade dominates.

This is *not* a single named academic theory. The closest literature is
work combining textual sentiment analysis with technical features
(Tetlock 2007 on news sentiment; Garcia 2013 on news + sentiment;
Heston-Sinha 2017 on news momentum vs reversal).

## How we use this list

When designing a new strategy or interpreting a finding:
1. Identify which archetype the strategy is closest to.
2. Note where the archetype usually fails — that's the first place to
   look for tail risk and likely failure modes.
3. Build the hypothesis as a falsifiable rule, test it on the conditional
   event study.

The archetype is a prior, not a guarantee. The data still has to support
the hypothesis.

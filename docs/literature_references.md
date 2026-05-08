# Literature References

The trading archetypes our hypotheses test against come from a real
academic literature. **None of these papers are reproduced in our code**
— we use them as priors that justify which patterns are worth testing
on our data. This file lays out the heritage and notes the gap between
"what the paper prescribes" and "what we built."

See [strategy_archetypes.md](strategy_archetypes.md) for the
trading-strategy taxonomy these papers map to.

---

## How to read this

Each section has:
- **Seminal papers** — the canonical references for the archetype
- **What we use** — our actual implementation choice
- **What's prescribed** — what a paper-faithful implementation would be
- **Gap** — how rigorous we are vs the published methodology

The gap matters because any "this works" finding from our
hypothesis tester could be over-fit to our hand-rolled choices, not
the underlying phenomenon. The path to more rigor is to swap
hand-rolled pieces for paper-prescribed pieces and re-run.

---

## 1. Momentum / Trend persistence

| | |
|---|---|
| **Seminal papers** | Jegadeesh & Titman 1993 (JoF) — *"Returns to Buying Winners and Selling Losers"* (12-month lookback, 3-month hold). Carhart 1997 (JoF) — added momentum factor. **Asness, Moskowitz, Pedersen 2013 (JoF)** — *"Value and Momentum Everywhere"* — works across 8 asset classes, hedges with value. Daniel & Moskowitz 2016 (JFE) — *"Momentum Crashes"* — fails badly in regime breaks. |
| **What we use** | `trend_up` / `trend_down` regime tags via ADX + SMA50 slope. Hypotheses H2/H3 use 5-day forward window. |
| **What's prescribed** | Cross-sectional momentum: rank assets by trailing 12-month return, long top decile / short bottom. Holding period 1-3 months. |
| **Gap** | We don't compute momentum as a return-rank factor — we use ADX/slope as a *regime* indicator. Different operationalization but same intuition. |

---

## 2. Mean reversion / Overreaction

| | |
|---|---|
| **Seminal papers** | De Bondt & Thaler 1985 (JoF) — *"Does the Stock Market Overreact?"* — long-horizon (3-5 yr) reversal in extreme winners/losers. Lehmann 1990 (QJE) — short-term (weekly) reversal. Lo & MacKinlay 1988 (RFS) — random-walk hypothesis fails at short horizons. |
| **What we use** | `stretched_up` / `stretched_down` tags via RSI > 75 or %B > 1.0. Hypothesis H1/H1b uses 5-day fade. |
| **What's prescribed** | Short-term: weekly returns, fade extreme deciles. Long-term: 3-5 year holding for proper overreaction trade. |
| **Gap** | Our 5-day window sits between Lehmann's 1-week (short-term) and de Bondt's 3-5yr (long-term). Reasonable for commodities (faster cycles than stocks) but not directly cited. |

---

## 3. News drift / Post-Earnings Announcement Drift (PEAD)

| | |
|---|---|
| **Seminal papers** | **Bernard & Thomas 1989 (JAR)** — *"Post-Earnings-Announcement Drift"* — most-replicated finance anomaly. **Tetlock 2007 (JoF)** — *"Giving Content to Investor Sentiment"* — first text-sentiment paper using WSJ "Abreast of the Market". **Tetlock, Saar-Tsechansky, Macskassy 2008 (JoF)** — *"More Than Words"* — firm-specific news predicting earnings + returns. Heston & Sinha 2017 (FAJ) — momentum vs reversal in news. |
| **What we use** | Daily narrative scores from the rule-based extractor + 5-day forward window. Hypothesis H2 maps directly to news drift. |
| **What's prescribed** | Tetlock used General Inquirer Harvard IV-4 dictionary; Loughran-McDonald (2011) is the modern finance-specific replacement. Decay typically 2-5 days for news drift. |
| **Gap** | Our keyword rules in `oil_topic_rules.json` are hand-coded, not LM. Likely under-counts subtle bearish framings and over-counts mentions of "weak" / "loss" / "decline" in non-bearish contexts. Migrating to LM is the **single highest-ROI rigor upgrade**. |

---

## 4. Sentiment / Investor mood

| | |
|---|---|
| **Seminal papers** | **Baker & Wurgler 2006 (JoF)** — *"Investor Sentiment and the Cross-Section of Stock Returns"* — composite sentiment index. Stambaugh, Yu, Yuan 2012 (JFE) — sentiment + short-sale constraints. Bouchaud, Krueger, Landier, Thesmar 2019 (JoF) — sticky expectations. |
| **What we use** | Source-bucket weights (chatter < institutional < official). `chatter_score` and `source_divergence` fields per topic-day. |
| **What's prescribed** | Baker-Wurgler index uses 6 macro inputs (closed-end fund discount, NYSE turnover, IPO volume, IPO first-day return, equity share, dividend premium). Constructed as PCA. |
| **Gap** | Our `chatter_score` is a fraction of events from social buckets — not a B-W-style PCA across multiple sentiment proxies. Could enrich. |

---

## 5. Volume confirmation

| | |
|---|---|
| **Seminal papers** | Lee & Swaminathan 2000 (JoF) — *"Price Momentum and Trading Volume"* — high-volume winners do better, low-volume winners reverse. Llorente, Michaely, Saar, Wang 2002 (RFS) — volume as info signal. |
| **What we use** | `volume_ratio` (today / 20d mean). Hypotheses H4 (high-vol confirm) and H5 (low-vol fade). |
| **What's prescribed** | Lee-Swaminathan use volume turnover (volume / shares outstanding) — a normalized metric. We use raw daily volume / 20-day mean which is similar in spirit. |
| **Gap** | Close enough; volume normalization is comparable. |

---

## 6. Carry / Term structure

| | |
|---|---|
| **Seminal papers** | Koijen, Moskowitz, Pedersen, Vrugt 2018 (JFE) — *"Carry"* — universal across asset classes. Lustig & Verdelhan 2007 (AER) — currency carry. **Erb & Harvey 2006 (FAJ)** — *"The Strategic and Tactical Value of Commodity Futures"* — curve shape (backwardation/contango) is dominant return driver in commodities. **Fama & French 1987 (JoB)** — basis predicts spot. |
| **What we use** | **Nothing yet.** No futures-curve data, no carry signal. |
| **What's prescribed** | Backwardation = positive carry → tactical long. Contango = negative carry → reduce long. |
| **Gap** | Big. Adding curve data (front vs second-month spread for WTI/Brent) would unlock the most empirically robust commodity strategy. Future work. |

---

## 7. Volatility / Risk premium

| | |
|---|---|
| **Seminal papers** | Bollerslev, Tauchen, Zhou 2009 (RFS) — variance risk premium. Whaley — VIX. Bondarenko 2014 — pricing of variance risk. |
| **What we use** | `shock` regime tag (ATR ratio > 1.5) — vol-spike detection. No vol-premium harvesting. |
| **What's prescribed** | Sell variance during low vol-of-vol regimes, buy it back when realized exceeds implied. |
| **Gap** | We detect vol regimes but don't trade vol-premium. Out of scope for now. |

---

## 8. Cross-sectional value (commodities-relevant)

| | |
|---|---|
| **Seminal papers** | Fama & French 1992/1993 — value premium foundation. Asness, Moskowitz, Pedersen 2013 — value works in commodities. **Gorton & Rouwenhorst 2006 (FAJ)** — long-only commodity returns ≈ stocks but very different correlations. **Cheng & Xiong 2014 (ARFE)** — *"Financialization of Commodity Markets"* — institutional inflows changed dynamics post-2004. |
| **What we use** | Nothing. We're single-commodity (oil). No cross-sectional value play. |
| **What's prescribed** | Rank commodity sectors by 5-yr-mean-deviation, long undervalued / short overvalued. |
| **Gap** | Out of scope until we expand beyond crude. |

---

## 9. Text / narrative strategies (most relevant to what we built)

| | |
|---|---|
| **Seminal papers** | **Tetlock 2007** (cited above). **Loughran & McDonald 2011 (JoF)** — *"When is a Liability not a Liability?"* — the canonical finance lexicon. Available as a free dictionary. **Huang, Lehavy, Zang, Zheng 2018 (MS)** — analyst report tone predicts target price revisions. Garcia 2013 (JoF) — sentiment in financial news during recessions. **Calomiris & Mamaysky 2019 (JFE)** — country-level news topics explain market moves. |
| **What we use** | Rule-based topic extractor with hand-coded keyword lists. Loughran-McDonald is **not used**. |
| **What's prescribed** | Loughran-McDonald has ~6 categories: positive, negative, uncertainty, litigious, modal-strong, modal-weak. Each event scored against the LM dictionary; modifiers (negation, intensity) handled. |
| **Gap** | **Single biggest known shortcut in our system.** Our keyword rules conflate "weakness in the dollar" (bullish for oil) with "weak demand" (bearish). LM has the negation handling and modifier logic to disambiguate. Migrating extract step to LM is the highest-ROI rigor upgrade. |

---

## 10. Information arbitrage / Alternative data

| | |
|---|---|
| **Seminal papers** | Tetlock, Hong, Lim — analyst dispersion + returns. Modern: Engelberg, Reed, Ringgenberg 2012 (JFE) — *"How Are Shorts Informed?"* — short-seller activity as info signal. |
| **What we use** | We collect alternative data (chatter, RSS, manual uploads) but don't trade speed. |
| **What's prescribed** | Sub-second reaction to news / social / data releases. |
| **Gap** | We're a slow-data system on purpose — narrative integration over hours/days, not seconds. Different game. |

---

## Practical takeaways for our system

1. **Highest-impact upgrade**: replace `oil_topic_rules.json` keyword extractor with a Loughran-McDonald-based scorer. Would tighten the news-drift hypotheses materially.
2. **Biggest unused archetype**: carry / term structure. Adding futures-curve data (front-month vs 2nd-month spread) would unlock a separate, well-validated commodity strategy.
3. **Closest match to literature**: H2 (news drift in trend) and H1 (sentiment fade at extremes) — both are direct reads of well-studied phenomena.
4. **Most fragile**: H6/H7 (chatter leads officials) — based on Tetlock-style information-arbitrage but with thresholds we picked (`source_divergence > 0.4`). No paper prescribes those numbers.

---

## Reading order if you want to study this seriously

If new to this literature, the path I'd suggest:

1. **Tetlock 2007** — first text-sentiment paper, sets the foundation for everything we do
2. **Loughran-McDonald 2011** — the lexicon paper; explains why finance text needs a finance-specific dictionary
3. **Bernard & Thomas 1989** — most-replicated anomaly in finance, proves news drift is real
4. **Asness, Moskowitz, Pedersen 2013** — momentum + value across asset classes
5. **Erb & Harvey 2006** — for commodity-specific perspective; explains why curve shape dominates
6. **Cheng & Xiong 2014** — modern context; how financialization changed commodity markets

Most are freely available via SSRN or NBER working-paper sites.

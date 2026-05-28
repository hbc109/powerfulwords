# Strategy versions log

Tracks the trading strategy as it evolves — weights, factors, thresholds,
execution rules — with the date of each change, the metric that motivated
it, and the next-period result. The point is to make iteration honest:
without a written log, it's too easy to forget what we tried, why, and
whether it actually worked.

---

## Where we sit vs pro shops

What we've built sits squarely in the **systematic commodity macro**
lineage — same DNA as CTAs (AHL, Winton, Aspect), commodity macro funds
(Castleton, Trafigura analytics, Glencore's risk book) and sell-side
commodity desks (Goldman, Morgan Stanley). Multi-source ingestion,
factor extraction, regime classification, regime-conditional weighting,
composite signal, backtest with hit-rate / Sharpe / drawdown.

The gap is **not conceptual** — it's discipline, infrastructure, scale:

| Dimension | This system | Pro shop |
|---|---|---|
| Factor count | 4 (narrative, term structure, positioning, inventory) | 50–200 |
| Symbols | 2 (WTI, Brent) | 50–500 across asset classes |
| Data cadence | Daily / weekly | Tick-level + intraday |
| Risk management | None yet | Vol-targeting, Kelly sizing, position limits, factor-exposure caps |
| Execution | Close-to-close, 5bps fixed | Smart routing, slippage models, execution alpha |
| Statistical rigor | Single backtest | Walk-forward, bootstrap CIs, regime-change detection |
| Out-of-sample discipline | About to start | Locked-in, weekly evaluation |
| Team | 1 | 10–100+ |

What actually separates pro shops from "retail multi-factor model"
attempts is rarely a secret factor — it's:

1. **Real statistical discipline** (out-of-sample, multiple-hypothesis
   correction, sample-size requirements before any change).
2. **Proper risk management** that prevents one bad regime from sinking
   the book.
3. **Obsessive attention to execution costs** — slippage usually eats
   more PnL than signal "improvements".
4. **Killing models when the data says they're broken**, instead of
   re-tuning them to recent noise.

Realistic edge for a one-person setup isn't beating them on factor
count — it's leaning into what they *can't* easily do: **fewer markets
much more deeply**, **more concentrated bets** (no LP-aware drawdown
limits), and **qualitative judgment** as a final filter.

---

## Iteration discipline

Rules for changing the strategy after v1:

1. **New data is out-of-sample, not training data.** We tuned weights
   from the 2023–2026 backtest. Do not re-tune on 2026-05-15+ results;
   use them as a *test* of whether tuned weights still work.
2. **Bar for change.** Only re-tune if the rolling 6-month backtest
   underperforms baseline by **>2pp** *and* the gap is consistent
   across multiple regimes *and* a structural explanation exists
   ("EIA stopped publishing X", regime shift, etc.).
3. **Always log it here.** Date, what changed, the metric that
   motivated it, and after at least one month: did it help.
4. **Walk-forward later.** Eventually replace single-shot backtests
   with walk-forward (refit on 2023–2024, test on 2025; refit on
   2024–2025, test on 2026; etc.).
5. **The "do nothing" baseline.** Always compare any proposed change
   to *not* changing. Half the time the right call is to leave it.

---

## Version history

### v1 — 2026-05-14 — initial composite signal

**State at lock-in:**

- **Factors**: narrative (theme z-score), positioning (CFTC COT,
  contrarian, gated past 1σ), inventory (EIA + JODI seasonal-deviation
  z, equal-weight average), term_structure (WTI/Brent M1-M2 spread z) —
  the last is in the dashboard but excluded from backtests because
  historical data is biased deferred-spread proxy.
- **Regime classification**: `app/research/regime.py` — multi-label
  (`trend_up`, `trend_down`, `range`, `stretched_up`, `stretched_down`,
  `shock`), priority-collapsed to a single `primary_regime`.
- **Per-symbol regime weights** in `app/config/strategy_config.json`
  — Brent and WTI tuned independently because their per-regime
  characteristics diverged (Brent's `trend_up` and `stretched_down`
  needed much higher narrative weight; WTI's didn't).
- **Composite formula**: weighted sum of available factors per
  regime, weights renormalized over factors actually present.
- **Position sizing**: linear thresholded — `composite > 0.1 → 1x`,
  `> 0.4 → 2x`, max 2x. No vol-targeting, no Kelly, no stops.
  `one_way_cost = 5bps`.

**Backtest result (2023-05-08 → 2026-05-11, 251 trading days):**

| Symbol | Composite vs narrative-only (5d hit rate) | Composite PnL backtest |
|---|---|---|
| WTI | **+1.5pp** (56.5% vs 55.0%) | +67.8% return, 0.64 Sharpe, −35.5% max DD |
| Brent | −2.4pp (53.4% vs 55.8%) | +3.1% return, 0.21 Sharpe, −46.4% max DD |

**Honest assessment:**

- WTI composite earns its keep on hit-rate and PnL.
- Brent composite is essentially flat; non-narrative factors (US-centric
  inventory, COT) are less leading for Brent than for WTI. Pushing
  narrative weight higher just approaches the narrative-only baseline.
- WTI's max DD of −35.5% is uncomfortable — a real risk-management
  layer would sit above this signal, not be a feature of it.
- Term structure factor not yet in the backtest pipeline; it will
  go in once a roll-aware historical fetcher is built.

**Locked at this commit. Next data (2026-05-15+) is out-of-sample.**

---

## Open questions (revisit later)

- **Are WTI and Brent really the same beast?** The current architecture (one composite, per-symbol regime weights on the same factor set) assumes they respond to the same factors in different proportions. They may actually need different factor *sets* entirely — WTI lives on EIA + Cushing + CFTC, Brent lives on ICE positioning, ARA/Singapore stocks, OPEC+ compliance, freight rates. Our data stack is US-skewed; that's the root cause of Brent's persistent backtest underperformance, not a weight-tuning problem. *Note added 2026-05-18.*

---

## Roadmap — closing the gap to pro shops

What we have today ≈ a simplified, retail-scale version of a quant
commodity fund's **signal layer**. The architecture (multi-factor
composite, regime conditioning, forward-test, backtest) is
recognizable. What's absent is everything *around* the signal:
risk management, execution realism, portfolio construction, deep
alt data, performance attribution.

### Priority order depends on mode

**We are currently in pure paper / forward-test mode** — no real
capital is deployed. The priorities below reflect that. If/when
real money is deployed, **risk management (currently P3) moves to
P1 and becomes mandatory before any trade.** Today, building
elaborate risk infrastructure for a hypothetical real-money future
is exactly the kind of over-engineering this project tries to avoid.

The priorities, ordered by leverage-per-effort *for the current
paper-mode reality*:

### Priority 1 — Spread / curve trading book (new for forward-test mode)

*(Originally listed as P2; promoted to P1 in 2026-05-22 revision.
The case is below in "Priority 2" still — see [Spread / curve trading
section](#priority-2-spread--curve-trading) just below.)*

In short: calendar spreads (WTI M1−M2, Brent M1−M2) have structurally
lower vol, near-zero macro beta, and capture a focused physical-flow
signal that's the natural fit for retail edge. We already store
M1/M2/M3/M6 in `market_prices` and `term_structure_factor` already
z-scores the spread — the bones exist. Building this adds a new
forward-test signal source decorrelated from the existing flat-price
book, which is more useful in paper mode than infrastructure for
hypothetical real-money risk.

### Priority 2 — Cross-asset overlay (was P3)

*(Unchanged from original P3 — see [Cross-asset overlay section](#priority-3-cross-asset-overlay).)*
DXY, S&P, 10y, copper as macro-regime filter. Cheap (free yfinance
data), useful, fits the paper-mode "see what the model would have
done" framing.

### Priority 3 — Risk management (deferred until real capital)

The arguments for vol-targeting, drawdown auto-flat, factor-exposure
caps remain correct **for real money** — they reduce the path-volatility
of any deployed strategy. But for paper trading, what they primarily
affect is the realism of paper PnL — not the validity of the signal
itself. Defer until there's a concrete plan to deploy capital, then
build properly with the benefit of having watched the live signal
behave for months. Original detailed writeup retained below
([Risk management section](#priority-3-risk-management-was-p1-now-deferred)).

### Priority 4 — Alternative data (unchanged)

Paid feeds — ICE Brent COT, ARA, Singapore, Vortexa/Kpler. Only
worth the ongoing cost if real capital is deployed. Same logic as
risk management: defer until the real-money use case materializes.

---

### Detailed writeups (kept below for reference)

The detailed writeups follow in their original P1-P4 numbering so
existing links still work. Refer to the priority shuffle above for
the current ordering.

### Priority 1 — Risk management (was P1, now deferred)

**Why:** Real-world P&L gets crushed by absent risk management more
often than by weak signals. Right now sizing is just `composite > 0.1
→ 1x, > 0.4 → 2x`, no scaling for volatility, no drawdown auto-flat,
no factor-exposure caps. The composite backtest's WTI max drawdown
of −35.5% reflects this — the signal is profitable but the path is
brutal.

**What to add:**
- **Vol-targeted sizing**: scale position size so each trade risks
  the same dollar amount, computed from rolling ATR or realized vol.
  Reduces the "got destroyed in one bad regime" risk.
- **Drawdown auto-flat**: if rolling 20-day P&L drops past −X%, flat
  all positions until recovery. Simple circuit breaker.
- **Per-factor exposure cap**: don't let any single factor (e.g.,
  inventory) drive more than Y% of the composite signal — protects
  against a stale or broken factor sending the model in the wrong
  direction.

**Effort:** A few days of code, no new data. Backtest framework
already exists, just need to wrap the position sizing layer.

**Expected impact:** Doesn't help hit rate, but should improve
Sharpe meaningfully (probably 0.6 → 0.9+ on WTI) and cut max DD by
roughly half. Most importantly, makes the system *deployable* —
right now the signal is interesting but the risk profile isn't.

### Priority 2 — Spread / curve trading

**Why this is the natural fit for retail edge.** Calendar spreads
(M1 − M2) have structural properties that make them dramatically
more retail-friendly than outright flat price:

| Property | Outright | Calendar spread (M1-M2) |
|---|---|---|
| Annualized volatility | ~30-40% | ~10-15% |
| Beta to macro shocks (Fed, $-dollar, equities) | High — both legs of outright trade move together | **Near zero** — same commodity, macro moves cancel |
| Capital required | Full notional × position | Margin offset for paired contracts: often 30-60% less |
| What it captures | Direction of oil prices | **Physical tightness vs. oversupply** — the structural signal |
| Historical Sharpe (well-timed spread trades) | 0.4-0.7 | Often 1.0+ |

The key intuition: a LONG WTI outright trade exposes you to *"Iran
tension drops, oil tanks 5%"* risk. A LONG WTI M1-M2 spread doesn't
care about flat price at all — it only cares about whether the curve
stays in backwardation or rolls into contango. That's a much more
focused signal, and the kind of signal where physical-flow data
(which moves slowly) actually leads price (which moves fast).

**We already have ~80% of the infrastructure:**

| Have | Still need |
|---|---|
| M1, M2, M3, M6 prices in `market_prices` for both WTI and Brent | Compute and store spread series (M1−M2, M1−M3) as their own "symbols" — e.g., `WTI_M1M2_SPREAD` |
| `term_structure_factor` already z-scores the M1-M2 spread | Use it as-is — z-score IS the natural signal for spread trades |
| Composite, regime, backtest, paper-trading engine | A second "book" in `paper_trades` with spread symbols. Composite_score already accepts symbol; add new entries to `regime_factor_weights` |
| Position sizing via thresholds | Re-tune thresholds — spread vol is ~⅓ outright vol, so signal thresholds need to be tighter (probably ±0.05 instead of ±0.10) |

**One real gotcha — the roll.** When the M1 contract expires, "M1-M2"
becomes "(new M1) - (new M2)", which is the *deferred* M2-M3 spread
in old terms. The reported spread changes discontinuously even though
nothing economically happened. The paper-trade book has to handle
this cleanly — either:
- Close spread positions a few days before each roll and reopen on
  the new front contracts (simpler, slight slippage)
- Or carry positions across the roll with a synthetic adjustment to
  the entry price (cleaner PnL accounting, more code)

We'd start with option 1 and only build option 2 if backtest shows
the roll P&L noise is meaningful.

**Effort:** ~1 week. New fetcher changes (already done — just store
the derived spread alongside), spread book in `paper_trades`, dashboard
panel mirroring the existing one, backtest framework adaption.

**Expected impact:** Pure additive — calendar spread P&L is nearly
uncorrelated with flat price P&L, so portfolio Sharpe rises just by
running both books simultaneously. Expect spread book Sharpe ≥ 1.0
on a regime-conditional version of the existing term-structure
signal.

**Why this should NOT be Priority 1 despite the appeal.** Spread
*blowouts* during physical events (sudden contango-to-backwardation
flips, or vice versa) are bigger than typical spread vol — without
vol-targeted sizing in place, they can hurt disproportionately.
Build risk management (P1) first; then a spread book is much safer
to deploy.

### Priority 3 — Cross-asset overlay

**Why:** Oil-in-isolation misses real regime signals. A WTI rally
with DXY ↑ and equities ↓ is a different beast than a WTI rally
with DXY ↓ and equities ↑ (former is supply-driven, latter is
demand/risk-on). At minimum, DXY and S&P-500 give a useful regime
check.

**What to add:**
- Fetch DXY (`DX=F` or `DX-Y.NYB`), S&P-500 (`^GSPC`), 10y yield
  (`^TNX`), copper (`HG=F`) via the existing yfinance fetcher.
- A small "macro regime" classifier (dollar-strong vs weak,
  risk-on vs risk-off) used as a meta-filter or extra factor.
- Optional: veto rules ("don't add to long oil if dollar is
  strengthening past 1σ — supply shock not confirmed by
  cross-asset").

**Effort:** ~1-2 days, free data.

**Expected impact:** Modest standalone, but high leverage as a
filter on the existing composite — should cut some of the WTI
trend_down regime drag without losing the shock-regime wins.

### Priority 4 — Alternative data (most expensive)

**Why:** This is the *real* fix for Brent's persistent
underperformance. The data we lack (ICE Brent COT, ARA stocks,
Singapore stocks, satellite tank imagery, Vortexa/Kpler tanker
flows) is exactly the data that drives Brent's price.

**What to add:**
- ICE Brent COT (paid Bloomberg/Refinitiv) — replace the niche
  NYMEX BZ slice we currently use
- Vortexa or Kpler API for tanker flows — actual floating storage
  and Atlantic-basin movements
- Platts ARA stocks (sub) — European inventory tightness
- Enterprise Singapore weekly stocks (sub) — Asian demand pull

**Effort:** Hours of integration per source + ongoing monthly cost
(maybe $200-1000/mo depending on which you pick).

**Expected impact:** Should bring Brent composite from −2.4pp under
baseline to genuinely additive (+1-3pp). Would also enable physical-
flow trading patterns (storage arbitrage, freight-driven plays).
Only worth it if you commit serious capital to the model.

### Parked factor candidates (revisit on data/capital trigger)

Factors that are conceptually worth adding but blocked on either
reliable free data or on real-money justification:

- **Crude option skew (25-delta put IV − 25-delta call IV)** —
  Genuinely orthogonal to current factors; captures forward-looking
  tail-risk pricing the others miss. Academic evidence in Bates jump
  factor / BIS commodity vol literature suggests some predictive
  value. **Blocked on data**: CME options aren't free at the
  bid/ask level; yfinance option chains for CL=F are spotty;
  CBOE OVX tracks realized vol, not skew. Revisit when (a) a
  reliable free skew source appears, or (b) real capital justifies
  a paid CME/Bloomberg feed. Also some risk of co-linearity with
  positioning (both reflect sentiment) — would need a quick A/B
  test on hit-rate to confirm orthogonal contribution before
  committing.

### What we deliberately won't build

Some "complete trading system" features are appropriate for an
institutional desk but actively wrong for a retail-scale one-person
setup:

- **High-frequency / intraday signals** — execution alpha at <1s
  scale requires infrastructure we don't have
- **Compliance / regulatory reporting** — only needed past
  CFTC large-trader thresholds (∼$1M+ notional positions)
- **Multi-strategy orchestration** — meaningful only when running
  3+ separate signal families
- **Optionality / vol-surface trading** — requires options data
  pipeline + Black-Scholes-aware risk system; pure flat price stays
  simpler

The discipline here is *don't build pro features unless they
actually help at your scale*.

---

### Template — copy below for the next change

```
### vN — YYYY-MM-DD — short title

**Motivation:** what metric / observation triggered this change.
Cite the file/script/backtest run.

**Change:** what specifically changed (config diff, weight diff,
new factor, etc.).

**Hypothesis:** why we expect this to help. Be specific.

**Acceptance criterion:** what numeric result one month from now
would tell us this worked. Specify per-symbol / per-regime if relevant.

**Result (filled in 1+ month later):** did the criterion hold? If no,
revert and note what we learned. If yes, lock and move on.
```

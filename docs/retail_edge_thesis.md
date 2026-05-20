# Retail edge thesis — how to actually use this system

A working theory of where edge comes from for a one-person retail
oil trader, and what that means for how the composite signal,
paper trading, and daily reports in this repo should be used.

Sister of [strategy_versions.md](strategy_versions.md) — that doc
covers *what we built and where it's going*; this doc covers
*how to use it given who we are*.

---

## The honest starting point

What we built (composite signal, regime classifier, factor stack,
backtest, paper trading) is recognizable to a professional
systematic commodity desk. **It is not, on its own, an edge.** Pros
have everything we have plus 50× more factors, alt data we can't
afford, paid positioning feeds, execution alpha, vol-surface trading,
risk-warehoused dealer flow, and ten people watching the screens.

So the question is: what can a retail oil trader actually beat them
at? The answer is **not "have better signals"**. It's **"do things
they structurally can't do, with the same signals."**

---

## Five real edges available to retail

| Edge | Why it works for retail | Why pros structurally can't replicate |
|---|---|---|
| **Patience / "do nothing"** | You can sit flat for weeks waiting for a clean setup. | Pros must put capital to work — LP fees + career risk forces activity. |
| **Long holding period** | Hold a 6-month structural thesis through interim drawdowns. | Monthly mark-to-market + redemption windows force shorter horizons. |
| **Concentration** | Put 50%+ of capital behind one high-conviction view. | Risk officers / position limits cap pros at 5–10% per position. |
| **Focus depth** | Know oil deeply, ignore other asset classes. | Pros must cover hundreds of markets to deploy capital at scale. |
| **Tail-regime willingness** | Lean *into* shock regimes (COVID, Russia invasion, 2008 collapse). | Margin calls + drawdown limits force pros to flat in tails. |

Notice every one of these is about doing **less, but harder**. Not
about better signals. The retail playbook is fewer trades, in fewer
markets, with more conviction, held longer, sized bigger when the
setup is clean.

---

## The awkward problem: our system is pro-shaped

The composite signal produces a directional read **every day** for
both WTI and Brent, with entry threshold at ±0.10. That cadence is
correct for a pro book trading constantly — wrong for a retail
trader whose edge requires patience.

If we use the system at face value (LONG/SHORT every time |composite|
> 0.10), we end up trading 200+ times a year on a system that doesn't
have informational edge — i.e., we paid the transaction costs of a
pro shop without having any of their advantages. **That's the worst
outcome.**

---

## Five usage shifts that convert this system to retail-edge

These are tweaks to *how the system is used*, not the system itself.

### 1. Raise the activation threshold dramatically

- **Current**: composite > 0.10 → 1x; > 0.40 → 2x; <0.10 → flat.
- **Retail**: composite > 0.60 → 1x; > 0.90 → 2x; otherwise flat.

Translates to maybe 4–10 trades per year per symbol instead of 100+.
The composite values that pass this filter are the ones where
*multiple factors agree strongly* — the only regime where the system
actually has signal.

### 2. Extend holding period

- **Current paper trade**: flips whenever the next snapshot direction
  changes. Often days.
- **Retail**: minimum 4-week hold once entered. Exit only on extreme
  reversal (|composite| > 1.5 in the other direction) or a clear
  structural break (e.g., OPEC+ flips production policy).

Pros can't afford this — they'd be flat by month-end on any
drawdown. You can.

### 3. Size aggressively at the extremes

- **Current max**: 2x position.
- **Retail max for genuinely extreme setups** (composite > 0.9 +
  3+ factors aligned + a clear catalyst + macro doesn't disagree):
  4–5x. A "max conviction" trade should be a real bet.

Pros structurally cannot do this. They have factor-exposure caps,
position concentration limits, and risk-officer overrides that
prevent any single trade from being more than ~5% of book. You don't.

### 4. Skip markets you don't have edge in

- **Brent backtest**: composite −2.4pp vs. narrative-only. Honest
  read: we have no edge on Brent (US-skewed data). Pros must trade
  Brent because they have to deploy capital across the global oil
  complex. You don't. **Just trade WTI.**
- Re-evaluate yearly. If alt-data was ever added that closed
  Brent's gap, revisit.

### 5. Overlay domain reading on the signal trigger

- The system says "narrative + factors aligned bearish". You then
  check independently: does the curve confirm? Is positioning at
  an extreme that argues for a reversal? Does the news this week
  point at a clear catalyst? What would *invalidate* this trade?
- A signal trigger that survives this filter is much higher quality
  than one that doesn't.
- This is where personal domain knowledge becomes the actual edge —
  the model can't read a Goldman note for nuance, but you can.

---

## What the system uniquely lets retail do

Not signal generation per se. The retail-edge-relevant things our
system actually delivers:

### Pattern recognition over time

Paper trading + backtest let you see when this composite actually
works (shock + trending regimes) and when it doesn't (range,
stretched). Over 6–12 months, you build a calibrated feel for the
regimes where you should pay attention vs. ignore. **No book teaches
this; only running it does.**

### Discipline forcing function

When narrative is screaming bearish but you "feel" bullish (because
you've been long for weeks and don't want to flip), the system's
signal is an emotional check on your bias. The hard part of retail
isn't picking trades; it's not adding when you shouldn't, and exiting
when the thesis breaks. The model is unemotional — let it shame you
into discipline.

### Calibrated news filter

The narrative + theme breakdown helps you see *which* news is moving
the model vs. which is random chatter. After a few months of watching
it, you start seeing market-moving headlines coming through the
narrative score before flat price has reacted. Useful as a filter on
your own reading.

### Auditable, repeatable framework

When you take a trade, the composite breakdown gives you a written
record of *why* — which factors said what, at what magnitude. After
the trade closes, you can attribute success/failure to specific
factors. Over years this builds an evidence base for how to refine
the system, the weights, or your own behavior. Without this audit
trail, retail trading is just a series of half-remembered war stories.

---

## A concrete retail playbook

1. **Identify 4–8 high-conviction setups per year.** Composite > 0.8,
   multiple factors aligned, a clear catalyst, your domain reading
   confirms.
2. **Size those 3–5x** and **hold 4–8 weeks** unless a clean reversal
   (|composite| > 1.5 the other way) or a clear structural break.
3. **Sit flat the other ~80% of the time.** This is the hardest part
   psychologically, but it's the entire game.
4. **Develop a contrarian "tail" muscle.** When news flow is panicked
   and pros are forced flat (margin calls, year-end de-risking,
   forced selling), be the buyer or seller of last resort. The 4–5
   trades per decade where you nail this make most of the total
   return.

It's a small number of trades, sized boldly, held with discipline.
Boring philosophically, very hard psychologically. Probably the only
realistic way for a one-person retail oil trader to actually beat
sitting in T-bills.

---

## What this implies for the system roadmap

Reread the [roadmap in strategy_versions.md](strategy_versions.md)
through this lens:

- **Risk management (Priority 1)** — still highest leverage. Vol-
  targeting + drawdown auto-flat protect the rare big bets. Worth
  building even if we only take 4 trades a year.
- **Spread / curve trading (Priority 2)** — useful but secondary.
  Spreads ARE a retail-edge play (lower vol, higher Sharpe, less
  punishing during drawdowns) but require more sophistication to use
  well. Consider after Priority 1 lands.
- **Cross-asset overlay (Priority 3)** — directly serves the
  "high-conviction filter" logic above. A composite >0.8 reading with
  DXY ↑ + equities ↓ confirming risk-off is a different (better)
  trade than the same signal during a benign macro backdrop.
- **Alt data (Priority 4)** — only worth it once real capital is
  committed. Until then, the absence of ICE Brent COT etc. just
  means we don't trade Brent.

The roadmap and the retail-edge thesis agree: **infrastructure
matters more than more factors**. Risk management + macro context
unlock the few big-bet patterns retail can actually win on.

---

## What this thesis does NOT promise

- **Daily profitability.** This thesis is built around being flat
  most of the time and taking concentrated bets. The equity curve
  will be choppy and barely correlated with whatever oil prices
  did over any given month.
- **Beating buy-and-hold every year.** In a strong trending year
  (oil up 50%) a 4-trade retail playbook will probably underperform
  just being long flat price. The pitch is *risk-adjusted* edge
  over a multi-year period.
- **An out for being lazy with the work.** Sitting flat ≠ ignoring
  the market. The flat periods are when you read the daily reports,
  refine your priors, and prepare to recognize the rare setup when
  it forms. The bet sizing only earns its keep if the pattern
  recognition is real, and that takes work.

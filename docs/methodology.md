# Oil Narrative Engine — Methodology

A reference for how this system collects information, turns it into narrative
events, scores them daily, and how to read the resulting numbers.

> **Companion docs:**
> [`strategy_archetypes.md`](strategy_archetypes.md) — taxonomy of trading
> archetypes our hypotheses test against.
> [`literature_references.md`](literature_references.md) — academic papers
> behind each archetype, with a "what we use vs what's prescribed" gap analysis.

Source code is the source of truth; this doc describes intent and constants
as of the current state. When numbers move, update this file.

---

## 0. Theoretical foundation

The model rests on seven specific claims about how oil markets behave and
how a small system can extract edge from them. Each claim drives a concrete
design choice; together they form the *why* behind the architecture
documented in sections 1-8.

### 0.1 Multi-source aggregation beats any single source

No single input — narrative, positioning, inventory, term structure — is
reliable enough by itself to generate consistent edge. Each captures a
different *aspect* of the market with different reliability and decay rate:

| Input | What it captures | Information decay | Source noise |
|---|---|---|---|
| Narrative | Sentiment, event flow, consensus tilt | Hours to days | High (chatter / opinion) |
| Term structure | Real-time physical tightness | Real-time | Low |
| Positioning (COT) | Speculator crowdedness | Weekly | Low but stale |
| Inventory | Physical balance vs. seasonal | Weekly (EIA) / monthly (JODI) | Low but lagged |

**Design implication:** the system *must* be an ensemble. Single-factor
strategies overfit to whichever regime the chosen factor happens to work in.
Diversification across orthogonal inputs is the first edge source.

### 0.2 Factor predictive value is regime-conditional

The same factor often has *opposite* predictive value in different market
regimes. E.g., extreme positioning crowding is a strong reversal signal
in `stretched_*` regimes, but a momentum confirmation in clean trends. A
tight inventory print is a high-conviction directional signal in `shock`
regimes but background noise in `range`.

**Design implication:** weights must adapt to regime. A single static
weight vector across all market conditions averages out what should be
case-by-case judgment. Hence `regime_factor_weights` keyed by
primary_regime (see 8A.5).

### 0.3 Contrarian fade at extremes, momentum follow in trends

Speculator positioning is mean-reverting at extremes (CTAs and
discretionary funds reload at the same time, pile into the same direction,
then capitulate together). Inventory and term structure are largely
fundamental — they move with physical reality, not sentiment. Narrative
sits in between.

**Design implication:** each factor's *sign convention* should match its
behavior. Positioning is contrarian (sign-flipped, gated past ±1σ). Term
structure is direct (backwardation = bullish). Inventory is direct (low
stocks vs. seasonal = bullish). Mechanically encoded, not left to
post-hoc judgment.

### 0.4 Z-scoring is the right common scale

Raw narrative scores, spread prices, COT net length, and inventory levels
live on wildly different scales — you can't linearly combine them without
normalization. Z-scoring against each factor's own recent history (typically
30-90 days) puts everything on a "how unusual is today" scale where weights
are dimensionally comparable.

**Design implication:** every factor function returns roughly `[-2, +2]`.
Composite weights then have intuitive meaning ("inventory weight of 0.30
means a 1σ inventory move contributes 0.3 to the composite").

### 0.5 Information has heterogeneous decay rates

A breaking news event affects the narrative score within hours. A CFTC COT
report is from last Tuesday but released Friday. An EIA inventory print is
from a week ago. The model is asked to make a decision *today* using
inputs of wildly different vintages.

**Design implication:** we use the *latest available* reading as the
current state of each factor — never extrapolate, never re-use stale
data as if it were fresh. The reports/AI judgment flow separately tags
data freshness so the human reader knows what's news vs. background.

### 0.6 Per-symbol asymmetry — not all crudes are the same

WTI and Brent are both crude, but they respond to different drivers. WTI
is a domestic crude with a physical chokepoint at Cushing — sensitive to
US inventory, US refinery runs, US shale economics. Brent is a waterborne
benchmark for global trade — sensitive to Middle East geopolitics, ICE
positioning, Asian refinery demand, freight rates. Our US-skewed data
naturally favors WTI; Brent needs international data we don't have.

**Design implication:** `regime_factor_weights` is keyed by **(symbol,
regime)**, not just regime. Brent's `trend_up` weights are 60% narrative
because that's where its remaining edge is; WTI's `trend_up` weights
include heavier inventory and term structure because those signals
genuinely lead WTI prices.

### 0.7 Discipline beats cleverness

The biggest source of failed quant strategies is not bad signals — it's
overfitting to historical data, then deploying with no out-of-sample
test. Once a strategy is locked, you have to be patient enough to let
it run on truly new data before judging it. Re-tuning weights every time
recent performance disappoints is just data-mining in slow motion.

**Design implication:** locked strategy versions
(`docs/strategy_versions.md` v1 lock at 2026-05-14), written acceptance
criteria for each change, future walk-forward backtests (refit on years
N to N+1, test on year N+2). Reduces the temptation to chase noise.

### 0.8 What this theory does NOT claim

- **It does not claim markets are inefficient in a way only we can see.**
  All four factors we use are known to the industry. Edge comes from
  *combining them with discipline*, not from secret data.
- **It does not claim the model will work in all regimes.** Backtest
  evidence shows clear regime dependence — composite earns its keep in
  shock + trend regimes, marginal in range, weak on Brent.
- **It does not claim signal quality alone produces returns.** Without
  position sizing, risk management, and execution discipline, even a
  high-hit-rate signal can lose money. The signal layer is necessary
  but not sufficient.

---

## 1. What this system does

Aggregate text from many free sources (official agencies, sell-side reports,
financial news, social chatter), extract discrete **narrative events** from
the text (e.g. "OPEC announced production cut"), and score each
**(date, topic)** combination on a daily basis. The scores feed the Streamlit
dashboard at `localhost:5081` and the event study at
`scripts/run_event_study_weekly.py`.

The goal is not to predict price directly, but to make narrative pressure
visible and measurable so it can be combined with price-based signals
downstream.

---

## 2. Pipeline

```
fetch_sources.py  →  ingest_folder.py  →  extract_narratives.py  →  score_narratives.py
   raw text             documents +            narrative_events       daily_narrative_scores
   in inbox             chunks tables          table                  + daily_theme_scores
```

Hourly cron (`crontab -l`) runs all five steps:
`init_sources → fetch → ingest → extract → score`. Weekly cron (Sundays 02:30)
also runs `run_event_study_weekly.py`.

### Step-by-step
- **fetch_sources.py** — calls every enabled fetcher in
  `app/config/fetcher_config.json`; writes raw `.txt` files into
  `data/inbox/<source_bucket>/<source_id>/`.
- **ingest_folder.py** — chunks the text, stores `documents` and `chunks`
  rows keyed by `source_id`. Skips dupes by filename.
- **extract_narratives.py** — for each chunk, applies the rule-based or
  LLM extractor (configured by `app/config/llm_config.json`) and writes
  one row per detected event into `narrative_events`.
- **score_narratives.py** — aggregates events by `(score_date, topic)` and
  writes `daily_narrative_scores` (subtopic level) and `daily_theme_scores`
  (rolled up by theme).

---

## 3. Source taxonomy

Every source is classified into a **bucket** with a credibility weight
(`app/config/scoring_config.json::bucket_weights`):

| Bucket | Weight | Examples |
|---|---|---|
| `official_data` | 1.00 | EIA weekly, OPEC press releases, OFAC, WhiteHouse.gov, Fed speeches |
| `official_reports` | 0.95 | EIA STEO, IEA OMR, OPEC MOMR, IMF WEO |
| `institutional_public` | 0.85 | Producer press releases, refiner earnings calls |
| `sellside_private` | 0.82 | GS / JPM / Citi / MS / BoA / Macquarie weeklies (manual upload) |
| `authoritative_news` | 0.72 | Reuters, Bloomberg, OilPrice.com, RBN Energy, Google News, ZeroHedge |
| `social_open` | 0.45 | Reddit, Bluesky, Hacker News, StockTwits, X, Truth Social |
| `social_private_manual` | 0.30 | Forwarded WeChat / Telegram / WhatsApp |

The full registry lives in `app/config/source_registry.yaml`.

### Credibility tiers
A separate `credibility_tier` (1–5, lower = more authoritative) is set per
source. This influences event-level credibility, which feeds the scoring
weights below.

### Cost levels
`free` sources get a small +5% bonus (`free_source_bonus`) — design choice
to lean toward the free tier we explicitly built around.

---

## 4. Narrative event extraction

A **narrative event** is the atomic unit of signal. One source document can
produce zero, one, or many events. Each event has:

| Field | Meaning |
|---|---|
| `theme` / `topic` | Hierarchical narrative label (e.g. `supply` / `opec_policy`) |
| `direction` | `bullish` / `bearish` / `mixed` / `neutral` |
| `credibility` | 0–1, derived from source bucket + tier |
| `novelty` | 0–1, how new this take is vs. recent history |
| `confidence` | 0–1, extractor's confidence in the read |
| `verification_status` | `officially_confirmed` / `partially_confirmed` / `unverified` / `refuted` |
| `horizon` | `intraday` / `swing` / `medium_term` |
| `rumor_flag` | bool — extractor saw rumor language |
| `evidence_text` | the exact passage that produced the event |

### Extractor modes (`app/config/llm_config.json`)
- **rule** (default in cron) — keyword/pattern-based, free, fast, lower recall.
- **llm** — Anthropic Claude or OpenAI GPT-4o; better recall but token cost.
  Requires `REPORT_LLM_API_KEY`. `mode_default: auto` falls back to rules
  when no API key is configured.

---

## 5. Daily narrative score — the formula

For each `(score_date, commodity, topic)` with at least one event,
`score_narratives.py` computes:

```
narrative_score = raw_score
                  × persistence_multiplier
                  × breadth_multiplier
                  − crowding_penalty
```

### 5.1 raw_score
Sum of per-event signed strengths:
```
event_strength = sign(direction)
               × bucket_weight
               × verification_multiplier
               × horizon_multiplier
               × (0.45·credibility + 0.35·novelty + 0.20·confidence)
               × (1 − rumor_penalty)        if rumor_flag
               × (1 + official_confirmation_bonus)  if officially_confirmed
               × (1 + free_source_bonus)    if cost_level == "free"
```

Constants (`scoring_config.json`):
- `direction_sign`: bullish=+1.0, bearish=-1.0, mixed=+0.25, neutral=0
- `verification_multipliers`: officially=1.0 / partially=0.85 / unverified=0.6 / refuted=0.2
- `horizon_multipliers`: intraday=0.85 / swing=1.0 / medium_term=1.1
- `rumor_penalty`: 0.18, `official_confirmation_bonus`: 0.12, `free_source_bonus`: 0.05

### 5.2 persistence (half-life 5 days)
```
persistence = min(1.0, Σ exp(−offset/5)) / 2     for offset = 1..20,
                                                   while prior_day_sign matches today's sign
```
Walks back up to 20 days from `score_date`. Counts only consecutive
same-sign days (breaks on first opposite-sign day). Capped at 1.0.

```
persistence_multiplier = 1 + 0.25 · persistence       # max +25%
```

A topic that's been quietly bullish for a week gets a noticeable
amplifier when fresh bullish events appear.

### 5.3 breadth (today only)
```
breadth = min(1.0, distinct_sources_on_today / 5)
breadth_multiplier = 1 + 0.40 · breadth                # max +40%
```

A story carried by 5+ distinct sources today is worth more than the same
sentiment from one source repeated.

### 5.4 crowding penalty
```
crowding_penalty = max(0, event_count − 4) × 0.04
```

Diminishes the score when one topic has too many events in a single day —
typically a sign of one news cycle being amplified rather than fresh
information.

### 5.5 What doesn't roll forward
A day with zero events for topic X produces **no row** for `(date, X)`.
The dashboard shows nothing for that topic that day. The persistence
multiplier only applies *to today's events*; it doesn't synthesize a
score from yesterday's events.

If you want a true rolling-average view that survives gap days, that's a
separate computation (TBD).

### 5.6 Auxiliary fields
Stored alongside the score for diagnostics:

| Field | Meaning |
|---|---|
| `event_count` | How many events fed this row |
| `breadth` | 0–1, source diversity on the day |
| `persistence` | 0–1, trend continuity score |
| `source_divergence` | 0–1, gap between official-bucket and chatter-bucket direction |
| `official_confirmation_score` | Fraction of events officially confirmed |
| `news_breadth_score` | Fraction from authoritative_news |
| `chatter_score` | Fraction from social buckets |
| `crowding_score` | Penalty applied this day |

A high **source_divergence** with low **official_confirmation_score**
means "the chatter is talking about something institutions aren't yet
confirming" — interesting both as a leading indicator and as a noise
warning.

---

## 6. Reading the dashboard

`localhost:5081`, served by `app/dashboard/streamlit_app.py`.

- **Date picker** — calendar from 2010-01-01 to today. Picks any single
  day; if no scored narratives exist for that day, the view is empty.
- **Primary narrative** — the topic with the highest `|narrative_score|`
  on the picked day.
- **Market bias** — sum of all narrative_scores on the day. Positive =
  bullish overall, negative = bearish.
- **Recommendations tab** — turns the daily theme scores into a sized
  long/short list per "book" defined in `multi_strategy_config.json`.
  Includes a 30-day rolling z-score per book.
- **Backtest / Multi-Backtest tabs** — use the recommendations against
  historical prices to simulate book performance.
- **Upload tab** — drag-and-drop a PDF/DOCX/TXT, pick the source bucket
  and the report's *publication date* (calendar back to 2010), and the
  document goes straight into the inbox for the next ingest.

---

## 7. Event study methodology

`scripts/run_event_study.py` answers: *when narrative score is X on day
T, what does price do over T+1, T+3, T+5, T+10?*

### Buckets
Daily narrative scores are bucketed by absolute magnitude and sign:
- `strong_bullish` / `bullish` / `neutral` / `bearish` / `strong_bearish`

### Forward returns
For each bucketed observation, compute `(close_{T+h} − close_T) / close_T`
for each horizon `h`. Aggregate:
- `avg_fwd_ret_<h>d` — mean forward return
- `hit_rate_<h>d` — fraction of observations where price moved in the
  direction the narrative predicted (up for bullish, down for bearish)

### Weekly snapshot
`scripts/run_event_study_weekly.py` runs the study for WTI and Brent
each Sunday and appends a one-row-per-(symbol, bucket) summary to
`data/processed/research/event_study_history.csv`. Full JSON snapshots
are kept in `event_study_history/{date}_{symbol}.json` so we can track
how the buckets evolve over time.

### How to read the output
- Hit rate < 50% on a "bullish" bucket means the narrative is **fading
  contrarianly** — bullish chatter clusters near tops.
- Hit rate > 50% means **trend confirmation** — narrative direction
  matches price direction.
- Sample size per bucket matters. Below ~30 observations, results
  reflect regime noise more than structural signal.

### Current finding (as of 2026-04-30, N=89 across 26 score-dates)
- **Pre-Iran spike (Jan-Feb)**: bullish narratives co-moved with price
  (trend confirmation).
- **Post-Iran spike (Apr 10-30)**: bullish narratives faded
  contrarianly — price dropped 7.9% over 5 days after strong-bullish
  reads, hit rate 22%.

Read this as **regime-dependent**, not structural. Sample is dominated
by one big up-then-down move. More history needed.

---

## 8. Known limitations

- **Sample depth.** Auto-fetch began 2026-04-29; manual analyst PDFs go
  back to 2026-04-10. Pre-April history is sparse and platform-dependent
  (HN searchable to inception, Bluesky from 2024, Reddit only recent).
- **Truth Social.** Direct API is Cloudflare-gated. Trump's posts come
  in indirectly via Google News coverage with ~15-30 min delay.
- **X / Twitter.** Not free. Coverage flows in via news syndication
  but raw posts are unavailable.
- **DOE / Treasury / State / OFAC RSS.** All probed URLs return 404 —
  press releases enter via Google News reporting on them.
- **Bluesky cursor pagination.** Some queries 403 past page 1 (rate
  limit). First page works fine; historical depth is limited.
- **Rule-based extractor recall.** The default extractor uses keyword
  patterns; it misses subtle narratives. LLM mode is more thorough but
  costs tokens. We run rule-based in cron; switch to LLM only for the
  final daily aggregation if you want depth.
- **Quiet days.** Score row exists only when a topic has events that
  day. Persistence boosts existing same-day events; it does not
  synthesize scores from history.
- **Single-commodity scope.** Currently `crude_oil` only. Other
  commodities (NatGas, products, refined fuels) would need their own
  topic taxonomies and rule sets.

---

## 8A. Composite signal — regime-conditional factor blend

Layered on top of the narrative score, the composite signal combines
narrative with one or more market-derived factors using **regime-conditional
weights**. The intuition: different factors have different signal value
depending on whether the market is trending, ranging, stretched, or in
shock — so the same factor stack should weight inputs differently per
regime.

### 8A.1 Architecture

```
narrative score (per book)              ─┐
term structure z-score (M1−M2)          ─┤── weighted by regime ──→ composite ──→ direction
positioning z-score (gated, contrarian) ─┤
[future: inventory, momentum, ...]      ─┘
```

- **Regime** comes from `app/research/regime.py` (multi-label, primary
  regime via priority: `shock` > `stretched_*` > `trend_*` > `range`).
- **Weights per regime** live in `app/config/strategy_config.json` under
  `regime_factor_weights`. Missing factors are renormalized out so the
  panel never breaks when a factor is unavailable.
- **All factors are z-scored** so weights are dimensionally comparable
  across factors of very different raw scales.

### 8A.2 Term-structure factor

Front-month spread (M1 − M2) z-scored over the trailing 90 days.
Positive = backwardation = bullish for flat price. Source: Yahoo Finance
delivery-month tickers (e.g., `CLM26.NYM`, `CLN26.NYM`). See
`app/fetchers/term_structure.py`.

**Caveat:** the prices stored in `market_prices` are tagged under
*today's* contract identifiers, so historical backtests beyond ~30 days
read the *deferred* spread rather than the actual front spread that
was trading then. A roll-aware historical fetcher is the next iteration
when sufficient native data has accumulated.

### 8A.3 Positioning factor (CFTC COT)

Weekly Money-Manager net length expressed as `(MM long − MM short) / OI %`,
z-scored over the trailing 52 weeks.

- **Source:** CFTC Disaggregated Futures-and-Options Combined report
  (Socrata API at `publicreporting.cftc.gov`, dataset `kh3c-gbw2`).
  Updated every Friday afternoon US time with Tuesday-close data.
- **Market identifiers** (verified live as of 2026-05):
  - WTI → `CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE` (the
    legacy NYMEX identifier stopped reporting; CFTC now tracks WTI
    flow through the ICE Europe entry)
  - Brent → `BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE` (the
    NYMEX-listed financially-settled Brent contract)

**Why contrarian.** Money managers are momentum followers — they pile
in near tops and capitulate near bottoms. Extreme MM net length
therefore fades on average. Trend-following exposure is already in
term structure (and later, momentum); doubling it up via positioning
would just add correlation, not signal.

**Why a threshold.** Below ~1σ from the trailing mean, MM positioning
is essentially noise — the contrarian edge only shows up at extremes.
The factor applies a **soft gate**: within ±1σ it contributes 0;
past it the magnitude grows linearly with distance past the gate.

```
|z| ≤ 1.0σ  → factor = 0          (gated out)
|z| > 1.0σ  → factor = -sign(z) * (|z| - 1.0)   (contrarian, scaled)
```

So z=±0.8 → 0; z=±1.5 → ∓0.5; z=±2.0 → ∓1.0.

**Sign convention.** Positive factor = MMs are *less* long than usual
= bullish (room to add). Negative = MMs are *more* long than usual
= bearish (crowded, fade).

The threshold is exposed as a parameter (`extreme_threshold=1.0`) on
`positioning_factor()` in `app/scoring/factors.py`; tune later if
backtests suggest 1.0σ is too tight or too loose.

### 8A.4 Inventory factor (EIA Weekly)

Seasonal-deviation z-score across four EIA US petroleum stock series,
equal-weight averaged into one factor. Sign-flipped so that **high
stocks vs the seasonal baseline = bearish** (negative factor value).

- **Source:** EIA Weekly Petroleum Status Report via the EIA Open Data
  API v2 (`api.eia.gov/v2/petroleum/stoc/wstk`). Free; requires an API
  key from `eia.gov/opendata/register.php` (set as `EIA_API_KEY` env
  var). Updated every Wednesday ~10:30am ET (Thursday after holidays).
- **Series included** (equal-weight average):

  | Series ID | Cadence | What it is | Why |
  |---|---|---|---|
  | `WCESTUS1` | Weekly | US crude stocks (excl. SPR) | Headline crude balance |
  | `W_EPC0_SAX_YCUOK_MBBL` | Weekly | Cushing OK crude stocks | WTI delivery point — front-spread driver |
  | `WGTSTUS1` | Weekly | Total motor gasoline stocks | End-demand pull (refinery throughput) |
  | `WDISTUS1` | Weekly | Total distillate stocks | End-demand pull (diesel + heating oil) |
  | `JODI_OECD_CRUDE_STOCKS` | Monthly (lag ~6-8 weeks) | Sum of CRUDEOIL CLOSTLV across the OECD basket (US, JP, DE, FR, GB, IT, ES, NL, KR, CA, AU) | International (Europe + Asia) context EIA misses |

  JODI primary data only carries crude-side products (CRUDEOIL, NGL,
  OTHERCRUDE, TOTCRUDE) — there's no gasoline / distillate / jet there.
  Refined-product international coverage would require the JODI
  *secondary* dataset; for now we only pull JODI crude because EIA
  already covers US products well, and the value-add of JODI is
  international **crude** context.

  JODI is fetched directly from `jodidata.org` annual primary CSVs
  (`/_resources/files/downloads/oil-data/annual-csv/primary/{year}.csv`),
  no API key required.

**Why a seasonal baseline.** Raw inventory levels follow a strong
annual cycle (refinery turnarounds, summer driving, winter heating).
What matters is whether stocks are *unusually* high or low **for this
time of year**, not vs an absolute mean. So for each series:

```
peers          = readings within ±7 days of (current week-of-year)
                 over the trailing 5 years
series_z       = (latest - mean(peers)) / std(peers)
inventory_factor = -1 * mean(series_z over the 4 series)
```

Series with fewer than 3 same-week peers in the lookback are dropped
from the average rather than failing the whole factor.

**Sign convention.** Positive factor → stocks below seasonal → tight
market → bullish. Negative → stocks above seasonal → oversupplied
→ bearish.

**Coverage.** Same factor used for WTI and Brent. EIA US data is the
weekly headline (Brent–WTI weekly-balance correlation ~80%); JODI
adds international crude context but is monthly + lagged, so its
same-week-of-year peer count is much lower (~5 across 5 years vs ~11
for EIA, since JODI matches only one calendar month per year while
EIA's ±7-day window spans two adjacent weekly readings per year).
That naturally weights EIA more in the average without an explicit
weight knob.

Fujairah (FOIZ weekly, Middle East hub) and Singapore (EnterpriseSG
weekly, Asia hub) are candidates for future iterations when scraping
is built.

### 8A.5 Composite formula

For each (symbol, date):

```
1. Look up regime for the symbol on the date.
2. Pull weights[regime] from strategy_config.json.
3. Drop factors that are None for this date; renormalize remaining weights.
4. composite = Σ (weight_i_renorm × factor_value_i)
5. direction = LONG if composite > 0.1 else SHORT if composite < -0.1 else FLAT
```

The breakdown table on the dashboard shows each factor's value, its
renormalized weight, and its contribution — so when factors disagree
(e.g., term structure bullish, positioning bearish) you can see
immediately which one is winning the blend in this regime.

---

## 8B. Trading rules & execution semantics

How the composite signal translates into a position, and how
positions are recorded / resolved in the paper-trading ledger and the
composite backtest. Both consume the same rules so results are
directly comparable.

### 8B.1 Signal → position

`score_to_target_position` (in `app/strategy/backtest_engine.py`)
applied with composite-scale thresholds:

| Composite | Position |
|---|---|
| `> +0.40` | **+2x LONG** (strong) |
| `> +0.10` | **+1x LONG** |
| `−0.10 ≤ x ≤ +0.10` | **FLAT** (no position, no trade) |
| `< −0.10` | **−1x SHORT** |
| `< −0.40` | **−2x SHORT** (strong) |

`max_abs_position = 2.0`. The dead-band ±0.10 is the *"odds aren't
good enough to act"* zone. FLAT days are still recorded in the paper
ledger with `direction=FLAT, target_position=0` so we can see that
the model considered the day and chose not to trade.

### 8B.2 Execution price — close-to-close

Both the composite backtest and the paper-trading snapshot use:

```
entry_close      = latest market_prices.close on or before plan_date
exit_close       = entry_close of the next trade that flips direction
realized_pnl_pct = (exit_close / entry_close − 1) × target_position
```

No bid/ask, no slippage, no partial fills — close prints assumed
executable. For real-world comparison, subtract roughly 5–10bps per
turnover.

### 8B.3 Cost convention

- **Composite backtest**: applies `one_way_cost_bps = 5.0` on every
  unit of turnover (equity × |Δ position| × cost_rate). Realized PnL
  in `equity_curve` is net of cost.
- **Paper trading ledger**: does **not** deduct cost yet. `realized_pnl_pct`
  in the `paper_trades` table is gross of fees. Subtract ~5–10bps per
  turnover for like-for-like comparison with the backtest.

### 8B.4 Auto-resolution (paper trading)

When the next snapshot's direction differs from the open position's
direction, the previous trade closes at the new entry close:

```
on snapshot for symbol S on date D:
  if open_position(S).direction != new_direction:
    UPDATE paper_trades
       SET exit_date = D,
           exit_close = today_close(S),
           realized_pnl_pct = (today_close / open.entry_close − 1) × open.target_position,
           holding_days = D − open.plan_date
     WHERE trade_id = open.trade_id
  INSERT new row with the new direction
```

Same direction (e.g., LONG → LONG, possibly different size) does
**not** close the position — sizing changes accumulate as turnover
but stay in the same trade record.

### 8B.5 Cron schedule

- **Hourly at `:05`** — full pipeline (`init_sources → fetch_sources →
  fetch_prices → compute_regimes → ingest → extract → score →
  hypotheses`). Refreshes prices, COT, EIA inventory, narrative
  scores, regime tags.
- **Composite backtest at `03:15`** — recomputes the historical
  backtest each night (`run_composite_backtest.py`) so the dashboard
  serves a current backtest by morning.
- **Paper trading snapshot at `07:00`** — runs `snapshot_paper_trades.py`
  AFTER NYMEX WTI's `17:00 ET = 05:00 UTC+8` settlement, with margin
  for the `06:05` hourly `fetch_prices` to publish the official close.
  Earlier than `06:00 UTC+8` risks recording entries off the wrong close.
- **Sunday at `02:30`** — weekly event-study rerun.

`(symbol, plan_date)` is unique in `paper_trades` — re-running the
snapshot for the same day is a no-op. See `ops/crontab` for the
reproducible install.

### 8B.6 What this is and isn't

✅ **Is**:
- A truthful, ongoing scorecard of the model's signal vs. realized
  market moves, marked-to-market each time direction flips.
- A drift detector — cumulative paper-PnL hit-rate diverging from
  backtest expectations is an early warning that something has
  changed (regime characteristics, factor reliability, data source).
- Direct apples-to-apples comparison with the historical composite
  backtest because both use the same close-to-close PnL machinery.

❌ **Is not**:
- Real PnL. No fees, no slippage, no MOC fill quality, no overnight
  funding cost, no margin/financing model.
- A complete trading system. Production trading needs a sizing /
  vol-targeting / drawdown-limit layer above this signal — see
  the "what's still missing" section in `docs/strategy_versions.md`.

---

## 9. Glossary

- **Bucket** — Source category with a credibility weight. See section 3.
- **Theme** — Top-level narrative grouping (`supply`, `demand`,
  `geopolitics`, `policy`, `macro`).
- **Topic** — Subtheme within a theme (e.g. `supply` ⊃ `opec_policy`,
  `shale_production`, `shipping_disruption`).
- **Event** — One discrete narrative read from a chunk of text.
- **Score date** — The publication date of the source document. UTC.
- **Persistence** — How long a same-direction narrative has carried,
  measured with a 5-day half-life.
- **Breadth** — Source diversity on a given day for a topic.
- **Source divergence** — Gap between official-bucket sentiment and
  chatter-bucket sentiment for the same topic.
- **Crowding** — Penalty for narrow but loud topics (one cycle echoed
  many times).
- **Hit rate** — Fraction of observations where price moved the
  direction the narrative predicted.
- **Forward return** — `(price_{T+h} − price_T) / price_T`.

---

## 10. Where things live

| Concern | Path |
|---|---|
| Source registry | `app/config/source_registry.yaml` |
| Fetcher specs | `app/config/fetcher_config.json` |
| Scoring constants | `app/config/scoring_config.json` |
| LLM extractor config | `app/config/llm_config.json` |
| Topic / theme rules | `app/config/oil_topic_rules.json`, `theme_hierarchy.json` |
| Strategy / book config | `app/config/strategy_config.json`, `multi_strategy_config.json` |
| Score logic | `app/scoring/daily_score.py` |
| Event study core | `app/research/event_study.py` |
| Streamlit dashboard | `app/dashboard/streamlit_app.py` |
| Database | `data/oil_narrative.db` |
| Cron logs | `/tmp/oil_pipeline.log` |
| Research outputs | `data/processed/research/` (gitignored) |

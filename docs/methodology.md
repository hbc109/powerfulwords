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

### 8A.4 Composite formula

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

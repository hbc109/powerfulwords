# Oil Narrative Engine

End-to-end prototype that turns unstructured crude-oil narratives
(official reports, sell-side research, news, market chatter) into
structured signals, daily theme scores, multi-instrument backtests,
and an actionable morning digest.

## What this project does

```
fetch  →  ingest  →  extract  →  score  →  research/backtest  →  recommend
```

Concretely:

1. **Fetch** narratives from public sources (Reddit, EIA, IEA, OPEC,
   Reuters, SHANA, SPA) into a categorized inbox.
2. **Ingest** dropped/fetched files (PDF / DOCX / TXT). The inbox is
   the drop point — no CSV manifests needed.
3. **Extract** narrative events using either a rule-based extractor
   (multi-topic per chunk) or a provider-agnostic LLM extractor
   (Claude default, OpenAI optional).
4. **Score** daily narratives at both subtheme and rolled-up theme
   levels, with breadth, persistence, source divergence, crowding,
   and a free-source bonus.
5. **Research / backtest** against price series. Multi-book engine
   supports outrights, spreads, and product cracks with per-book
   theme weights and vetoes.
6. **Recommend**: morning markdown digest plus an interactive
   Streamlit dashboard with heatmaps, narrative-vs-price overlay,
   source attribution, today's recommendations, and per-book P&L.

## Main features

- **Drag-and-drop inbox** at `data/inbox/<bucket>/<source_id>/` —
  filename `YYYY-MM-DD_slug.ext` is enough metadata.
- **Auto-fetchers** for Reddit, EIA TWIP RSS, IEA news, OPEC press
  releases, Reuters, and a generic state-news-agency scraper for
  SHANA / SPA.
- **3-tier source taxonomy** (official / institutional / chatter)
  with a free-source preference baked into scoring.
- **Theme + subtheme hierarchy** — five main themes (supply, demand,
  inventories, macro, geopolitics) over the existing topic taxonomy,
  rolled up into a daily theme tape.
- **LLM theme discovery** — Claude scans recent chunks and proposes
  new themes/subthemes that don't fit the taxonomy. Interactive CLI
  to promote them into the live config.
- **Provider-agnostic LLM layer** (Claude default, OpenAI supported)
  with rule-based fallback when no API key is set.
- **Theme-conditioned strategy** — per-theme weights and vetoes
  (e.g. "no long oil while macro is strongly bearish").
- **Multi-instrument backtest** — outrights, spreads, and product
  cracks with configurable point-value P&L.
- **Morning digest** — markdown report with recommendations, theme
  tape, and top evidence. Optional SMTP email delivery.
- **Streamlit dashboard** — Recommendations / Overview / Trends /
  Research / Backtest / Multi-book tabs.

## Quickstart

One-time setup:

```bash
pip install -r requirements.txt
python scripts/init_sources.py        # load source registry into SQLite
python scripts/setup_inbox.py         # create data/inbox/<bucket>/<source_id>/
python scripts/check_setup.py         # verify everything is healthy
```

Set your Anthropic API key (free signup at https://console.anthropic.com).
Without one, extraction falls back to a rule-based mode that still works:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
# Persist it: echo 'export ANTHROPIC_API_KEY=sk-ant-...' >> ~/.bashrc
```

Optional — wire up email delivery for the morning digest:

```bash
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USER=you@gmail.com
export SMTP_PASS=your_app_password    # for Gmail use an App Password
export SMTP_FROM=you@gmail.com
export SMTP_TO=trader@example.com
```

### Run it every morning — one command

```bash
python scripts/run_daily.py
```

That runs the full pipeline:
fetch sources → fetch prices (Yahoo) → ingest → extract → score →
multi-book backtest → morning digest.

Add `--dashboard` to also launch the Streamlit UI when it's done:

```bash
python scripts/run_daily.py --dashboard
```

Outputs:
- `data/processed/digests/morning_<date>.md` — markdown report
- (if SMTP set) email to `SMTP_TO`
- `data/processed/backtests/multi_backtest_crude_oil.json` — per-book P&L
- Streamlit dashboard at http://localhost:8501

### Individual scripts (for finer control)

```bash
python scripts/fetch_sources.py       # narratives only
python scripts/fetch_prices.py        # prices only (Yahoo Finance)
python scripts/ingest_folder.py       # chunk + store new files
python scripts/extract_narratives.py  # rule mode (default) or --mode llm
python scripts/score_narratives.py    # daily subtheme + theme scores
python scripts/run_multi_backtest.py  # WTI + Brent + spread + cracks
python scripts/morning_digest.py      # markdown + optional email
python scripts/run_dashboard.py       # interactive view

python scripts/discover_themes.py     # LLM scans for emerging themes
python scripts/approve_themes.py      # walk through proposed themes y/n
python scripts/run_event_study.py --symbol WTI --commodity crude_oil --horizons 1,3,5,10
```

See `INBOX_QUICKSTART.md` for the drag-and-drop walkthrough.

## LLM provider

Default: Claude (`claude-sonnet-4-6` via the `anthropic` SDK).
Switch to OpenAI by editing the `provider` field in
`app/config/llm_config.json`. Set the matching env var:

```bash
export ANTHROPIC_API_KEY=...   # default
# or
export OPENAI_API_KEY=...
```

No key? The pipeline runs end-to-end with the rule-based extractor.

## Optional email delivery

Set these to have `morning_digest.py` send the report instead of
just writing the file:

```bash
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USER=you@gmail.com
export SMTP_PASS=your_app_password
export SMTP_FROM=you@gmail.com
export SMTP_TO=trader@example.com
# export SMTP_SSL=1                # optional, use SMTPS instead of STARTTLS
```

## Repository structure

```text
app/
  config/        scoring, strategy, multi-strategy, LLM, theme hierarchy,
                 fetcher and source-registry configs
  dashboard/     Streamlit app (Recommendations / Overview / Trends /
                 Research / Backtest / Multi-book tabs)
  db/            SQLite connection and repository helpers
  discovery/     LLM-driven theme discovery
  extractors/    rule-based + LLM narrative extractors, provider layer
  fetchers/      Reddit, RSS, OPEC, IEA, agency_html
  models/        Pydantic models
  prompts/       LLM prompt templates
  research/      forward-return event study
  scoring/       daily score aggregation + theme rollup
  strategy/      backtest engine, multi-book engine, recommendations

scripts/
  setup_inbox.py            create the categorized inbox folder tree
  init_sources.py           load source registry into SQLite
  fetch_sources.py          pull fresh narratives into the inbox
  ingest_folder.py          ingest everything new from the inbox
  ingest_documents.py       (alt) manifest-based ingestion
  extract_narratives.py     rule or LLM extraction
  score_narratives.py       daily subtheme + theme rollup
  load_prices_csv.py        load market_prices from CSV
  run_event_study.py        forward-return event study
  run_backtest.py           single-symbol backtest
  run_multi_backtest.py     multi-instrument backtest (WTI / Brent / spread / cracks)
  discover_themes.py        LLM scan for emerging themes
  approve_themes.py         interactive y/n promotion of proposed themes
  morning_digest.py         markdown digest + optional SMTP email
  run_dashboard.py          launch Streamlit
  inspect_*.py              CLI inspectors for scores / event studies / backtests

data/
  inbox/                    drop point (gitignored — only the folder
                            skeleton + a couple of demo seeds are tracked)
  raw/                      legacy demo inputs and price CSVs
  processed/                generated outputs (gitignored except .gitkeep):
                              clean_text/, chunks/, metadata/, events/,
                              signals/, signals/themes/, research/,
                              backtests/, themes/, digests/
  oil_narrative.db          SQLite DB (gitignored)
```

## Network notes

Several public sources sit behind Akamai/Cloudflare-style edge
protection that blocks data-center / VPS IPs. From a normal home or
office network they work fine; from cloud you may see `[BLOCKED]`
from EIA, OPEC, IEA, SHANA, or SPA. Reddit and most RSS feeds work
from anywhere.

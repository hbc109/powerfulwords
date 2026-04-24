# Oil Narrative Engine

An end-to-end prototype for crude oil narrative research, signal generation, event study, dashboard monitoring, and rule-based backtesting.

## What this project does

This repository turns unstructured oil-market information into structured research outputs.

Pipeline:

1. Ingest documents from a categorized inbox (`data/inbox/<bucket>/<source_id>/`)
   or from a CSV manifest
2. Parse and chunk PDF / DOCX / TXT files
3. Extract narrative events using either:
   - a rule-based baseline extractor (multi-topic per chunk)
   - a provider-agnostic LLM extractor (Claude default, OpenAI optional)
4. Score daily narratives per topic with breadth, persistence, source
   divergence, crowding, and a free-source bonus
5. Run event studies against price data
6. Run a daily backtest with per-trade topic attribution
7. Monitor results in a Streamlit dashboard

## Main features

- **Drag-and-drop ingestion** via categorized inbox folders — no manifest needed
- SQLite storage with additive schema migrations
- Crude oil narrative taxonomy with multi-topic extraction per chunk
- **Provider-agnostic LLM** layer (Claude / OpenAI), with rule-based fallback
- Free-source preference baked into scoring
- Daily narrative scoring across topic / breadth / persistence / divergence
- Forward-return event study
- Daily backtest with topic attribution per trade
- Streamlit dashboard

## Quickstart

See `INBOX_QUICKSTART.md` for the drag-and-drop flow, or the older
`STEP3_QUICKSTART.md` ... `STEP7_QUICKSTART.md` for the manifest-based flow.

## Repository structure

```text
app/
  config/        configuration files
  db/            SQLite connection and repository helpers
  dashboard/     Streamlit dashboard
  extractors/    rule-based and LLM extractors
  models/        Pydantic models
  prompts/       LLM prompt templates
  research/      event study logic
  scoring/       daily score aggregation
  strategy/      backtest engine

scripts/
  init_sources.py
  ingest_documents.py
  extract_narratives.py
  score_narratives.py
  load_prices_csv.py
  run_event_study.py
  run_backtest.py
  run_dashboard.py

data/
  raw/           input documents and price CSV files
  processed/     generated outputs

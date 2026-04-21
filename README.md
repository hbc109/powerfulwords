# Oil Narrative Engine

An end-to-end prototype for crude oil narrative research, signal generation, event study, dashboard monitoring, and rule-based backtesting.

## What this project does

This repository turns unstructured oil-market information into structured research outputs.

Pipeline:

1. Ingest documents from local folders and CSV manifests
2. Parse and chunk PDF / DOCX / TXT files
3. Extract narrative events using either:
   - a rule-based baseline extractor
   - an OpenAI-powered LLM extractor
4. Score daily narratives across topics and sources
5. Run event studies against price data
6. Run a simple daily backtest
7. Monitor results in a Streamlit dashboard

## Main features

- Local document ingestion
- SQLite storage
- Crude oil narrative taxonomy
- Rule-based extraction
- Optional OpenAI LLM extraction with fallback
- Daily narrative scoring
- Forward-return event study
- Daily rule-based backtest
- Streamlit dashboard

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

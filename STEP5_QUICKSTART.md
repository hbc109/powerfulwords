# Step 5 Quickstart

1. Confirm Step 1-4 are done:
```bash
python scripts/init_sources.py
python scripts/ingest_documents.py --manifest manifests/step3_demo_manifest.csv
python scripts/extract_narratives.py
python scripts/score_narratives.py
```

2. Load demo prices:
```bash
python scripts/load_prices_csv.py --csv data/raw/prices/wti_demo_prices.csv
```

3. Run event study:
```bash
python scripts/run_event_study.py --symbol WTI --commodity crude_oil --horizons 1,3,5,10
```

4. Inspect result:
```bash
python scripts/inspect_event_study.py --file data/processed/research/event_study_crude_oil_WTI.json
```

Outputs:
- SQLite table used: market_prices, daily_narrative_scores
- Research file: data/processed/research/event_study_crude_oil_WTI.json

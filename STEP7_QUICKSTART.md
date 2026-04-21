# Step 7 Quickstart

1. Make sure Step 1-6 data exists:
```bash
python scripts/init_sources.py
python scripts/ingest_documents.py --manifest manifests/step3_demo_manifest.csv
python scripts/extract_narratives.py
python scripts/score_narratives.py
python scripts/load_prices_csv.py --csv data/raw/prices/wti_demo_prices.csv
```

2. Run backtest:
```bash
python scripts/run_backtest.py
```

3. Inspect backtest:
```bash
python scripts/inspect_backtest.py --file data/processed/backtests/backtest_crude_oil_WTI.json
```

Outputs:
- JSON result: `data/processed/backtests/backtest_crude_oil_WTI.json`

Current model:
- aggregate daily narrative score across topics
- map score to daily target position
- apply simple transaction costs
- produce equity curve and trade log

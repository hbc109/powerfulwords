# Step 6 Quickstart

1. Make sure Step 1-5 have been run:
```bash
python scripts/init_sources.py
python scripts/ingest_documents.py --manifest manifests/step3_demo_manifest.csv
python scripts/extract_narratives.py
python scripts/score_narratives.py
python scripts/load_prices_csv.py --csv data/raw/prices/wti_demo_prices.csv
python scripts/run_event_study.py --symbol WTI --commodity crude_oil --horizons 1,3,5,10
```

2. Install dashboard dependency:
```bash
pip install streamlit pandas
```

3. Launch dashboard:
```bash
python scripts/run_dashboard.py
```

Main panels:
- Scores
- Events
- Prices
- Research

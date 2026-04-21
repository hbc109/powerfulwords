# Step 4 Quickstart

1. Make sure Step 1-3 have been run:
```bash
python scripts/init_sources.py
python scripts/ingest_documents.py --manifest manifests/step3_demo_manifest.csv
python scripts/extract_narratives.py
```

2. Score daily narrative signals:
```bash
python scripts/score_narratives.py
```

3. Inspect the latest scores:
```bash
python scripts/inspect_scores.py
```

Outputs:
- SQLite table: `daily_narrative_scores`
- JSON files: `data/processed/signals/`

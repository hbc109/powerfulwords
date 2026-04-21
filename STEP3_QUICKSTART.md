# Step 3 Quickstart

1. Initialize sources and DB

```bash
python scripts/init_sources.py
```

2. Ingest demo files or your own files

```bash
python scripts/ingest_documents.py --manifest manifests/step3_demo_manifest.csv
```

3. Extract narrative events

```bash
python scripts/extract_narratives.py
```

Outputs:
- SQLite table: narrative_events
- JSON event files: data/processed/events/

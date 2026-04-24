# Inbox Quickstart

Drag-and-drop ingestion. No CSV manifest required.

## One-time setup

```bash
python scripts/init_sources.py   # load the source registry into SQLite
python scripts/setup_inbox.py    # create data/inbox/<bucket>/<source_id>/ folders
```

This creates the folder tree from `app/config/source_registry.yaml`.
Every source_id in the registry gets its own folder.

## Auto-fetching from public sources

Pull recent items from configured public sources (Reddit, EIA TWIP RSS,
OPEC press releases) directly into the inbox:

```bash
python scripts/fetch_sources.py            # run every enabled fetcher
python scripts/fetch_sources.py --only reddit_oil
python scripts/fetch_sources.py --days 7   # override lookback window
```

Configure which sources run in `app/config/fetcher_config.json`.
Re-runs are safe — files already on disk are skipped.

**Network notes**: EIA and OPEC sit behind Akamai/Cloudflare-style edge
protection that blocks many cloud / VPS IPs. If you see `[BLOCKED]`
from one of them, run from your home or office network instead.

## Daily flow

1. Drop files into the matching folder (or run `scripts/fetch_sources.py`).
   Filename must start with a date:

   ```
   data/inbox/official_reports/iea_omr/2026-04-23_iea_omr_apr.pdf
   data/inbox/authoritative_news/reuters_energy/2026-04-24_opec_extends_cuts.txt
   data/inbox/social_private_manual/wechat_forwarded/2026-04-24_red_sea_chatter.txt
   ```

2. Ingest everything new (idempotent — already-ingested files are skipped):

   ```bash
   python scripts/ingest_folder.py
   ```

3. Run the rest of the pipeline as before:

   ```bash
   python scripts/extract_narratives.py           # rule mode, or set API key + --mode llm
   python scripts/score_narratives.py
   python scripts/run_event_study.py --symbol WTI --commodity crude_oil --horizons 1,3,5,10
   python scripts/run_backtest.py
   python scripts/run_dashboard.py
   ```

## Adding a new source

1. Add an entry under `sources:` in `app/config/source_registry.yaml`.
2. Re-run `python scripts/init_sources.py && python scripts/setup_inbox.py`.

## File naming rules

- Must start with `YYYY-MM-DD_` — that date becomes `published_at`.
- Supported extensions: `.txt`, `.pdf`, `.docx`.
- Files without a date prefix are skipped with a warning.

## LLM provider

`app/config/llm_config.json` controls which provider is used when
`--mode llm` or `--mode auto` is selected. Default is **anthropic**.

Set the matching key:

```bash
export ANTHROPIC_API_KEY=your_key     # default
# or switch the `"provider"` field to "openai" and:
# export OPENAI_API_KEY=your_key
```

No API key? The pipeline falls back to the rule-based extractor so the
whole flow still works offline.

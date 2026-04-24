# Inbox

Drop files into the matching folder, then run:

```bash
python scripts/ingest_folder.py
```

Folder layout: `data/inbox/<source_bucket>/<source_id>/<file>`.

File naming convention: `YYYY-MM-DD_short_title.ext`. The date prefix is
parsed as `published_at`. Supported extensions: `.txt`, `.pdf`, `.docx`.

Source buckets (most-trusted first):
- `official_data` — EIA weekly inventory, OPEC press releases, OFAC, SHANA, SPA
- `official_reports` — EIA STEO, IEA OMR, OPEC MOMR, IMF WEO, World Bank
- `institutional_public` — Producer press releases, refiner earnings calls
- `sellside_private` — Sell-side research (only ingest what you're authorized to)
- `authoritative_news` — Reuters, Bloomberg headlines
- `social_open` — X, Reddit, Truth Social
- `social_private_manual` — Forwarded WeChat / WhatsApp / Telegram chatter

Add or rename a source: edit `app/config/source_registry.yaml`, then re-run
`python scripts/init_sources.py && python scripts/setup_inbox.py`.

"""Create the categorized inbox folder structure under data/inbox/.

Run once after pulling the repo (or any time the source registry changes).
Folder layout:

    data/inbox/<source_bucket>/<source_id>/

Drop a file (PDF / DOCX / TXT) into the matching folder, then run
`python scripts/ingest_folder.py` — the ingester reads source_bucket and
source_id from the folder path, so no manifest is required.

File naming convention: `YYYY-MM-DD_short_title.ext`. The date prefix is
parsed as published_at; files without a parseable date prefix are skipped
with a warning.
"""

from __future__ import annotations

from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.config.settings import load_source_registry

INBOX_ROOT = BASE_DIR / "data" / "inbox"
README_PATH = INBOX_ROOT / "README.md"

README_BODY = """# Inbox

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
"""


def main() -> None:
    registry = load_source_registry()
    INBOX_ROOT.mkdir(parents=True, exist_ok=True)
    README_PATH.write_text(README_BODY, encoding="utf-8")

    created = 0
    for src in registry.get("sources", []):
        bucket = src["source_bucket"]
        sid = src["source_id"]
        folder = INBOX_ROOT / bucket / sid
        folder.mkdir(parents=True, exist_ok=True)
        keep = folder / ".gitkeep"
        if not keep.exists():
            keep.touch()
            created += 1

    print(f"Inbox ready at {INBOX_ROOT}")
    print(f"Created {created} new placeholder folders.")


if __name__ == "__main__":
    main()

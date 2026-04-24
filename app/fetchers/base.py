"""Common types and helpers for source fetchers.

A fetcher pulls items from a single external source and returns a list
of FetchedDocument objects. The runner (scripts/fetch_sources.py) is
responsible for writing each document into the inbox folder where
ingest_folder.py picks it up.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


USER_AGENT = (
    "oil-narrative-engine/1.0 (+https://github.com/hbc109/powerfulwords) "
    "research/personal-use"
)


@dataclass
class FetchedDocument:
    source_id: str          # must match a row in app/config/source_registry.yaml
    source_bucket: str      # must match the source_id's bucket in the registry
    published_at: date
    title: str
    text: str               # raw narrative body — what gets chunked + extracted
    url: Optional[str] = None
    external_id: Optional[str] = None  # remote ID (used for dedup if filename collides)
    extra: dict = field(default_factory=dict)


_SLUG_RE = re.compile(r"[^a-z0-9_]+")


def slugify(s: str, max_len: int = 60) -> str:
    """Make a filesystem-safe slug from a title."""
    s = (s or "").strip().lower().replace(" ", "_")
    s = _SLUG_RE.sub("", s)
    return s[:max_len].strip("_") or "untitled"


def filename_for(doc: FetchedDocument) -> str:
    """Inbox filename convention: YYYY-MM-DD_slug.txt."""
    return f"{doc.published_at.isoformat()}_{slugify(doc.title)}.txt"

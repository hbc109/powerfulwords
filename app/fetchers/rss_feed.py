"""Generic RSS / Atom feed fetcher.

Use for any source that publishes a feed: EIA This Week in Petroleum,
Reuters energy desk, Bloomberg free tier, IEA news, etc. Item link is
followed to fetch the full HTML body when the feed only carries a
summary.
"""

from __future__ import annotations

from datetime import date, datetime
from time import mktime
from typing import List, Optional

import feedparser
import requests
from bs4 import BeautifulSoup

from app.fetchers.base import USER_AGENT, FetchedDocument


def _feed_entry_published(entry) -> Optional[date]:
    for field in ("published_parsed", "updated_parsed", "created_parsed"):
        ts = entry.get(field)
        if ts:
            return datetime.fromtimestamp(mktime(ts)).date()
    return None


def _strip_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    # Collapse runs of blank lines
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return "\n".join(lines)


def fetch_full_article(url: str, timeout: int = 20) -> Optional[str]:
    try:
        resp = requests.get(
            url, headers={"User-Agent": USER_AGENT}, timeout=timeout
        )
        resp.raise_for_status()
    except Exception:
        return None
    return _strip_html(resp.text)


def fetch_rss(
    feed_url: str,
    source_id: str,
    source_bucket: str,
    limit: int = 25,
    since: Optional[date] = None,
    follow_links: bool = True,
    min_chars: int = 200,
) -> List[FetchedDocument]:
    """Pull entries from an RSS/Atom feed.

    If `follow_links` is True and the feed entry's `summary` is shorter
    than `min_chars`, fetch the linked article and use its stripped HTML
    as the body. Useful for feeds (e.g. EIA TWIP) that only carry titles.
    """
    parsed = feedparser.parse(feed_url, agent=USER_AGENT)
    docs: List[FetchedDocument] = []

    for entry in parsed.entries[:limit]:
        published = _feed_entry_published(entry)
        if published is None:
            continue
        if since is not None and published < since:
            continue

        title = (entry.get("title") or "").strip()
        summary = _strip_html(entry.get("summary") or "")
        body = summary

        if follow_links and len(body) < min_chars and entry.get("link"):
            full = fetch_full_article(entry["link"])
            if full:
                body = full

        if len(body) < min_chars:
            continue

        docs.append(FetchedDocument(
            source_id=source_id,
            source_bucket=source_bucket,
            published_at=published,
            title=title,
            text=body,
            url=entry.get("link"),
            external_id=entry.get("id") or entry.get("link"),
        ))
    return docs

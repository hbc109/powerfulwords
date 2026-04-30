"""Fetch Hacker News stories matching energy/oil keywords.

Uses the free Algolia HN search API (no auth). Each matching story
becomes one FetchedDocument: title + any submitted text + the linked
URL. The narrative extractor can then optionally follow the link,
though we keep things simple here and let the title+text carry the
narrative signal.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import List, Optional

import requests

from app.fetchers.base import USER_AGENT, FetchedDocument


SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"


def fetch_query(
    query: str,
    source_id: str,
    source_bucket: str = "social_open",
    limit: int = 30,
    since: Optional[date] = None,
    min_points: int = 0,
    min_chars: int = 80,
    timeout: int = 20,
) -> List[FetchedDocument]:
    """Search HN stories matching `query` newer than `since`."""
    params = {"query": query, "tags": "story", "hitsPerPage": min(100, limit)}
    if since is not None:
        ts = int(datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        params["numericFilters"] = f"created_at_i>={ts}"

    resp = requests.get(
        SEARCH_URL,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()

    docs: List[FetchedDocument] = []
    for hit in payload.get("hits", []):
        title = (hit.get("title") or "").strip()
        if not title:
            continue
        if (hit.get("points") or 0) < min_points:
            continue

        created_iso = hit.get("created_at")
        if not created_iso:
            continue
        try:
            dt = datetime.fromisoformat(created_iso.replace("Z", "+00:00"))
        except ValueError:
            continue
        d = dt.astimezone(timezone.utc).date()
        if since is not None and d < since:
            continue

        story_text = (hit.get("story_text") or "").strip()
        url = hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID')}"

        body_lines = [title]
        if story_text:
            body_lines.extend(["", story_text])
        body_lines.extend([
            "",
            f"HN points: {hit.get('points', 0)}, comments: {hit.get('num_comments', 0)}",
            f"URL: {url}",
        ])
        body = "\n".join(body_lines)
        if len(body) < min_chars:
            continue

        docs.append(FetchedDocument(
            source_id=source_id,
            source_bucket=source_bucket,
            published_at=d,
            title=title,
            text=body,
            url=url,
            external_id=f"hn_{hit.get('objectID')}",
            extra={
                "points": hit.get("points"),
                "num_comments": hit.get("num_comments"),
                "author": hit.get("author"),
                "query": query,
            },
        ))
    return docs

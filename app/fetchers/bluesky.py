"""Fetch Bluesky posts via the public AppView search API.

The `app.bsky.feed.searchPosts` endpoint on `public.api.bsky.app` does
not require auth. Posts are short (≤300 chars), so we bundle all posts
for a query within a single day into one FetchedDocument.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import List, Optional

import requests

from app.fetchers.base import USER_AGENT, FetchedDocument


SEARCH_URL = "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts"


def fetch_query(
    query: str,
    source_id: str,
    source_bucket: str = "social_open",
    limit: int = 100,
    since: Optional[date] = None,
    until: Optional[date] = None,
    min_chars: int = 200,
    timeout: int = 20,
    max_pages: int = 10,
) -> List[FetchedDocument]:
    """Search Bluesky for `query` and bundle posts by day.

    If `until` is given, paginates with `cursor` up to `max_pages` to
    walk older posts; both `since` and `until` are passed to the API
    so it filters server-side too.
    """
    base_params = {"q": query, "limit": min(100, limit), "sort": "latest"}
    if since is not None:
        base_params["since"] = datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    if until is not None:
        base_params["until"] = datetime.combine(until, datetime.min.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    pages_to_fetch = max_pages if until else 1
    all_posts = []
    cursor = None
    for _ in range(pages_to_fetch):
        params = dict(base_params)
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(
            SEARCH_URL, params=params,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        posts = payload.get("posts") or []
        all_posts.extend(posts)
        cursor = payload.get("cursor")
        if not cursor or not posts:
            break

    by_day: dict[date, list[dict]] = defaultdict(list)
    for post in all_posts:
        record = post.get("record") or {}
        created = record.get("createdAt")
        if not created:
            continue
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        except ValueError:
            continue
        d = dt.astimezone(timezone.utc).date()
        if since is not None and d < since:
            continue
        if until is not None and d > until:
            continue
        by_day[d].append(post)

    docs: List[FetchedDocument] = []
    for d, posts in by_day.items():
        posts.sort(key=lambda p: ((p.get("record") or {}).get("createdAt") or ""))
        lines = [
            f"Bluesky chatter for query '{query}' on {d.isoformat()}",
            f"({len(posts)} posts)",
            "",
        ]
        for p in posts:
            handle = (p.get("author") or {}).get("handle") or "anon"
            text = ((p.get("record") or {}).get("text") or "").strip().replace("\n", " ")
            if not text:
                continue
            likes = p.get("likeCount") or 0
            reposts = p.get("repostCount") or 0
            lines.append(f"@{handle} [♥{likes} ↻{reposts}]: {text}")

        body = "\n".join(lines)
        if len(body) < min_chars:
            continue

        slug_query = query.replace(" ", "_")[:40]
        docs.append(FetchedDocument(
            source_id=source_id,
            source_bucket=source_bucket,
            published_at=d,
            title=f"Bluesky '{query}' chatter — {d.isoformat()}",
            text=body,
            url=f"https://bsky.app/search?q={query}",
            external_id=f"bluesky_{slug_query}_{d.isoformat()}",
            extra={"query": query, "post_count": len(posts)},
        ))
    return docs

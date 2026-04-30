"""Fetch posts from oil-related subreddits via Reddit's public JSON API.

No auth needed for read-only listings. Returns each post (title +
selftext) as a FetchedDocument bound to the configured source_id.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import List, Optional

import requests

from app.fetchers.base import USER_AGENT, FetchedDocument


REDDIT_LISTING_URL = "https://www.reddit.com/r/{subreddit}/{listing}.json"
REDDIT_SEARCH_URL = "https://www.reddit.com/r/{subreddit}/search.json"


def fetch_subreddit(
    subreddit: str,
    source_id: str,
    source_bucket: str = "social_open",
    listing: str = "new",
    limit: int = 25,
    since: Optional[date] = None,
    query: Optional[str] = None,
    timeout: int = 20,
) -> List[FetchedDocument]:
    """Pull `limit` posts from r/<subreddit>. Filter to >= since if given.

    If `query` is provided, hits the subreddit search endpoint
    (restricted to the sub) — useful for pulling oil chatter out of
    noisy general subs like wallstreetbets or StockMarket.

    Skips link-only posts (no selftext) — narrative extraction needs body text.
    """
    if query:
        url = REDDIT_SEARCH_URL.format(subreddit=subreddit)
        params = {"q": query, "restrict_sr": "on", "sort": "new", "limit": limit}
    else:
        url = REDDIT_LISTING_URL.format(subreddit=subreddit, listing=listing)
        params = {"limit": limit}
    resp = requests.get(
        url,
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()

    docs: List[FetchedDocument] = []
    for child in payload.get("data", {}).get("children", []):
        d = child.get("data") or {}
        if d.get("stickied"):
            continue
        text = (d.get("selftext") or "").strip()
        title = (d.get("title") or "").strip()
        if not text:
            # Link-only post — skip (no narrative body to chunk).
            continue
        body = f"{title}\n\n{text}"
        if len(body) < 80:
            continue
        created = d.get("created_utc")
        if created is None:
            continue
        published = datetime.fromtimestamp(created, tz=timezone.utc).date()
        if since is not None and published < since:
            continue
        docs.append(FetchedDocument(
            source_id=source_id,
            source_bucket=source_bucket,
            published_at=published,
            title=title,
            text=body,
            url=f"https://www.reddit.com{d.get('permalink', '')}",
            external_id=d.get("id"),
            extra={
                "subreddit": subreddit,
                "score": d.get("score"),
                "author": d.get("author"),
                "num_comments": d.get("num_comments"),
            },
        ))
    return docs

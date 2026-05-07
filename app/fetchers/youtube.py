"""Fetch YouTube channel videos (auto-transcript) for ingest.

Uses the channel's free Atom feed (no API key required) to list recent
videos, then youtube_transcript_api to grab each video's auto-generated
transcript. Emits one FetchedDocument per video that:
  (1) is within the lookback window
  (2) has a title matching the keyword filter (default: oil-related)
  (3) has a transcript available

Channel IDs are the long-form `UC...` strings. Resolve from a
@handle by visiting youtube.com/<handle> and extracting the
<link rel="canonical"> meta tag (or use the doc string below).
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime, timezone
from typing import List, Optional

import feedparser
import requests

from app.fetchers.base import FetchedDocument


# YouTube's RSS endpoint requires a browser-like User-Agent;
# the project's default UA returns 404 from this endpoint.
BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

DEFAULT_OIL_KEYWORDS = [
    "oil", "crude", "brent", "wti", "opec", "energy", "gasoline",
    "diesel", "petroleum", "refinery", "refining", "shale",
    "saudi", "iran", "russia", "hormuz", "drilling", "tanker",
    "pipeline", "natural gas", "lng",
]


def _video_id_from_entry(entry) -> Optional[str]:
    vid = entry.get("yt_videoid") or entry.get("yt_videoId")
    if vid:
        return vid
    link = entry.get("link") or ""
    m = re.search(r"v=([\w-]{6,})", link)
    return m.group(1) if m else None


def _title_matches(title: str, keywords: List[str]) -> bool:
    t = title.lower()
    return any(k in t for k in keywords)


def fetch_channel(
    channel_id: str,
    source_id: str,
    source_bucket: str = "social_open",
    limit: int = 10,
    since: Optional[date] = None,
    keyword_filter: Optional[List[str]] = None,
    languages: Optional[List[str]] = None,
    transcript_throttle_sec: float = 0.5,
    timeout: int = 20,
) -> List[FetchedDocument]:
    """Pull recent videos from one channel and fetch their transcripts.

    Filtering at fetch time avoids burning transcript API requests on
    obviously off-topic videos (CNBC posts a lot of non-oil content).
    """
    keywords = keyword_filter if keyword_filter is not None else DEFAULT_OIL_KEYWORDS
    languages = languages or ["en", "en-US", "en-GB"]

    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    resp = requests.get(rss_url, headers={"User-Agent": BROWSER_UA}, timeout=timeout)
    resp.raise_for_status()
    parsed = feedparser.parse(resp.text)
    channel_title = parsed.feed.get("title", channel_id)

    # Lazy import — only load if we have any candidates
    api = None

    docs: List[FetchedDocument] = []
    for entry in parsed.entries[:limit]:
        title = (entry.get("title") or "").strip()
        if not title:
            continue
        if keywords and not _title_matches(title, keywords):
            continue

        # Published date
        pub_struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if not pub_struct:
            continue
        published = datetime(*pub_struct[:6], tzinfo=timezone.utc).date()
        if since is not None and published < since:
            continue

        video_id = _video_id_from_entry(entry)
        if not video_id:
            continue

        # Fetch transcript (lazy-init API client)
        if api is None:
            from youtube_transcript_api import YouTubeTranscriptApi
            api = YouTubeTranscriptApi()
        try:
            fetched = api.fetch(video_id, languages=languages)
        except Exception as e:
            # Common failures: TranscriptsDisabled, NoTranscriptFound, VideoUnavailable
            continue
        time.sleep(transcript_throttle_sec)  # be polite

        try:
            transcript_text = " ".join(snip.text for snip in fetched.snippets)
        except AttributeError:
            transcript_text = " ".join(s["text"] for s in fetched.to_raw_data())
        transcript_text = re.sub(r"\s+", " ", transcript_text).strip()
        if len(transcript_text) < 200:
            continue

        author = entry.get("author") or channel_title
        body = (
            f"{title}\n\n"
            f"Channel: {channel_title}\n"
            f"Author: {author}\n"
            f"Video: https://www.youtube.com/watch?v={video_id}\n\n"
            f"{transcript_text}"
        )

        docs.append(FetchedDocument(
            source_id=source_id,
            source_bucket=source_bucket,
            published_at=published,
            title=title,
            text=body,
            url=f"https://www.youtube.com/watch?v={video_id}",
            external_id=f"yt_{video_id}",
            extra={
                "channel_title": channel_title,
                "channel_id": channel_id,
                "video_id": video_id,
                "transcript_chars": len(transcript_text),
            },
        ))
    return docs

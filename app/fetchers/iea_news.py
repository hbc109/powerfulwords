"""Fetch IEA public news / press releases.

The full IEA Oil Market Report is paywalled, but news, press releases,
and the public OMR summary are free at https://www.iea.org/news.

Same edge-protection caveat as OPEC: data-center IPs may be blocked.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from app.fetchers.base import USER_AGENT, FetchedDocument


LISTING_URL = "https://www.iea.org/news"
ROOT = "https://www.iea.org"

DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})")


def _parse_date(text: str) -> Optional[date]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", fmt).date()
        except ValueError:
            continue
    return None


def _get(url: str, timeout: int = 20) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    if resp.status_code in (403, 503):
        raise PermissionError(
            f"IEA blocked the request ({resp.status_code}). "
            "Common from data-center IPs; try from a normal network."
        )
    resp.raise_for_status()
    return resp.text


def fetch_iea_news(
    source_id: str = "iea_omr",
    source_bucket: str = "official_reports",
    limit: int = 10,
    since: Optional[date] = None,
    keyword_filter: Optional[List[str]] = None,
) -> List[FetchedDocument]:
    """Pull IEA news entries. Optionally filter to titles matching any of
    `keyword_filter` (case-insensitive, default: oil-related terms)."""
    listing_html = _get(LISTING_URL)
    soup = BeautifulSoup(listing_html, "html.parser")

    if keyword_filter is None:
        keyword_filter = ["oil", "crude", "petroleum", "energy", "opec", "gas"]
    keyword_filter = [k.lower() for k in keyword_filter]

    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/news/" not in href:
            continue
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 6:
            continue
        if not any(k in title.lower() for k in keyword_filter):
            continue
        absolute = urljoin(ROOT, href)
        parent = a.find_parent()
        parent_text = parent.get_text(" ", strip=True) if parent else ""
        candidates.append({
            "url": absolute,
            "title": title,
            "date": _parse_date(parent_text) or _parse_date(title),
        })

    seen = set()
    ordered = []
    for c in candidates:
        if c["url"] in seen:
            continue
        seen.add(c["url"])
        ordered.append(c)
    ordered.sort(key=lambda c: c["date"] or date.min, reverse=True)

    docs: List[FetchedDocument] = []
    for c in ordered[: limit * 2]:
        if since is not None and c["date"] is not None and c["date"] < since:
            continue
        try:
            body_html = _get(c["url"])
        except Exception:
            continue
        bs = BeautifulSoup(body_html, "html.parser")
        for tag in bs(["script", "style", "nav", "header", "footer", "aside", "form"]):
            tag.decompose()
        body = "\n".join(ln.strip() for ln in bs.get_text("\n", strip=True).splitlines() if ln.strip())
        if len(body) < 250:
            continue
        docs.append(FetchedDocument(
            source_id=source_id,
            source_bucket=source_bucket,
            published_at=c["date"] or date.today(),
            title=c["title"],
            text=body,
            url=c["url"],
            external_id=c["url"],
        ))
        if len(docs) >= limit:
            break
    return docs

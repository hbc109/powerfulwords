"""Generic state-news-agency HTML scraper.

Used for SHANA (Iran Petroleum Ministry) and SPA (Saudi Press Agency).
Both publish narrative press releases from official sources, free, but
their HTML is highly variable. This scraper does a best-effort:

- GET the listing page
- Pick the first N <a href> children that look like article links
  (filtered by a path-substring you provide)
- Follow each link, strip HTML, return as FetchedDocument

If the agency layout changes, just edit the listing_url and
link_filter and re-run. PermissionError on 403/503 (typical for
data-center IPs).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from app.fetchers.base import USER_AGENT, FetchedDocument


DATE_PATTERNS = [
    (re.compile(r"(\d{4})-(\d{2})-(\d{2})"), "%Y-%m-%d"),
    (re.compile(r"(\d{2})/(\d{2})/(\d{4})"), "%d/%m/%Y"),
    (re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})"), "%d %B %Y"),
]


def _parse_date_anywhere(text: str) -> Optional[date]:
    if not text:
        return None
    for regex, fmt in DATE_PATTERNS:
        m = regex.search(text)
        if not m:
            continue
        try:
            return datetime.strptime(m.group(0), fmt).date()
        except ValueError:
            continue
    return None


def _get(url: str, timeout: int = 25) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    if resp.status_code in (403, 503):
        raise PermissionError(
            f"{urlparse(url).netloc} blocked the request ({resp.status_code}). "
            "Common from data-center IPs; try from a normal network."
        )
    resp.raise_for_status()
    return resp.text


def fetch_agency(
    listing_url: str,
    link_filter: str,
    source_id: str,
    source_bucket: str = "official_data",
    limit: int = 10,
    since: Optional[date] = None,
    min_chars: int = 200,
) -> List[FetchedDocument]:
    listing_html = _get(listing_url)
    soup = BeautifulSoup(listing_html, "html.parser")

    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if link_filter not in href:
            continue
        text = a.get_text(" ", strip=True)
        if not text or len(text) < 8:
            continue
        absolute = urljoin(listing_url, href)
        parent = a.find_parent()
        parent_text = parent.get_text(" ", strip=True) if parent else ""
        candidates.append({
            "url": absolute,
            "title": text,
            "date": _parse_date_anywhere(parent_text) or _parse_date_anywhere(text),
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
        if len(body) < min_chars:
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

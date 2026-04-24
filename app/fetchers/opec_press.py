"""Scrape OPEC press releases.

OPEC publishes press releases as HTML at https://www.opec.org/...
The listing page contains links + dates; each release page has the
full body. This fetcher scrapes the listing, then follows each link.

Note: OPEC's CDN (Cloudflare/Akamai class) often blocks data-center
IPs. From a normal home / office IP it works fine; from VPS or some
WSL setups it may return 403. The fetcher raises a clear error in
that case.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from app.fetchers.base import USER_AGENT, FetchedDocument


LISTING_URL = "https://www.opec.org/opec_web/en/press_room/8.htm"
ROOT = "https://www.opec.org"

DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})")


def _parse_date(text: str) -> Optional[date]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    day, month, year = m.group(1), m.group(2), m.group(3)
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(f"{day} {month} {year}", fmt).date()
        except ValueError:
            continue
    return None


def _get(url: str, timeout: int = 20) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    if resp.status_code in (403, 503):
        raise PermissionError(
            f"OPEC blocked the request ({resp.status_code}). "
            "This commonly happens from data-center IPs; try from a normal network."
        )
    resp.raise_for_status()
    return resp.text


def fetch_press_releases(
    source_id: str = "opec_press_releases",
    source_bucket: str = "official_data",
    limit: int = 10,
    since: Optional[date] = None,
) -> List[FetchedDocument]:
    listing_html = _get(LISTING_URL)
    soup = BeautifulSoup(listing_html, "html.parser")

    # The listing layout uses anchor tags inside a press-release section.
    # We extract every <a href> that looks like an internal press release page.
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        if "press_room" not in href and "/press" not in href:
            continue
        absolute = urljoin(ROOT, href)
        text = a.get_text(" ", strip=True)
        if not text:
            continue
        # try to find a sibling/ancestor date string
        parent_text = a.find_parent().get_text(" ", strip=True) if a.find_parent() else ""
        parsed = _parse_date(parent_text) or _parse_date(text)
        candidates.append({"url": absolute, "title": text, "date": parsed})

    # De-dup by URL, keep newest first.
    seen_urls = set()
    ordered = []
    for c in candidates:
        if c["url"] in seen_urls:
            continue
        seen_urls.add(c["url"])
        ordered.append(c)
    ordered.sort(key=lambda c: c["date"] or date.min, reverse=True)

    docs: List[FetchedDocument] = []
    for c in ordered[: limit * 2]:  # over-pull because some won't have body
        if since is not None and c["date"] is not None and c["date"] < since:
            continue
        try:
            body_html = _get(c["url"])
        except Exception:
            continue
        body_soup = BeautifulSoup(body_html, "html.parser")
        for tag in body_soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()
        body = body_soup.get_text("\n", strip=True)
        body = "\n".join(ln.strip() for ln in body.splitlines() if ln.strip())
        if len(body) < 200:
            continue
        published = c["date"] or date.today()
        docs.append(FetchedDocument(
            source_id=source_id,
            source_bucket=source_bucket,
            published_at=published,
            title=c["title"],
            text=body,
            url=c["url"],
            external_id=c["url"],
        ))
        if len(docs) >= limit:
            break
    return docs

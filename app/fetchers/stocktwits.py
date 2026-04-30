"""Fetch StockTwits messages for energy cashtags via the public stream API.

No auth needed for reads. Individual messages are short, so we bundle
all messages for a symbol within a single day into one FetchedDocument
— the narrative extractor sees coherent chatter rather than 100
disconnected fragments.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import List, Optional

import requests

from app.fetchers.base import USER_AGENT, FetchedDocument


STREAM_URL = "https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"


def fetch_symbol(
    symbol: str,
    source_id: str,
    source_bucket: str = "social_open",
    limit: int = 30,
    since: Optional[date] = None,
    min_chars: int = 200,
    timeout: int = 20,
) -> List[FetchedDocument]:
    """Pull recent messages for `$symbol` (e.g. 'CL_F', 'USO') and bundle by day.

    StockTwits caps `limit` at ~30 per request. One FetchedDocument per
    distinct day; bundles older than `since` are dropped.
    """
    resp = requests.get(
        STREAM_URL.format(symbol=symbol),
        params={"limit": limit},
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=timeout,
    )
    resp.raise_for_status()
    payload = resp.json()
    if (payload.get("response") or {}).get("status") != 200:
        return []

    by_day: dict[date, list[dict]] = defaultdict(list)
    for msg in payload.get("messages", []):
        created = msg.get("created_at")
        if not created:
            continue
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        except ValueError:
            continue
        d = dt.astimezone(timezone.utc).date()
        if since is not None and d < since:
            continue
        by_day[d].append(msg)

    docs: List[FetchedDocument] = []
    for d, msgs in by_day.items():
        msgs.sort(key=lambda m: m.get("created_at") or "")
        lines = [
            f"StockTwits chatter for ${symbol} on {d.isoformat()}",
            f"({len(msgs)} messages)",
            "",
        ]
        for m in msgs:
            user = (m.get("user") or {}).get("username") or "anon"
            sentiment = (m.get("entities") or {}).get("sentiment") or {}
            tag = ""
            if isinstance(sentiment, dict) and sentiment.get("basic"):
                tag = f" [{sentiment['basic']}]"
            body = (m.get("body") or "").strip().replace("\n", " ")
            if not body:
                continue
            lines.append(f"@{user}{tag}: {body}")

        text = "\n".join(lines)
        if len(text) < min_chars:
            continue

        docs.append(FetchedDocument(
            source_id=source_id,
            source_bucket=source_bucket,
            published_at=d,
            title=f"StockTwits ${symbol} chatter — {d.isoformat()}",
            text=text,
            url=f"https://stocktwits.com/symbol/{symbol}",
            external_id=f"stocktwits_{symbol}_{d.isoformat()}",
            extra={"symbol": symbol, "msg_count": len(msgs)},
        ))
    return docs

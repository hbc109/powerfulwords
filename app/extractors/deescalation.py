"""De-escalation direction guard.

Risk-escalation topics (war, supply outage, shipping disruption, sanctions)
default to BULLISH in the keyword topic rules. But an article about the risk
*resolving* — ceasefire, Strait reopening, supply restored, sanctions lifted —
is BEARISH for crude, not bullish: the geopolitical risk premium unwinds.

The keyword extractor can't see that (a ceasefire matches the war keywords and
inherits the topic's bullish default). This deterministic guard flips the sign.
It is intentionally conservative — it only flips a *bullish* event on a
risk-escalation topic, only when de-escalation language is present AND not
out-weighed by re-escalation / collapse language ("ceasefire collapses",
"truce violated", "back to war"). Used as a backstop for BOTH the rule and the
LLM extractor.
"""

from __future__ import annotations

# Topics whose escalation direction is bullish (risk-on). A *resolution* of any
# of these is bearish for crude.
RISK_ESCALATION_TOPICS = {
    "geopolitical_risk",
    "shipping_disruption",
    "supply_disruption",
    "sanctions_risk",
    "weather_risk",
}

# De-escalation / resolution language (substring match, lower-cased).
_DEESCALATION = [
    "ceasefire", "cease-fire", "cease fire", "truce", "peace deal",
    "peace agreement", "peace accord", "de-escalat", "deescalat",
    "reopen", "reopened", "reopening", "restored", "restoration",
    "supply restored", "resume supply", "resumed supply", "resuming supply",
    "sanctions lifted", "lifted sanctions", "lifting sanctions",
    "sanctions eased", "eased sanctions", "easing sanctions",
    "ease tension", "eased tension", "easing tension", "tensions ease",
    "normaliz", "stand down", "stood down", "withdraw", "withdrew",
    "withdrawal", "pull out", "pulled out", "war ends", "war ended",
    "war is over", "conflict ends", "conflict ended", "detente",
    "停火", "和平", "恢复供应", "解除制裁", "缓和", "撤军",
]

# If these appear, the de-escalation is failing or reversing — do NOT flip.
_REVERSAL = [
    "collapse", "collapsed", "collapses", "violat", "broke down",
    "breaks down", "broken down", "fail", "reject", "no ceasefire",
    "no truce", "breach", "resume fighting", "resumed strikes",
    "resumes strikes", "back to war", "shattered", "threat to", "fragile",
    "破裂", "失败", "升级",
]


def _count(text: str, terms) -> int:
    return sum(text.count(t) for t in terms)


def resolve_direction(text: str, topic: str, direction: str) -> tuple[str, bool]:
    """Return (direction, flipped).

    Flip bullish -> bearish on a risk-escalation topic when de-escalation
    language is present and not out-weighed by re-escalation language.
    """
    if topic not in RISK_ESCALATION_TOPICS or direction != "bullish":
        return direction, False
    low = (text or "").lower()
    de = _count(low, _DEESCALATION)
    if de == 0:
        return direction, False
    rev = _count(low, _REVERSAL)
    if de > rev:
        return "bearish", True
    return direction, False

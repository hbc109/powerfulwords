from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class ProposedSubtheme(BaseModel):
    label: str = Field(description="Snake-case identifier for the proposed subtheme.")
    description: str = Field(description="One-sentence description of what this subtheme captures.")
    parent_theme: str = Field(description="Best-fit existing main theme, or a newly-proposed one.")
    suggested_keywords: List[str] = Field(
        default_factory=list,
        description="3-10 keyword phrases that would trigger this subtheme.",
    )
    example_evidence: List[str] = Field(
        default_factory=list,
        description="Short verbatim excerpts from the input chunks that would map to this subtheme.",
    )
    direction_bias: Optional[str] = Field(
        default=None,
        description="Default direction this subtheme tends to push prices: bullish, bearish, mixed, or neutral.",
    )


class ProposedTheme(BaseModel):
    label: str = Field(description="Snake-case identifier for a brand-new main theme not in the existing list.")
    description: str = Field(description="One-sentence description of what this theme covers.")
    suggested_subthemes: List[str] = Field(default_factory=list)


class ThemeDiscoveryResult(BaseModel):
    summary: str = Field(description="Two-sentence summary of what the model found across the input chunks.")
    new_subthemes: List[ProposedSubtheme] = Field(default_factory=list)
    new_themes: List[ProposedTheme] = Field(default_factory=list)
    coverage_note: Optional[str] = Field(
        default=None,
        description="Optional note on what fraction of input chunks already fit the existing taxonomy.",
    )

"""Composite signal: regime-conditional weighted sum of factors.

Reads `regime_factor_weights` from app/config/strategy_config.json and
combines a narrative score plus any number of factor scores into a
single directional signal, with a per-factor breakdown for the UI.

All factor values are expected on roughly the same scale (z-score,
i.e. roughly [-2, 2]) so weights are comparable across factors.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "strategy_config.json"


def _load_weights() -> dict:
    cfg = json.loads(CONFIG_PATH.read_text())
    return cfg.get("regime_factor_weights", {})


def composite_score(
    regime: str,
    narrative_score: Optional[float],
    factors: dict,
    *,
    weights_override: Optional[dict] = None,
) -> dict:
    """Combine narrative + factors using the weights for `regime`.

    `factors` is e.g. {"term_structure": 0.45, "momentum": -0.8}.
    Missing factors get zero contribution; extras are ignored. Weights
    are renormalized over the factors actually present so the total
    stays on the same scale even when a factor is unavailable.

    Returns:
      {
        "total": float,
        "regime": str,
        "breakdown": [
          {"factor": str, "value": float, "weight": float, "contribution": float},
          ...
        ],
      }
    """
    table = weights_override if weights_override is not None else _load_weights()
    if regime not in table:
        raise KeyError(f"No regime_factor_weights entry for regime {regime!r}")
    weights = {k: v for k, v in table[regime].items() if not k.startswith("_")}

    inputs = dict(factors)
    if narrative_score is not None:
        inputs["narrative"] = narrative_score

    available = {k: w for k, w in weights.items() if k in inputs and inputs[k] is not None}
    total_weight = sum(available.values())
    if total_weight == 0:
        return {"total": 0.0, "regime": regime, "breakdown": []}

    breakdown = []
    total = 0.0
    for k, w in available.items():
        norm_w = w / total_weight
        v = float(inputs[k])
        contrib = norm_w * v
        total += contrib
        breakdown.append({"factor": k, "value": v, "weight": norm_w, "contribution": contrib})

    breakdown.sort(key=lambda r: abs(r["contribution"]), reverse=True)
    return {"total": total, "regime": regime, "breakdown": breakdown}

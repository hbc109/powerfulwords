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


def _resolve_weights_for_symbol(table: dict, symbol: str, regime: str) -> dict:
    """Look up symbol-specific regime weights, falling back to WTI."""
    sym_table = table.get(symbol)
    if not isinstance(sym_table, dict):
        sym_table = table.get("WTI")  # default
    if not isinstance(sym_table, dict):
        raise KeyError("regime_factor_weights has no per-symbol tables (expected at least 'WTI').")
    if regime not in sym_table:
        raise KeyError(f"No regime_factor_weights[{symbol!r}][{regime!r}] entry.")
    return sym_table[regime]


def composite_score(
    symbol: str,
    regime: str,
    narrative_score: Optional[float],
    factors: dict,
    *,
    weights_override: Optional[dict] = None,
) -> dict:
    """Combine narrative + factors using the weights for (symbol, regime).

    `factors` is e.g. {"term_structure": 0.45, "momentum": -0.8}.
    Missing factors contribute zero — weights are used as configured,
    NOT renormalized to fill the gap. This keeps the composite signal
    stationary across history: pre-COT-era rows (when positioning
    data didn't exist) get a smaller signal magnitude than full-coverage
    rows, instead of silently rebalancing the remaining factors to
    full weight (which would make early-era and late-era backtest
    contributions structurally incomparable).

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
    regime_weights = _resolve_weights_for_symbol(table, symbol, regime)
    weights = {k: v for k, v in regime_weights.items() if not k.startswith("_")}

    inputs = dict(factors)
    if narrative_score is not None:
        inputs["narrative"] = narrative_score

    breakdown = []
    total = 0.0
    for k, w in weights.items():
        if k not in inputs or inputs[k] is None:
            continue
        v = float(inputs[k])
        contrib = w * v
        total += contrib
        breakdown.append({"factor": k, "value": v, "weight": w, "contribution": contrib})

    breakdown.sort(key=lambda r: abs(r["contribution"]), reverse=True)
    return {"total": total, "regime": regime, "breakdown": breakdown}

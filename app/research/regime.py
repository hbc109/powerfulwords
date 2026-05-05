"""Regime classification for crude / product price series.

Multi-label by design: a parabolic blow-off can be `trend_up` AND
`stretched_up` AND `shock` simultaneously. Storing all tags lets us
slice later. For dashboard display we collapse to a single
`primary_regime` via priority order.

Detection rules (defaults — tuneable):
  trend_up        ADX ≥ 20 AND close > SMA50 AND SMA50 slope ≥ 0
  trend_down      ADX ≥ 20 AND close < SMA50 AND SMA50 slope ≤ 0
  range           ADX < 20
  stretched_up    RSI(14) > 75 OR Bollinger %B > 1.0
  stretched_down  RSI(14) < 25 OR Bollinger %B < 0
  shock           ATR(14) / mean(ATR(14), 60d) > 1.5

Priority for `primary_regime`:
  shock > stretched_up > stretched_down > trend_up > trend_down > range
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.research.indicators import (
    adx, atr, bollinger_pctb, rsi, sma, sma_slope,
)


PRIMARY_PRIORITY = [
    "shock",
    "stretched_up",
    "stretched_down",
    "trend_up",
    "trend_down",
    "range",
]


def compute_regimes(
    df: pd.DataFrame,
    *,
    adx_trend_threshold: float = 20.0,
    rsi_overbought: float = 75.0,
    rsi_oversold: float = 25.0,
    pctb_overbought: float = 1.0,
    pctb_oversold: float = 0.0,
    shock_atr_ratio: float = 1.5,
    atr_baseline_window: int = 60,
) -> pd.DataFrame:
    """Per-date regime tags for one symbol.

    `df` must contain columns date, high, low, close (other columns
    ignored). Returns a DataFrame with one row per date including the
    raw indicator values (for transparency) and a comma-separated
    `regime_tags` plus a single `primary_regime`.
    """
    df = df.sort_values("date").reset_index(drop=True).copy()
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    rsi14 = rsi(close, period=14)
    adx14 = adx(high, low, close, period=14)
    atr14 = atr(high, low, close, period=14)
    pctb = bollinger_pctb(close, period=20, n_std=2.0)
    sma50 = sma(close, period=50)
    slope50 = sma_slope(close, period=50, lookback=5)
    atr_baseline = atr14.rolling(atr_baseline_window, min_periods=20).mean()
    atr_ratio = atr14 / atr_baseline.replace(0, np.nan)

    out = pd.DataFrame({
        "date": df["date"],
        "close": close,
        "rsi14": rsi14.round(3),
        "adx14": adx14.round(3),
        "atr14": atr14.round(4),
        "atr_ratio": atr_ratio.round(3),
        "bb_pctb": pctb.round(4),
        "sma50": sma50.round(4),
        "sma50_slope_5d_pct": slope50.round(4),
    })

    # Tag detection
    is_trend = adx14 >= adx_trend_threshold
    above_sma = close > sma50
    rising_sma = slope50 >= 0

    out["trend_up"]       = (is_trend & above_sma & rising_sma).fillna(False)
    out["trend_down"]     = (is_trend & ~above_sma & ~rising_sma).fillna(False)
    out["range"]          = (adx14 < adx_trend_threshold).fillna(False)
    out["stretched_up"]   = ((rsi14 > rsi_overbought) | (pctb > pctb_overbought)).fillna(False)
    out["stretched_down"] = ((rsi14 < rsi_oversold) | (pctb < pctb_oversold)).fillna(False)
    out["shock"]          = (atr_ratio > shock_atr_ratio).fillna(False)

    def collect_tags(row) -> str:
        return ",".join([k for k in PRIMARY_PRIORITY if bool(row.get(k, False))])

    def pick_primary(row) -> str:
        for k in PRIMARY_PRIORITY:
            if bool(row.get(k, False)):
                return k
        return "range"

    out["regime_tags"] = out.apply(collect_tags, axis=1)
    out["primary_regime"] = out.apply(pick_primary, axis=1)
    return out

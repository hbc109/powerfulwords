"""Compact pandas implementations of the technical indicators we use
for regime classification. Pure pandas/numpy — no third-party TA library.

Inputs: a price DataFrame with columns `high`, `low`, `close`, indexed
by date. Outputs: Series aligned to that index.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's RSI."""
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    # Wilder smoothing = EMA with alpha = 1/period
    avg_up = up.ewm(alpha=1 / period, adjust=False).mean()
    avg_down = down.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_up / avg_down.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50.0)


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's ATR (Average True Range)."""
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's ADX. Returns the smoothed directional index (0–100)."""
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = ((up_move > down_move) & (up_move > 0)).astype(float) * up_move.clip(lower=0)
    minus_dm = ((down_move > up_move) & (down_move > 0)).astype(float) * down_move.clip(lower=0)
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr_w = tr.ewm(alpha=1 / period, adjust=False).mean().replace(0, np.nan)
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_w
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_w
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1 / period, adjust=False).mean().fillna(0.0)


def bollinger_pctb(close: pd.Series, period: int = 20, n_std: float = 2.0) -> pd.Series:
    """Bollinger %B: (close - lower) / (upper - lower)."""
    sma = close.rolling(period, min_periods=period).mean()
    std = close.rolling(period, min_periods=period).std(ddof=0)
    upper = sma + n_std * std
    lower = sma - n_std * std
    width = (upper - lower).replace(0, np.nan)
    return ((close - lower) / width).fillna(0.5)


def sma(close: pd.Series, period: int = 50) -> pd.Series:
    return close.rolling(period, min_periods=period).mean()


def sma_slope(close: pd.Series, period: int = 50, lookback: int = 5) -> pd.Series:
    """Slope of the SMA over `lookback` days, expressed as percent change."""
    s = sma(close, period)
    return (s - s.shift(lookback)) / s.shift(lookback) * 100

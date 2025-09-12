import logging
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Tuple

def find_swing_points(df: pd.DataFrame, order: int = 5) -> Tuple[pd.Series, pd.Series]:
    if len(df) < (2 * order + 1):
        return pd.Series(dtype=float), pd.Series(dtype=float)
    highs = df['high'][(df['high'] == df['high'].rolling(2 * order + 1, center=True, min_periods=2 * order + 1).max())]
    lows = df['low'][(df['low'] == df['low'].rolling(2 * order + 1, center=True, min_periods=2 * order + 1).min())]
    return highs, lows

def find_fair_value_gaps(df: pd.DataFrame) -> List[Dict]:
    if len(df) < 3: return []
    high_minus_2 = df['high'].shift(2)
    low_minus_2 = df['low'].shift(2)
    fvg_candle_timestamp = df.index.to_series().shift(1)
    bullish_mask = df['low'] > high_minus_2
    bullish_gaps = pd.DataFrame({'type': 'bullish', 'bottom': high_minus_2[bullish_mask], 'top': df['low'][bullish_mask], 'timestamp': fvg_candle_timestamp[bullish_mask]})
    bearish_mask = df['high'] < low_minus_2
    bearish_gaps = pd.DataFrame({'type': 'bearish', 'bottom': df['high'][bearish_mask], 'top': low_minus_2[bearish_mask], 'timestamp': fvg_candle_timestamp[bearish_mask]})
    return pd.concat([bullish_gaps, bearish_gaps]).sort_index().dropna().to_dict('records')

def find_mss(df: pd.DataFrame, bias: str, lookback: int = 15, swing_orders: Optional[Dict] = None) -> Optional[Dict]:
    if len(df) < lookback: return None
    swing_orders = swing_orders or {'trending': 5, 'ranging': 3, 'volatile': 2}
    regime = get_market_regime(df)
    swing_order = swing_orders.get(regime.split('-')[0].lower(), 5)
    recent_df = df.tail(lookback)
    swing_highs, swing_lows = find_swing_points(recent_df, order=swing_order)

    if bias == 'buy' and not swing_highs.empty:
        last_swing_high_level = swing_highs.iloc[-1]
        break_candles = recent_df[(recent_df.index > swing_highs.index[-1]) & (recent_df['high'] > last_swing_high_level)]
        if not break_candles.empty:
            return {'type': 'bullish', 'level': last_swing_high_level, 'break_candle_ts': break_candles.index[0]}
    elif bias == 'sell' and not swing_lows.empty:
        last_swing_low_level = swing_lows.iloc[-1]
        break_candles = recent_df[(recent_df.index > swing_lows.index[-1]) & (recent_df['low'] < last_swing_low_level)]
        if not break_candles.empty:
            return {'type': 'bearish', 'level': last_swing_low_level, 'break_candle_ts': break_candles.index[0]}
    return None

def get_market_regime(df: pd.DataFrame, period: int = 20) -> str:
    if len(df) < period * 2: return "Not Enough Data"
    ema = df['close'].ewm(span=period, adjust=False).mean()
    atr = calculate_atr(df, 14).iloc[-1]
    avg_atr = calculate_atr(df.iloc[:-14], 50).iloc[-1]
    if atr > avg_atr * 1.75: return "VOLATILE"
    ema_slope = (ema.iloc[-1] - ema.iloc[-5]) / 5 if len(ema) > 5 else 0
    if abs(ema_slope) < (ema.iloc[-1] * 0.0005): return "RANGING"
    return "UP-TRENDING" if ema_slope > 0 else "DOWN-TRENDING"

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    if len(df) < period: return pd.Series([0] * len(df), index=df.index)
    tr = pd.concat([df['high'] - df['low'], abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def find_order_blocks(df: pd.DataFrame, displacement_factor: float = 1.0) -> List[Dict]:
    obs = []
    if len(df) < 4: return obs
    avg_body_size = abs(df['close'] - df['open']).rolling(window=20, min_periods=5).mean()
    for i in range(2, len(df) - 1):
        ob_candle, candle_after, displacement_candle = df.iloc[i-1], df.iloc[i], df.iloc[i+1]
        displacement_size = abs(displacement_candle['close'] - displacement_candle['open'])
        if displacement_size > (avg_body_size.iloc[i+1] * displacement_factor):
            if ob_candle['close'] < ob_candle['open'] and candle_after['close'] > candle_after['open']:
                obs.append({'type': 'Bullish', 'top': ob_candle['open'], 'bottom': ob_candle['low'], 'timestamp': ob_candle.name, 'displacement_close': displacement_candle['close'], 'open': ob_candle['open']})
            elif ob_candle['close'] > ob_candle['open'] and candle_after['close'] < candle_after['open']:
                obs.append({'type': 'Bearish', 'top': ob_candle['high'], 'bottom': ob_candle['open'], 'timestamp': ob_candle.name, 'displacement_close': displacement_candle['close'], 'open': ob_candle['open']})
    return obs

def get_dynamic_target(df: pd.DataFrame, current_price: float, side: str, lookback: int = 100, swing_orders: Optional[Dict] = None) -> Optional[float]:
    if df.empty or len(df) < lookback: return None
    swing_orders = swing_orders or {'trending': 5, 'ranging': 3, 'volatile': 2}
    regime = get_market_regime(df)
    swing_order = swing_orders.get(regime.split('-')[0].lower(), 5)
    highs, lows = find_swing_points(df.tail(lookback), order=swing_order)
    if side == 'buy' and not (targets := highs[highs > current_price]).empty: return targets.min()
    if side == 'sell' and not (targets := lows[lows < current_price]).empty: return targets.max()
    return None

def analyze_market_structure(df: pd.DataFrame, order=5) -> Tuple[str, Optional[float], Optional[float]]:
    highs, lows = find_swing_points(df, order=order)
    if len(highs) < 2 or len(lows) < 2: return "Neutral", None, None
    last_high, prev_high = highs.iloc[-1], highs.iloc[-2]
    last_low, prev_low = lows.iloc[-1], lows.iloc[-2]
    if last_high > prev_high and last_low > prev_low: return "Bullish", last_high, None
    if last_low < prev_low and last_high < prev_high: return "Bearish", last_low, None
    return "Neutral", None, None

def is_volume_significant(df: pd.DataFrame, timestamp: pd.Timestamp, factor: float = 1.5, lookback: int = 20) -> bool:
    if 'volume' not in df.columns or len(df) < lookback: return True
    recent_df = df.loc[df.index < timestamp].tail(lookback)
    if recent_df.empty: return True
    return df.loc[timestamp]['volume'] > (recent_df['volume'].mean() * factor)

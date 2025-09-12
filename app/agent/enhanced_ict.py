import logging
import pandas as pd
from typing import Dict, List, Optional, Tuple
from .analysis import find_swing_points, calculate_atr, analyze_market_structure

def calculate_confluence_score(htf_data: pd.DataFrame, ltf_data: pd.DataFrame, poi_range: Tuple[float, float], trade_direction: str) -> float:
    score = 30.0
    htf_trend, _, _ = analyze_market_structure(htf_data)
    ltf_trend, _, _ = analyze_market_structure(ltf_data)
    if htf_trend.lower().startswith(trade_direction): score += 20.0
    if ltf_trend.lower().startswith(trade_direction): score += 10.0
    
    pd_zones = find_premium_discount_zones(find_swing_points(htf_data))
    if pd_zones:
        poi_mid = (poi_range[0] + poi_range[1]) / 2
        if trade_direction == 'buy' and poi_mid < pd_zones.get('discount_level', float('inf')): score += 15.0
        elif trade_direction == 'sell' and poi_mid > pd_zones.get('premium_level', float('-inf')): score += 15.0

    return min(score, 100.0)

def find_premium_discount_zones(swing_points: Tuple[pd.Series, pd.Series]) -> Optional[Dict]:
    swing_highs, swing_lows = swing_points
    if len(swing_highs) < 2 or len(swing_lows) < 2: return None
    range_high, range_low = swing_highs.tail(10).max(), swing_lows.tail(10).min()
    if (range_size := range_high - range_low) <= 0: return None
    
    return {
        'equilibrium': range_low + (range_size * 0.5),
        'premium_level': range_low + (range_size * 0.618),
        'discount_level': range_low + (range_size * 0.382)
    }

def find_liquidity_sweeps(data: pd.DataFrame, direction: str, lookback: int = 20) -> List[Dict]:
    try:
        sweeps = []
        
        if len(data) < lookback + 5:
            return sweeps
        
        recent_data = data.tail(lookback)
        swing_highs, swing_lows = find_swing_points(recent_data, order=3)
        
        if direction == 'buy':
            # Look for sweeps below swing lows followed by reversal
            for i, low_price in swing_lows.items():
                # Check if price swept below this low and then reversed
                after_low = data.loc[i:].head(5)  # Next 5 candles
                if len(after_low) >= 3:
                    swept_below = after_low['low'].min() < low_price
                    reversed_up = after_low['close'].iloc[-1] > low_price
                    if swept_below and reversed_up:
                        sweeps.append({
                            'type': 'bullish_sweep',
                            'timestamp': i,
                            'level': low_price,
                            'sweep_low': after_low['low'].min()
                        })
        
        elif direction == 'sell':
            # Look for sweeps above swing highs followed by reversal
            for i, high_price in swing_highs.items():
                # Check if price swept above this high and then reversed
                after_high = data.loc[i:].head(5)  # Next 5 candles
                if len(after_high) >= 3:
                    swept_above = after_high['high'].max() > high_price
                    reversed_down = after_high['close'].iloc[-1] < high_price
                    if swept_above and reversed_down:
                        sweeps.append({
                            'type': 'bearish_sweep',
                            'timestamp': i,
                            'level': high_price,
                            'sweep_high': after_high['high'].max()
                        })
        
        return sweeps
    
    except Exception as e:
        logging.error(f"Error finding liquidity sweeps: {e}")
        return []
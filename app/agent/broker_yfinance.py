import yfinance as yf
import pandas as pd
import logging
from typing import Optional

class YFinanceBroker:
    """A broker implementation for fetching data from Yahoo Finance."""
    def __init__(self, config: dict):
        self.config = config

    def map_timeframe(self, timeframe: str) -> Optional[str]:
        # yfinance uses a different interval format than Alpaca
        mapping = {'15m': '15m', '1h': '1h', '4h': '1h', '1d': '1d'}
        if timeframe.lower() == '4h':
            logging.warning("yfinance does not support '4h' interval directly, falling back to '1h'. The charting engine will resample this.")
        return mapping.get(timeframe.lower())

    async def get_bars(self, symbol: str, timeframe: str, start: str, end: str) -> Optional[pd.DataFrame]:
        try:
            data = yf.download(tickers=symbol, start=start.split('T')[0], end=end.split('T')[0], interval=timeframe, progress=False, auto_adjust=False)
            if data.empty: return None
            data.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
            return data
        except Exception as e:
            logging.error(f"Error fetching data for {symbol} from yfinance: {e}")
            return None

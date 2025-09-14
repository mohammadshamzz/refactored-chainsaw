import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from .alpaca_broker import AlpacaBroker
from .database import execute_db_query

class DataManager:
    """
    Manages all market data operations, including fetching from the API
    and caching to a local PostgreSQL database to minimize API calls.
    """

    def __init__(self, db_pool, broker: AlpacaBroker):
        self.db_pool = db_pool
        self.broker = broker
        self.session_cache: Dict[str, pd.DataFrame] = {}

    async def get_bars_multiple(
        self,
        symbols: List[str],
        timeframe_str: str,
        start_dt: datetime,
        end_dt: datetime
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch bars for multiple symbols efficiently, handling a dict-of-DataFrames 
        API response and fetching only missing data.
        """
        results: Dict[str, pd.DataFrame] = {}
        symbols_to_fetch_api: Dict[str, datetime] = {}

        # 1. Fetch existing data from the database
        for symbol in symbols:
            db_df = await self._fetch_bars_from_db(symbol, timeframe_str, start_dt, end_dt)
            if not db_df.empty:
                results[symbol] = db_df
                latest_ts_in_db = db_df.index.max()
                
                timeframe_obj = self.broker.map_timeframe(timeframe_str)
                timeframe_delta = self.broker.timeframe_to_timedelta(timeframe_obj)
                if (end_dt - latest_ts_in_db) > timeframe_delta:
                    symbols_to_fetch_api[symbol] = latest_ts_in_db + timedelta(seconds=1)
            else:
                symbols_to_fetch_api[symbol] = start_dt
        
        # 2. Fetch missing data from the API in a bulk call
        if symbols_to_fetch_api:
            min_fetch_start_dt = min(symbols_to_fetch_api.values())
            symbols_list = list(symbols_to_fetch_api.keys())
            
            logging.info(f"Fetching API data for {len(symbols_list)} symbols from {min_fetch_start_dt.isoformat()}")

            try:
                # This call returns a dict like {'TSLA': df, 'AAPL': df}
                api_bars_dict = await self.broker.get_bars_multiple(
                    symbols_list,
                    self.broker.map_timeframe(timeframe_str),
                    min_fetch_start_dt.isoformat(),
                    end_dt.isoformat()
                )

                # CORRECTED LOGIC: Iterate over the dictionary's items
                if isinstance(api_bars_dict, dict):
                    for symbol, new_bars_df in api_bars_dict.items():
                        if not new_bars_df.empty:
                            # Save the newly fetched bars to the database
                            await self._save_bars_to_db(symbol, timeframe_str, new_bars_df)

                            # Merge with existing data from the DB
                            existing_bars = results.get(symbol, pd.DataFrame())
                            combined_df = pd.concat([existing_bars, new_bars_df])
                            
                            # De-duplicate and sort
                            combined_df = combined_df[~combined_df.index.duplicated(keep='last')].sort_index()
                            results[symbol] = combined_df

            except Exception as e:
                # The "Failed to fetch/save..." log message originates here
                logging.error(f"Failed to fetch/save API data for symbols {symbols_list}: {e}", exc_info=True)

        # 4. Ensure every requested symbol has a key in the final dict
        for symbol in symbols:
            if symbol not in results:
                results[symbol] = pd.DataFrame()

        return results

    async def _fetch_bars_from_db(self, symbol: str, timeframe_str: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        """Helper to fetch and format data from the database for a single symbol."""
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM market_data
            WHERE symbol = %s AND timeframe = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        db_data = await execute_db_query(
            self.db_pool, query, (symbol, timeframe_str, start_dt, end_dt), fetch="all"
        )
        
        if not db_data:
            return pd.DataFrame()
            
        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(db_data, columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        return df.set_index('timestamp')

    async def get_bars(self, symbol: str, timeframe_str: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        """
        Wrapper for single symbol fetching.
        """
        result = await self.get_bars_multiple([symbol], timeframe_str, start_dt, end_dt)
        return result.get(symbol, pd.DataFrame())

    async def _save_bars_to_db(self, symbol: str, timeframe: str, df: pd.DataFrame):
        """Saves a DataFrame of market data to the database, ensuring native Python types."""
        if df.empty:
            return

        records = []
        for timestamp, row in df.iterrows():
            records.append((
                symbol,
                timeframe,
                timestamp,
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume'])
            ))

        query = """
            INSERT INTO market_data (symbol, timeframe, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timeframe, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """
        await execute_db_query(self.db_pool, query, records, many=True)
        logging.debug(f"Saved/updated {len(records)} bars to DB for {symbol} {timeframe}")

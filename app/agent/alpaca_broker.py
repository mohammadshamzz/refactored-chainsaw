import os
import logging
import pandas as pd
import asyncio
from datetime import datetime, date, timedelta, timezone
from typing import Any, List, Optional, Dict

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.common.exceptions import APIError, RetryException
from alpaca.trading.requests import (
    GetOrdersRequest, ReplaceOrderRequest, MarketOrderRequest, 
    LimitOrderRequest, StopOrderRequest, GetCalendarRequest, GetAssetsRequest
)
from alpaca.trading.enums import QueryOrderStatus, OrderSide, TimeInForce, OrderClass, AssetStatus, AssetClass
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.requests import StockBarsRequest


class AlpacaBroker:
    def __init__(self, config: dict):
        """Initialize Alpaca clients from config dictionary."""
        broker_conf = config.get("alpaca", {})
        self.api_key = os.getenv("ALPACA_API_KEY")
        self.secret_key = os.getenv("ALPACA_SECRET_KEY")
        self.paper = broker_conf.get("paper_trading", True)

        if not self.api_key or not self.secret_key:
            raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in your environment.")

        self.api = TradingClient(self.api_key, self.secret_key, paper=self.paper)
        self.data_api = StockHistoricalDataClient(self.api_key, self.secret_key)

        self.market_open: Optional[datetime] = None
        self.market_close: Optional[datetime] = None
        self.last_calendar_check: Optional[date] = None

        logging.info(f"AlpacaBroker initialized. Paper trading: {self.paper}")

    # --- Helper Methods ---
    def map_timeframe(self, timeframe_str: str) -> TimeFrame:
        """Convert string like '1h' or '15m' into Alpaca TimeFrame."""
        mapping = {"m": TimeFrameUnit.Minute, "h": TimeFrameUnit.Hour, "d": TimeFrameUnit.Day}
        try:
            amount = int("".join(filter(str.isdigit, timeframe_str)))
            unit_char = "".join(filter(str.isalpha, timeframe_str)).lower()
            unit = mapping[unit_char]
            return TimeFrame(amount, unit)
        except (ValueError, KeyError):
            logging.warning(f"Could not parse timeframe '{timeframe_str}', defaulting to 1 Day.")
            return TimeFrame.Day

    def timeframe_to_timedelta(self, timeframe: TimeFrame) -> timedelta:
        """Converts an Alpaca TimeFrame object to a datetime.timedelta."""
        unit_map = {
            TimeFrameUnit.Minute: 'minutes',
            TimeFrameUnit.Hour: 'hours',
            TimeFrameUnit.Day: 'days'
        }
        unit_str = unit_map.get(timeframe.unit)
        if not unit_str:
            logging.warning(f"Unsupported timeframe unit {timeframe.unit} for timedelta conversion. Defaulting to 1 day.")
            return timedelta(days=1)
        return timedelta(**{unit_str: timeframe.amount})
        
    async def _retry_async(self, func, *args, retries=3, delay=2, backoff=2, **kwargs):
        """Retry wrapper for Alpaca SDK calls with exponential backoff."""
        last_exception = None
        for attempt in range(retries):
            try:
                return await asyncio.to_thread(func, *args, **kwargs)
            except (APIError, RetryException) as e:
                last_exception = e
                if any(phrase in str(e).lower() for phrase in ["position does not exist", "forbidden", "unauthorized", "not found"]):
                    raise e
                if attempt < retries - 1:
                    wait = delay * (backoff ** attempt)
                    logging.warning(f"API error on attempt {attempt+1}/{retries}: {e}. Retrying in {wait}s...")
                    await asyncio.sleep(wait)
        logging.error(f"API call failed after {retries} attempts: {func.__name__}")
        raise last_exception or RuntimeError(f"API call failed after {retries} attempts")

    # --- Account & Positions ---
    async def get_account(self) -> Any:
        return await self._retry_async(self.api.get_account)

    async def get_position(self, symbol: str) -> Any:
        try:
            return await self._retry_async(self.api.get_open_position, symbol)
        except APIError as e:
            if "position does not exist" in str(e).lower():
                return None
            raise

    async def get_all_positions(self) -> List[Any]:
        return await self._retry_async(self.api.get_all_positions)

    def get_tradable_assets(self):
        search_params = GetAssetsRequest(
            status=AssetStatus.ACTIVE,
            asset_class=AssetClass.US_EQUITY,
        )
        try:
            assets = self.api.get_all_assets(search_params)
            tradable_assets = [a for a in assets if a.tradable]
            logging.info(f"Fetched {len(tradable_assets)} tradable assets from Alpaca.")
            return tradable_assets
        except Exception as e:
            logging.error(f"Error fetching assets from Alpaca: {e}", exc_info=True)
            return []

    # --- Order Management ---
    async def get_orders(self, status: str = "open", limit: int = 100) -> List[Any]:
        request_params = GetOrdersRequest(status=QueryOrderStatus(status.upper()), limit=limit)
        return await self._retry_async(self.api.get_orders, request_params)

    async def submit_bracket_order(self, **kwargs) -> Any:
        request = LimitOrderRequest(order_class=OrderClass.BRACKET, **kwargs)
        return await self._retry_async(self.api.submit_order, request)

    async def cancel_order(self, order_id: str) -> bool:
        try:
            await self._retry_async(self.api.cancel_order_by_id, order_id)
            return True
        except APIError:
            return False

    async def modify_order(self, order_id: str, **kwargs) -> Any:
        request = ReplaceOrderRequest(**kwargs)
        return await self._retry_async(self.api.replace_order_by_id, order_id, request)

    # --- Market Hours ---
    async def _update_market_calendar(self):
        today = date.today()
        if self.last_calendar_check != today:
            try:
                calendar_request = GetCalendarRequest(start=today.isoformat(), end=today.isoformat())
                calendar = await self._retry_async(self.api.get_calendar, calendar_request)
                if calendar:
                    self.market_open = calendar[0].open.replace(tzinfo=timezone.utc)
                    self.market_close = calendar[0].close.replace(tzinfo=timezone.utc)
                    self.last_calendar_check = today
            except Exception as e:
                logging.warning(f"Could not update market calendar: {e}")
                self.market_open, self.market_close = None, None

    async def is_market_open(self) -> bool:
        await self._update_market_calendar()
        if not self.market_open or not self.market_close:
            return False
        now = datetime.now(timezone.utc)
        return self.market_open <= now <= self.market_close

    # --- Historical Data ---
    async def get_bars(self, symbol: str, timeframe: TimeFrame, start: str, end: str) -> pd.DataFrame:
        try:
            start_dt = pd.to_datetime(start, utc=True)
            end_dt = pd.to_datetime(end, utc=True)
            safe_end_dt = pd.Timestamp.now(tz='UTC') - pd.Timedelta(minutes=1)
            final_end_dt = min(end_dt, safe_end_dt)
            if start_dt >= final_end_dt:
                logging.warning(f"Invalid date range for {symbol}: start ({start_dt}) >= end ({final_end_dt})")
                return pd.DataFrame()

            request_params = StockBarsRequest(
                symbol_or_symbols=[symbol],
                timeframe=timeframe,
                start=start_dt.to_pydatetime(),
                end=final_end_dt.to_pydatetime(),
                adjustment='raw',
                feed='iex' if self.paper else 'sip'
            )

            bars_data = await self._retry_async(self.data_api.get_stock_bars, request_params)
            if not bars_data or bars_data.df.empty:
                return pd.DataFrame()

            df = bars_data.df
            if symbol not in df.index.get_level_values('symbol'):
                return pd.DataFrame()

            symbol_df = df.loc[symbol].copy()
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in symbol_df.columns for col in required_cols):
                logging.error(f"DataFrame for {symbol} is missing required columns: {symbol_df.columns.tolist()}")
                return pd.DataFrame()

            return symbol_df[required_cols]

        except Exception as e:
            logging.error(f"Critical error fetching bars for {symbol}: {e}", exc_info=True)
            return pd.DataFrame()

    async def get_bars_multiple(self, symbols: List[str], timeframe: TimeFrame, start: datetime, end: datetime) -> Dict[str, pd.DataFrame]:
        """Fetch bars for multiple symbols in a single API call."""
        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=symbols,
                timeframe=timeframe,
                start=start,
                end=end,
                adjustment='raw',
                feed='iex' if self.paper else 'sip'
            )
            bars_data = await self._retry_async(self.data_api.get_stock_bars, request_params)
            if not bars_data or bars_data.df.empty:
                return {s: pd.DataFrame() for s in symbols}

            df = bars_data.df
            result = {}
            for s in symbols:
                if s in df.index.get_level_values('symbol'):
                    s_df = df.loc[s].copy()
                    result[s] = s_df[['open', 'high', 'low', 'close', 'volume']]
                else:
                    result[s] = pd.DataFrame()
            return result

        except Exception as e:
            logging.error(f"Critical error fetching bars for symbols {symbols}: {e}", exc_info=True)
            return {s: pd.DataFrame() for s in symbols}

    # --- Validation & Health Check ---
    async def validate_connection(self) -> dict:
        result = {'account_access': False, 'trading_access': False, 'data_access': False, 'market_open': False, 'errors': []}
        try:
            await self.get_account()
            result['account_access'] = True
            await self.get_all_positions()
            result['trading_access'] = True

            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=5)

            # ✅ Pass proper TimeFrame object
            timeframe = self.map_timeframe("1d")
            bars_dict = await self.get_bars_multiple(["AAPL"], timeframe, start_time, end_time)
            if not bars_dict["AAPL"].empty:
                result['data_access'] = True
            else:
                result['errors'].append("No data returned for test symbol AAPL")

            result['market_open'] = await self.is_market_open()
        except Exception as e:
            result['errors'].append(str(e))
        return result

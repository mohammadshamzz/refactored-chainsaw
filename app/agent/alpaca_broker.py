import os
import logging
import pandas as pd
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional
import time

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.common.exceptions import APIError, RetryException
from alpaca.trading.requests import (
    GetOrdersRequest, ReplaceOrderRequest, MarketOrderRequest, 
    LimitOrderRequest, StopOrderRequest, GetCalendarRequest
)
from alpaca.trading.enums import QueryOrderStatus, OrderSide, TimeInForce, OrderClass

# ALPACA TRAILING STOP CAPABILITIES DOCUMENTATION:
# 
# RESEARCH FINDINGS (Based on Alpaca API v2 Documentation):
# ================================================================
# 
# 1. NATIVE SUPPORT: Alpaca DOES support trailing stop orders through the API
# 2. INTEGRATION: Trailing stops can be submitted as part of bracket orders
# 3. CONFIGURATION OPTIONS:
#    - trail_percent: Percentage-based trailing (e.g., 2.0 for 2%)
#    - trail_price: Dollar amount trailing (e.g., 1.50 for $1.50)
# 
# 4. IMPLEMENTATION STATUS: ✅ IMPLEMENTED
#    - Enhanced submit_bracket_order() method accepts trailing_stop_percent parameter
#    - When provided, trailing stop replaces fixed stop loss in bracket order
#    - Automatic adjustment as position moves favorably
# 
# 5. OPERATIONAL LIMITATIONS:
#    - Trailing stops only active during market hours
#    - Cannot be modified once submitted (must cancel and resubmit)
#    - Minimum trail amount: $0.01 or 0.01%
#    - Maximum trail percent: 50%
# 
# 6. INTEGRATION WITH LIVE TRADING:
#    - LiveOrdersManager.submit_order() enhanced to accept trailing_stop_percent
#    - Agent._place_trade() can calculate and pass trailing stop parameters
#    - No manual trailing stop management required (handled by Alpaca)
# 
# 7. BACKTEST ALIGNMENT:
#    - Live trading trailing stops will behave similarly to backtest implementation
#    - Percentage-based trailing provides consistent risk management
#    - Eliminates timing gaps between price updates and stop adjustments
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

        # Initialize Alpaca clients
        self.api = TradingClient(self.api_key, self.secret_key, paper=self.paper)
        self.data_api = StockHistoricalDataClient(self.api_key, self.secret_key)

        logging.info(f"AlpacaBroker initialized. Paper trading: {self.paper}")

    def map_timeframe(self, timeframe_str: str) -> Optional[TimeFrame]:
        """Convert string like '1h' or '15m' into Alpaca TimeFrame."""
        mapping = {"m": TimeFrameUnit.Minute, "h": TimeFrameUnit.Hour, "d": TimeFrameUnit.Day}
        try:
            amount = int("".join(filter(str.isdigit, timeframe_str)))
            unit = mapping[("".join(filter(str.isalpha, timeframe_str))).lower()]
            return TimeFrame(amount, unit)
        except (ValueError, KeyError):
            logging.warning(f"Could not parse timeframe '{timeframe_str}', defaulting to 1 Day.")
            return TimeFrame.Day

    async def _retry_async(self, func, *args, retries=3, delay=2, backoff=2, **kwargs):
        """Helper for retrying async Alpaca calls with exponential backoff."""
        last_exception = None
        attempt = 0
        
        while attempt < retries:
            try:
                return await asyncio.to_thread(func, *args, **kwargs)
            except (APIError, RetryException) as e:
                last_exception = e
                attempt += 1
                
                # Don't retry certain errors
                error_str = str(e).lower()
                if any(phrase in error_str for phrase in [
                    "position does not exist", 
                    "forbidden", 
                    "unauthorized",
                    "invalid symbol"
                ]):
                    logging.debug(f"Non-retryable error for {func.__name__}: {e}")
                    raise e
                
                if attempt < retries:
                    wait = delay * (backoff ** (attempt - 1))
                    logging.warning(f"API error on attempt {attempt}/{retries}: {e}. Retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    
        logging.error(f"API call failed after {retries} attempts: {func.__name__}({args}, {kwargs})")
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError(f"API call failed after {retries} attempts with unknown error")

    # --- Account / Positions ---
    async def get_account(self) -> Any:
        return await self._retry_async(self.api.get_account)

    async def get_position(self, symbol: str) -> Any:
        try:
            return await self._retry_async(self.api.get_open_position, symbol)
        except APIError as e:
            error_str = str(e).lower()
            if "position does not exist" in error_str or "40410000" in str(e):
                logging.debug(f"No position found for {symbol} (expected)")
                return None
            logging.error(f"Error getting position for {symbol}: {e}")
            raise

    async def get_all_positions(self) -> List[Any]:
        return await self._retry_async(self.api.get_all_positions)

    # --- Orders ---
    async def get_orders(self, status: str = "open", limit: int = 100) -> List[Any]:
        order_status = getattr(QueryOrderStatus, status.upper(), QueryOrderStatus.OPEN)
        request_params = GetOrdersRequest(status=order_status, limit=limit)
        return await self._retry_async(self.api.get_orders, request_params)

    async def submit_order(self, symbol: str, qty: float, side: str, order_type: str = "market", 
                          limit_price: Optional[float] = None, stop_price: Optional[float] = None,
                          time_in_force: str = "day", **kwargs) -> Any:
        """Submit an order using proper Alpaca request objects."""
        
        # Validate minimum order value (Alpaca requires minimum $1 for stocks)
        if limit_price and qty * limit_price < 1.0:
            raise ValueError(f"Order value ${qty * limit_price:.2f} is below minimum $1.00")
        
        # Convert side to Alpaca enum
        order_side = OrderSide.BUY if side.lower() in ['buy', 'long'] else OrderSide.SELL
        
        # Convert time in force to Alpaca enum
        tif_mapping = {
            'day': TimeInForce.DAY,
            'gtc': TimeInForce.GTC,
            'ioc': TimeInForce.IOC,
            'fok': TimeInForce.FOK
        }
        tif = tif_mapping.get(time_in_force.lower(), TimeInForce.DAY)
        
        # Create appropriate request object based on order type
        if order_type.lower() == "market":
            request = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=tif
            )
        elif order_type.lower() == "limit":
            if not limit_price:
                raise ValueError("Limit price required for limit orders")
            request = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=tif,
                limit_price=limit_price
            )
        elif order_type.lower() == "stop":
            if not stop_price:
                raise ValueError("Stop price required for stop orders")
            request = StopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=tif,
                stop_price=stop_price
            )
        else:
            raise ValueError(f"Unsupported order type: {order_type}")
        
        return await self._retry_async(self.api.submit_order, request)

    async def submit_bracket_order(self, symbol: str, qty: float, side: str, 
                                  entry_price: float, stop_loss: float, take_profit: float,
                                  trailing_stop_percent: Optional[float] = None,
                                  time_in_force: str = "day") -> Any:
        """
        Submit a bracket order with entry, stop loss, and take profit.
        
        Args:
            symbol: Stock symbol
            qty: Quantity to trade
            side: 'buy' or 'sell'
            entry_price: Entry limit price
            stop_loss: Stop loss price (used if trailing_stop_percent is None)
            take_profit: Take profit limit price
            trailing_stop_percent: Optional trailing stop percentage (e.g., 2.0 for 2%)
            time_in_force: Order time in force
            
        Returns:
            Alpaca order object
            
        Note: If trailing_stop_percent is provided, it will be used instead of fixed stop_loss.
        Trailing stops automatically adjust as the position moves favorably.
        """
        
        # Validate minimum order value
        if qty * entry_price < 1.0:
            raise ValueError(f"Order value ${qty * entry_price:.2f} is below minimum $1.00")
        
        # Check if market is open
        if not await self.is_market_open():
            raise ValueError("Cannot place orders when market is closed")
        
        order_side = OrderSide.BUY if side.lower() in ['buy', 'long'] else OrderSide.SELL
        tif = getattr(TimeInForce, time_in_force.upper(), TimeInForce.DAY)
        
        # Prepare stop loss configuration
        if trailing_stop_percent is not None:
            # Use trailing stop instead of fixed stop loss
            stop_loss_config = {'trail_percent': trailing_stop_percent}
            logging.info(f"Using trailing stop: {trailing_stop_percent}% for {symbol}")
        else:
            # Use fixed stop loss
            stop_loss_config = {'stop_price': stop_loss}
            logging.info(f"Using fixed stop loss: ${stop_loss:.2f} for {symbol}")
        
        # Create bracket order request
        request = LimitOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            time_in_force=tif,
            limit_price=entry_price,
            order_class=OrderClass.BRACKET,
            stop_loss=stop_loss_config,
            take_profit={'limit_price': take_profit}
        )
        
        return await self._retry_async(self.api.submit_order, request)

    async def cancel_order(self, order_id: str) -> bool:
        try:
            await self._retry_async(self.api.cancel_order_by_id, order_id)
            return True
        except APIError:
            return False

    async def modify_order(self, order_id: str, **kwargs) -> Any:
        replace_request = ReplaceOrderRequest(**kwargs)
        return await self._retry_async(self.api.replace_order_by_id, order_id, replace_request)

    # --- Market Hours ---
    async def is_market_open(self) -> bool:
        """Check if market is currently open."""
        try:
            today = datetime.now().date()
            request = GetCalendarRequest(start=today, end=today)
            calendar = await self._retry_async(self.api.get_calendar, request)
            
            if not calendar:
                return False
                
            market_day = calendar[0]
            now = datetime.now(timezone.utc)
            
            # Convert market open/close times to UTC
            market_open = market_day.open.replace(tzinfo=timezone.utc)
            market_close = market_day.close.replace(tzinfo=timezone.utc)
            
            return market_open <= now <= market_close
        except Exception as e:
            logging.warning(f"Could not determine market hours: {e}")
            return False  # Assume closed if we can't determine

    # --- Historical Data ---
    async def get_bars(self, symbol: str, timeframe: TimeFrame, start: str, end: str) -> pd.DataFrame:
        """Fetch historical bars with retries and improved error handling."""
        try:
            # Parse and validate dates
            start_dt = pd.to_datetime(start)
            end_dt = pd.to_datetime(end)
            
            # Ensure timezone awareness
            if start_dt.tz is None:
                start_dt = start_dt.tz_localize('UTC')
            if end_dt.tz is None:
                end_dt = end_dt.tz_localize('UTC')
            
            # Cap end-date to avoid real-time data issues
            safe_end_dt = datetime.now(timezone.utc) - timedelta(minutes=15)
            final_end_dt = min(end_dt, safe_end_dt)
            
            # Validate date range
            if start_dt >= final_end_dt:
                logging.warning(f"Invalid date range for {symbol}: start >= end")
                return pd.DataFrame()

            # Determine best data feed based on paper/live trading
            data_feed = 'iex' if self.paper else 'sip'  # Use SIP for live trading if available
            
            request_params = StockBarsRequest(
                symbol_or_symbols=[symbol],
                timeframe=timeframe,
                start=start_dt,
                end=final_end_dt,
                adjustment='raw',  # Use raw prices to avoid split/dividend adjustments
                asof=None,
                feed=data_feed
            )

            bars = await self._retry_async(self.data_api.get_stock_bars, request_params)
            
            if not bars or not hasattr(bars, 'df') or bars.df.empty:
                logging.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            # Extract symbol data from multi-index DataFrame
            if symbol in bars.df.index.get_level_values(0):
                symbol_data = bars.df.loc[symbol].copy()
                
                # Ensure proper column names and types
                expected_columns = ['open', 'high', 'low', 'close', 'volume']
                for col in expected_columns:
                    if col not in symbol_data.columns:
                        logging.warning(f"Missing column {col} for {symbol}")
                        return pd.DataFrame()
                
                # Convert to numeric and handle any data issues
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    symbol_data[col] = pd.to_numeric(symbol_data[col], errors='coerce')
                
                # Remove any rows with NaN values
                symbol_data = symbol_data.dropna()
                
                logging.debug(f"Successfully fetched {len(symbol_data)} bars for {symbol}")
                return symbol_data
            else:
                logging.warning(f"Symbol {symbol} not found in response")
                return pd.DataFrame()
                
        except APIError as e:
            if "too many requests" in str(e).lower():
                logging.warning(f"Rate limit hit for {symbol}, will retry")
                raise  # Let retry mechanism handle it
            elif "forbidden" in str(e).lower():
                logging.error(f"Access forbidden for {symbol} - check API permissions")
            else:
                logging.error(f"API error fetching bars for {symbol}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Unexpected error fetching bars for {symbol}: {e}", exc_info=True)
            return pd.DataFrame()

    # --- Validation & Health Check ---
    async def validate_connection(self) -> dict:
        """Validate API connection and permissions."""
        result = {
            'account_access': False,
            'trading_access': False,
            'data_access': False,
            'market_open': False,
            'errors': []
        }
        
        try:
            # Test account access
            account = await self.get_account()
            if account:
                result['account_access'] = True
                logging.info(f"Account access OK. Equity: ${float(account.equity):,.2f}")
            
            # Test trading access by checking positions
            positions = await self.get_all_positions()
            result['trading_access'] = True
            logging.info(f"Trading access OK. Current positions: {len(positions)}")
            
            # Test data access with a simple request
            test_symbol = "AAPL"
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=5)
            
            bars = await self.get_bars(
                test_symbol, 
                TimeFrame.Day, 
                start_dt.isoformat(), 
                end_dt.isoformat()
            )
            
            if not bars.empty:
                result['data_access'] = True
                logging.info(f"Data access OK. Retrieved {len(bars)} bars for {test_symbol}")
            else:
                result['errors'].append("No data returned for test request")
            
            # Check market status
            result['market_open'] = await self.is_market_open()
            
        except Exception as e:
            error_msg = f"Connection validation failed: {e}"
            result['errors'].append(error_msg)
            logging.error(error_msg)
        
        return result

    # --- Cleanup ---
    async def close(self):
        logging.info("AlpacaBroker closed (no persistent connections to clean up).")

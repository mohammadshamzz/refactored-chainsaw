import logging
import threading
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import asyncio
from typing import Optional, Dict, Any

# Import logging configuration
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logging_config import setup_logging
setup_logging()

# Import your custom modules
from .database import execute_db_query
from .analysis import (
    analyze_market_structure, find_order_blocks, find_fair_value_gaps,
    find_swing_points, get_market_regime, find_mss, is_volume_significant,
    get_dynamic_target, calculate_atr
)
from .risk_manager import calculate_position_size
from .live_orders_manager import OrderType
from .trading_journal import TradingJournal
from .external_notifications import external_notifier
from .notifications import notification_manager

class TradingAgent(threading.Thread):
    def __init__(self, shared_state, lock, db_pool, broker, live_orders_manager):
        super().__init__()
        self.daemon = True
        self._stop_event = threading.Event()
        self.shared_state = shared_state
        self.lock = lock
        self.db_pool = db_pool
        self.config = shared_state['config']
        self.broker = broker
        self.live_orders_manager = live_orders_manager # The manager is passed in uninitialized
        self.trading_journal = TradingJournal(db_pool)
        self.watchlist: Dict[str, dict] = {}
        self._trade_locks_by_symbol = {}
        self.active_trades_by_symbol = {}
        self.startup_notification_sent = False
        self.daily_equity_start = 0.0  # Track starting equity for daily loss calculation
        
        # PDT Management System (CRITICAL ADDITION)
        self.pdt_config = self.config.get('pdt_management', {'enabled': True, 'daily_trade_limit': 4})
        self.pdt_enabled = self.pdt_config.get('enabled', True)
        self.pdt_trade_limit = self.pdt_config.get('daily_trade_limit', 4)
        self.daily_trades_executed = 0
        self.current_trading_day = None
        self.pending_trade_candidates = []
        
        # Concurrency control for API calls
        self._api_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent API calls
        self._symbol_batch_index = 0  # For rotating through symbols
        
        logging.info(f"PDT Management: {'Enabled' if self.pdt_enabled else 'Disabled'} "
                    f"(Limit: {self.pdt_trade_limit} trades/day)")

    def _update_status(self, message, **kwargs):
        with self.lock:
            self.shared_state["agent_status"] = message
            self.shared_state["last_update"] = datetime.now()
            for key, value in kwargs.items():
                self.shared_state[key] = value
        logging.info(message)

    async def _connect_services(self):
        """Connect and validate all services."""
        try:
            # Validate Alpaca connection
            validation_result = await self.broker.validate_connection()
            
            if validation_result['errors']:
                error_msg = f"Broker validation failed: {'; '.join(validation_result['errors'])}"
                logging.error(error_msg)
                raise Exception(error_msg)
            
            if not validation_result['account_access']:
                raise Exception("Cannot access Alpaca account")
            
            if not validation_result['data_access']:
                logging.warning("Data access issues detected - some features may not work")
            
            account_info = await self.broker.get_account()
            equity = float(getattr(account_info, 'equity', 0))
            
            status_msg = f"Services connected. Account Equity: ${equity:,.2f}"
            if validation_result['market_open']:
                status_msg += " | Market: OPEN"
            else:
                status_msg += " | Market: CLOSED"
            
            self._update_status(status_msg, account_info=vars(account_info))
            
            # Log validation results
            logging.info(f"Broker validation: Account={validation_result['account_access']}, "
                        f"Trading={validation_result['trading_access']}, "
                        f"Data={validation_result['data_access']}, "
                        f"Market Open={validation_result['market_open']}")
                        
        except Exception as e:
            logging.error(f"Service connection failed: {e}")
            raise

    def run(self):
        """Synchronous wrapper that creates a new event loop for this background thread."""
        try:
            asyncio.run(self.run_async())
        except Exception as e:
            logging.error(f"TradingAgent thread crashed: {e}", exc_info=True)
            self._update_status(f"Crashed: {e}")

    async def run_async(self):
        """The main asynchronous loop for the trading agent, running in its own thread."""
        logging.info("Trading Agent's async loop started.")

        # --- FIX: All async initialization now happens here, inside the thread's dedicated event loop ---
        try:
            await self.live_orders_manager.initialize()
            await self.live_orders_manager.load_orders_from_db()
            self.live_orders_manager.start_monitoring() # This starts the order monitoring task
            
            await self._connect_services()
            await self.trading_journal.initialize()
            
            # Load persisted state
            await self._load_agent_state()
            
            if not self.startup_notification_sent:
                external_notifier.send_email_notification("System Startup", "The ICT Trading Agent has started successfully.")
                self.startup_notification_sent = True
        except Exception as e:
            logging.critical(f"FATAL: Agent failed during initial setup: {e}", exc_info=True)
            external_notifier.notify_system_error("Agent Startup Failure", str(e))
            self._update_status(f"Error on Startup: {e}")
            return # Stop the agent if initialization fails

        while not self._stop_event.is_set():
            try:
                await self._run_analysis_cycle()
                sleep_seconds = self.config['agent']['sleep_interval_seconds']
                await asyncio.sleep(sleep_seconds)
            except Exception as e:
                error_msg = f"CRITICAL ERROR in agent's main loop: {e}"
                logging.error(error_msg, exc_info=True)
                external_notifier.notify_system_error("Agent Main Loop Crash", str(e))
                await asyncio.sleep(30) # Wait before retrying
        
        # --- Graceful Shutdown ---
        await self.live_orders_manager.stop_monitoring()
        logging.info("Agent's async loop has finished.")

    def stop(self):
        """Signals the agent to stop its loops and shut down."""
        self._update_status("Stop signal received. Agent shutting down.")
        self._stop_event.set()

    def _update_htf_bias(self, symbol: str, bias: str):
        with self.lock:
            self.shared_state.setdefault('htf_bias', {})[symbol] = bias
    
    async def _get_daily_pnl(self) -> float:
        """Get today's P&L (both realized and unrealized)."""
        try:
            # Update daily equity tracking first
            await self._update_daily_equity_tracking()
            
            # Get current equity
            account = await self.broker.get_account()
            current_equity = float(getattr(account, 'equity', 0))
            
            # Calculate total daily P&L (realized + unrealized)
            daily_pnl = current_equity - self.daily_equity_start
            
            return daily_pnl
        except Exception as e:
            logging.exception(f"Error getting daily P&L - start equity: ${self.daily_equity_start:.2f}: {e}")
            return 0.0
    
    def _df_has_rows(self, df: pd.DataFrame, min_rows: int = 1) -> bool:
        """Helper to safely check if DataFrame has sufficient rows."""
        return df is not None and not df.empty and len(df) >= min_rows
    
    async def _reset_daily_pdt_counter(self):
        """Reset PDT counter for new trading day."""
        from datetime import date
        today = date.today()
        
        if self.current_trading_day != today:
            self.current_trading_day = today
            self.daily_trades_executed = 0
            self.pending_trade_candidates = []
            logging.info(f"New trading day: {today}. PDT counter reset to 0/{self.pdt_trade_limit}")
    
    def _calculate_confluence_score(self, trade_signal: dict, htf_data: pd.DataFrame) -> int:
        """
        Calculate confluence score for trade ranking (matches backtest logic).
        Score scale: 0-100
        """
        score = 0
        
        # Base score from setup type
        setup_type = trade_signal.get('setup_type', '')
        if 'MSS' in setup_type and 'FVG' in setup_type:
            score += 30  # Full ICT setup
        elif 'MSS' in setup_type:
            score += 20  # MSS confirmation
        elif 'FVG' in setup_type:
            score += 15  # FVG entry
        
        # Risk/Reward ratio bonus
        rr_ratio = trade_signal.get('risk_reward_ratio', 1.0)
        if rr_ratio >= 3.0:
            score += 20
        elif rr_ratio >= 2.0:
            score += 15
        elif rr_ratio >= 1.5:
            score += 10
        
        # ATR-based proximity scoring
        if not htf_data.empty:
            try:
                current_atr = calculate_atr(htf_data).iloc[-1]
                entry_price = trade_signal.get('entry_price', 0)
                current_price = htf_data['close'].iloc[-1]
                distance = abs(entry_price - current_price)
                
                if distance < current_atr * 0.5:
                    score += 15  # Very close to current price
                elif distance < current_atr * 1.0:
                    score += 10  # Close to current price
                elif distance < current_atr * 2.0:
                    score += 5   # Reasonable distance
            except Exception as e:
                logging.debug(f"Error calculating ATR-based score: {e}")
        
        # Volume confirmation bonus (if available)
        if trade_signal.get('volume_confirmed', False):
            score += 10
        
        return min(100, max(0, score))  # Clamp to 0-100 range
    
    async def _manage_trailing_stops(self):
        """
        Monitor open positions and update trailing stops.
        
        Note: This method provides manual trailing stop management for positions
        that were created without Alpaca's native trailing stops. New trades
        submitted with trailing_stop_percent use Alpaca's automatic trailing
        stop functionality and don't require manual management.
        """
        try:
            positions = await self.broker.get_all_positions()
            if not positions:
                return
            
            trailing_atr_mult = self.config['trading'].get('trailing_stop_atr_multiplier', 2.5)
            
            for position in positions:
                symbol = position.symbol
                side = 'buy' if float(position.qty) > 0 else 'sell'
                current_price = float(position.market_value) / float(position.qty)
                
                # Get recent data for ATR calculation
                market_data = await self._fetch_all_timeframes(symbol)
                ltf_data = market_data.get('ltf')
                
                if not self._df_has_rows(ltf_data, 20):
                    continue
                
                atr_series = calculate_atr(ltf_data)
                if not self._df_has_rows(atr_series, 1):
                    continue
                
                current_atr = atr_series.iloc[-1]
                
                # Calculate new trailing stop
                if side == 'buy':
                    new_stop = current_price - (current_atr * trailing_atr_mult)
                else:
                    new_stop = current_price + (current_atr * trailing_atr_mult)
                
                # Update stop loss if it's more favorable
                await self._update_trailing_stop(symbol, side, new_stop)
                
        except Exception as e:
            logging.exception(f"Error managing trailing stops for {len(positions) if 'positions' in locals() else 'unknown'} positions: {e}")
    
    async def _update_trailing_stop(self, symbol: str, side: str, new_stop: float):
        """
        Update trailing stop for a position if more favorable.
        
        Note: This method is used for manual trailing stop management of legacy
        positions. New positions use Alpaca's native trailing stops which are
        managed automatically by the broker.
        """
        try:
            # Get all open orders and filter by symbol
            orders = await self.broker.get_orders(status='open')
            stop_orders = [o for o in orders if getattr(o, 'symbol', None) == symbol and getattr(o, 'order_type', '') == 'stop']
            
            for order in stop_orders:
                current_stop = float(getattr(order, 'stop_price', 0))
                
                # Update if new stop is more favorable
                should_update = False
                if side == 'buy' and new_stop > current_stop:  # Raise stop for long position
                    should_update = True
                elif side == 'sell' and new_stop < current_stop:  # Lower stop for short position
                    should_update = True
                
                if should_update:
                    await self.broker.modify_order(order.id, stop_price=new_stop)
                    logging.info(f"Updated trailing stop for {symbol}: ${current_stop:.2f} -> ${new_stop:.2f}")
                    
        except Exception as e:
            logging.exception(f"Error updating trailing stop for {symbol} - side: {side}, new_stop: ${new_stop:.2f}: {e}")
    
    async def _save_agent_state(self):
        """Persist agent state to database."""
        try:
            state_data = {
                'watchlist': self.watchlist,
                'daily_equity_start': self.daily_equity_start,
                'daily_trades_executed': self.daily_trades_executed,
                'current_trading_day': self.current_trading_day.isoformat() if self.current_trading_day else None,
                'last_saved': datetime.now(timezone.utc).isoformat()
            }

            logging.info(f"Saving agent state - {state_data}"); 
            
            query = """
                INSERT INTO agent_state (id, state_data, updated_at)
                VALUES ('main_agent', %s, %s)
                ON CONFLICT (id) DO UPDATE SET 
                    state_data = EXCLUDED.state_data,
                    updated_at = EXCLUDED.updated_at
            """
            await execute_db_query(self.db_pool, query, (json.dumps(state_data, default=str), datetime.now()))
            
        except Exception as e:
            logging.exception(f"Error saving agent state - watchlist items: {len(self.watchlist)}: {e}")
    
    async def _load_agent_state(self):
        """Load persisted agent state from database."""
        try:
            # Create agent_state table if it doesn't exist
            create_table_query = """
                CREATE TABLE IF NOT EXISTS agent_state (
                    id VARCHAR(50) PRIMARY KEY,
                    state_data JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            await execute_db_query(self.db_pool, create_table_query)
            
            # Create PDT tracking table
            pdt_table_query = """
                CREATE TABLE IF NOT EXISTS pdt_tracking (
                    date DATE PRIMARY KEY,
                    trades_executed INTEGER DEFAULT 0,
                    trades_available INTEGER DEFAULT 4,
                    confluence_scores JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            await execute_db_query(self.db_pool, pdt_table_query)
            
            # Load state
            query = "SELECT state_data FROM agent_state WHERE id = %s"
            result = await execute_db_query(self.db_pool, query, ('main_agent',), fetch='one')
            
            if result:
                state_data = result['state_data'] if isinstance(result['state_data'], dict) else json.loads(result['state_data'])
                self.watchlist = state_data.get('watchlist', {})
                self.daily_equity_start = state_data.get('daily_equity_start', 0.0)
                self.daily_trades_executed = state_data.get('daily_trades_executed', 0)
                
                # Restore current trading day
                trading_day_str = state_data.get('current_trading_day')
                if trading_day_str:
                    from datetime import date
                    self.current_trading_day = date.fromisoformat(trading_day_str)
                
                # Update shared state for dashboard
                with self.lock:
                    self.shared_state['watchlist'] = dict(self.watchlist)
                
                logging.info(f"Loaded agent state: {len(self.watchlist)} watchlist items, "
                           f"PDT: {self.daily_trades_executed}/{self.pdt_trade_limit}")
            else:
                # Initialize daily equity start
                account = await self.broker.get_account()
                self.daily_equity_start = float(getattr(account, 'equity', 0))
                await self._save_agent_state()
                
        except Exception as e:
            logging.exception(f"Error loading agent state from database: {e}")
            # Initialize with defaults on load failure
            self.watchlist = {}
            self.daily_equity_start = 0.0
    
    async def _update_daily_equity_tracking(self):
        """Update daily equity tracking for loss calculation."""
        try:
            from datetime import date
            today = date.today()
            
            # Check if we need to reset daily tracking (new day)
            query = """
                SELECT equity_start, date_tracked FROM daily_equity_tracking 
                WHERE date_tracked = %s ORDER BY created_at DESC LIMIT 1
            """
            result = await execute_db_query(self.db_pool, query, (today,), fetch='one')
            
            if not result:
                # New day - record starting equity
                account = await self.broker.get_account()
                current_equity = float(getattr(account, 'equity', 0))
                
                insert_query = """
                    INSERT INTO daily_equity_tracking (date_tracked, equity_start, created_at)
                    VALUES (%s, %s, %s)
                """
                await execute_db_query(self.db_pool, insert_query, (today, current_equity, datetime.now()))
                self.daily_equity_start = current_equity
                
                logging.info(f"New trading day started. Starting equity: ${current_equity:.2f}")
            else:
                self.daily_equity_start = float(result['equity_start'])
                
        except Exception as e:
            logging.exception(f"Error updating daily equity tracking - start equity: ${self.daily_equity_start:.2f}: {e}")
    
    async def _refresh_shared_state(self):
        """Refresh shared state with current account and position info."""
        try:
            # Get current account info
            account = await self.broker.get_account()
            account_info = {
                'equity': float(getattr(account, 'equity', 0)),
                'buying_power': float(getattr(account, 'buying_power', 0)),
                'cash': float(getattr(account, 'cash', 0)),
                'portfolio_value': float(getattr(account, 'portfolio_value', 0)),
                'last_updated': datetime.now().isoformat()
            }
            
            # Get current positions
            positions = await self.broker.get_all_positions()
            open_positions = []
            for pos in positions:
                open_positions.append({
                    'symbol': pos.symbol,
                    'qty': float(pos.qty),
                    'side': 'long' if float(pos.qty) > 0 else 'short',
                    'market_value': float(pos.market_value),
                    'unrealized_pnl': float(getattr(pos, 'unrealized_pnl', 0)),
                    'avg_entry_price': float(getattr(pos, 'avg_entry_price', 0))
                })
            
            # Update shared state
            with self.lock:
                self.shared_state['account_info'] = account_info
                self.shared_state['open_positions'] = open_positions
                self.shared_state['active_trades_count'] = len(self.active_trades_by_symbol)
                # Add PDT information for dashboard
                self.shared_state['pdt_info'] = {
                    'enabled': self.pdt_enabled,
                    'daily_trades_executed': self.daily_trades_executed,
                    'daily_trade_limit': self.pdt_trade_limit,
                    'trades_remaining': max(0, self.pdt_trade_limit - self.daily_trades_executed),
                    'utilization_percent': (self.daily_trades_executed / self.pdt_trade_limit * 100) if self.pdt_trade_limit > 0 else 0
                }
                
        except Exception as e:
            logging.exception(f"Error refreshing shared state: {e}")
    
    async def _cleanup_closed_positions(self):
        """Remove closed positions from active trades tracking."""
        try:
            current_positions = await self.broker.get_all_positions()
            current_symbols = {pos.symbol for pos in current_positions}
            
            # Remove symbols that no longer have positions
            closed_symbols = []
            for symbol in list(self.active_trades_by_symbol.keys()):
                if symbol not in current_symbols:
                    closed_symbols.append(symbol)
                    del self.active_trades_by_symbol[symbol]
            
            if closed_symbols:
                logging.info(f"Cleaned up closed positions: {closed_symbols}")
                
        except Exception as e:
            logging.exception(f"Error cleaning up closed positions: {e}")
    
    async def _collect_and_rank_trade_candidates(self, symbols_batch: list) -> list:
        """
        Collect all trade candidates from symbol analysis and rank by confluence score.
        This matches the backtest's PDT management approach.
        """
        trade_candidates = []
        
        for symbol in symbols_batch:
            try:
                # Get market data for confluence scoring
                market_data = await self._fetch_all_timeframes(symbol)
                htf_data = market_data.get('htf', pd.DataFrame())
                
                # Check for trade signal
                trade_signal = await self._analyze_symbol_for_signal(symbol, market_data)
                
                if trade_signal:
                    # Use POI score from trade signal if available, otherwise calculate
                    if 'confluence_score' not in trade_signal:
                        confluence_score = self._calculate_confluence_score(trade_signal, htf_data)
                        trade_signal['confluence_score'] = confluence_score
                    
                    trade_candidates.append(trade_signal)
                    logging.info(f"Trade candidate: {symbol} (Score: {trade_signal.get('confluence_score', 0)})")
                    
            except Exception as e:
                logging.exception(f"Error collecting trade candidate for {symbol}: {e}")
        
        # Sort by confluence score (highest first)
        trade_candidates.sort(key=lambda x: x.get('confluence_score', 0), reverse=True)
        
        return trade_candidates
    
    async def _execute_best_trades_pdt_aware(self, trade_candidates: list):
        """
        Execute only the best trade candidates within PDT limits.
        This is the core PDT management logic from the backtest.
        """
        if not trade_candidates:
            return
        
        # Check PDT limits
        if self.pdt_enabled and self.daily_trades_executed >= self.pdt_trade_limit:
            logging.info(f"PDT limit reached ({self.pdt_trade_limit}). "
                        f"Skipping {len(trade_candidates)} trade candidate(s).")
            return
        
        # Check position limits
        current_positions = await self.broker.get_all_positions()
        max_positions = self.config['trading'].get('max_positions', 5)
        
        if len(current_positions) >= max_positions:
            logging.warning(f"Max positions ({max_positions}) reached. "
                           f"Cannot execute new trades.")
            return
        
        # Calculate available slots
        position_slots = max_positions - len(current_positions)
        pdt_slots = (self.pdt_trade_limit - self.daily_trades_executed 
                    if self.pdt_enabled else len(trade_candidates))
        available_slots = min(position_slots, pdt_slots)
        
        # Execute best trades within limits
        executed_count = 0
        for candidate in trade_candidates[:available_slots]:
            try:
                success = await self._place_trade(candidate)
                if success:
                    executed_count += 1
                    if self.pdt_enabled:
                        self.daily_trades_executed += 1
                    
                    logging.info(f"Executed trade {executed_count}/{available_slots}: "
                                  f"{candidate['symbol']} (Score: {candidate.get('confluence_score', 0)})")
                
            except Exception as e:
                logging.exception(f"Error executing trade for {candidate['symbol']}: {e}")
        
        if executed_count > 0:
            logging.info(f"PDT Status: {self.daily_trades_executed}/{self.pdt_trade_limit} trades used today")

    async def _run_analysis_cycle(self):
        try:
            # Reset PDT counter for new day
            await self._reset_daily_pdt_counter()
            
            # Check daily loss limit before analyzing symbols
            account = await self.broker.get_account()
            equity = float(getattr(account, 'equity', 0))
            
            # Get today's P&L to check daily loss limit
            daily_pnl = await self._get_daily_pnl()
            max_daily_loss_percent = self.config['trading'].get('max_daily_loss_percent', 3.0)
            daily_loss_percent = abs(daily_pnl / equity * 100) if equity > 0 else 0
            
            if daily_pnl < 0 and daily_loss_percent >= max_daily_loss_percent:
                self._update_status(f"Daily loss limit reached: -{daily_loss_percent:.1f}% (max: {max_daily_loss_percent}%). Trading halted.")
                logging.warning(f"Daily loss limit exceeded: -{daily_loss_percent:.1f}%. Skipping analysis cycle.")
                return
            
            symbols = self.config['trading']['instruments']
            
            # Rotate through symbols to avoid analyzing the same ones every cycle
            symbols_per_cycle = min(10, len(symbols))  # Analyze max 10 symbols per cycle
            total_symbols = len(symbols)
            
            # Calculate which symbols to analyze this cycle
            start_idx = self._symbol_batch_index % total_symbols
            if start_idx + symbols_per_cycle <= total_symbols:
                current_batch = symbols[start_idx:start_idx + symbols_per_cycle]
            else:
                # Wrap around to beginning
                current_batch = symbols[start_idx:] + symbols[:symbols_per_cycle - (total_symbols - start_idx)]
            
            self._symbol_batch_index = (self._symbol_batch_index + symbols_per_cycle) % total_symbols
            
            # Update status with PDT info
            pdt_status = f"PDT: {self.daily_trades_executed}/{self.pdt_trade_limit}" if self.pdt_enabled else "PDT: Disabled"
            self._update_status(f"Analyzing {len(current_batch)}/{total_symbols} symbols... "
                              f"Daily P&L: ${daily_pnl:.2f} | {pdt_status}")
            
            # Monitor existing positions for trailing stops
            # Note: Manual trailing stop management is only needed for positions
            # that were created without native Alpaca trailing stops
            if self.config['trading'].get('use_trailing_stops', False):
                await self._manage_trailing_stops()
            
            # Collect and rank trade candidates using PDT management
            trade_candidates = await self._collect_and_rank_trade_candidates(current_batch)
            
            # Execute best trades within PDT limits
            if trade_candidates:
                await self._execute_best_trades_pdt_aware(trade_candidates)
            
            # Clean up closed positions and update shared state
            await self._cleanup_closed_positions()
            await self._refresh_shared_state()
        except Exception as e:
            logging.exception(f"Critical error in PDT-aware analysis cycle: {e}")
            # Don't return - continue running

    async def _fetch_all_timeframes(self, symbol: str) -> dict:
        """
        Fetches bars for all configured timeframes for a given symbol, with throttling and retry on 429.
        Returns a dict: {timeframe_name: pd.DataFrame}.
        """
        

        timeframes_cfg = self.config.get('timeframes', {})
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=500)
        market_data = {}

        async def fetch_bars(tf_name: str, tf_str: str) -> pd.DataFrame:
            """
            Fetch one timeframe - broker handles retries internally.
            """
            try:
                bars = await self.broker.get_bars(
                    symbol,
                    self.broker.map_timeframe(tf_str),
                    start_dt.isoformat(),
                    end_dt.isoformat()
                )
                
                if bars.empty:
                    logging.warning(f"No data returned for {symbol} {tf_name}")
                else:
                    logging.debug(f"Fetched {len(bars)} bars for {symbol} {tf_name}")
                
                return bars
            except Exception as e:
                logging.exception(f"Error fetching {tf_name} for {symbol}: {e}")
                return pd.DataFrame()

        # Use semaphore to control concurrent API calls
        async def fetch_with_semaphore(tf_name, tf_str):
            async with self._api_semaphore:
                return tf_name, await fetch_bars(tf_name, tf_str)
        
        # Fetch all timeframes concurrently but with rate limiting
        tasks = [fetch_with_semaphore(tf_name, tf_str) for tf_name, tf_str in timeframes_cfg.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                logging.exception(f"Error fetching timeframe for {symbol}: {result}")
                continue
            tf_name, df = result
            market_data[tf_name] = df

        return market_data

    async def _analyze_symbol_for_signal(self, symbol: str, market_data: dict) -> Optional[dict]:
        """
        Enhanced symbol analysis that returns trade signals with confluence scoring.
        This replaces the existing _analyze_symbol method's trade generation logic.
        """
        htf_data = market_data.get('htf')
        itf_data = market_data.get('itf') 
        ltf_data = market_data.get('ltf')
        
        if not self._df_has_rows(htf_data, 50) or not self._df_has_rows(ltf_data, 50):
            return None
        
        # Check for existing position (existing logic)
        if symbol in self.active_trades_by_symbol:
            return None
            
        position = await self.broker.get_position(symbol)
        if position:
            # Update in-memory tracking
            self.active_trades_by_symbol[symbol] = {
                'qty': float(position.qty),
                'side': 'buy' if float(position.qty) > 0 else 'sell',
                'entry_price': float(position.avg_entry_price) if hasattr(position, 'avg_entry_price') else None
            }
            return None
        
        # Market structure analysis (existing logic)
        try:
            market_trend, _, _ = analyze_market_structure(htf_data)
            self._update_htf_bias(symbol, market_trend.title())
        except Exception as e:
            logging.error(f"[{symbol}] Analysis failed: {e}")
            return None
        
        # Regime filter (Fix #6: Use backtest default True)
        if self.config['trading'].get('regime_filter_enabled', True):
            regime = get_market_regime(htf_data)
            if regime in ['VOLATILE', 'RANGING']:  # Fix #6: Match backtest order
                if symbol in self.watchlist:
                    del self.watchlist[symbol]
                    # Update shared state for dashboard
                    with self.lock:
                        self.shared_state['watchlist'] = dict(self.watchlist)
                return None
        
        # Check watchlist for LTF confirmation
        if symbol in self.watchlist:
            return self._check_ltf_confirmation_enhanced(symbol, htf_data, ltf_data, self.watchlist[symbol])
        else:
            # Look for new HTF setup
            await self._find_htf_setup_async(symbol, htf_data, itf_data)
            return None





    def _calculate_poi_score(self, poi: Dict[str, Any], htf_data: pd.DataFrame, current_atr: float) -> int:
        """
        Assigns a confluence score to POIs based on size, proximity, and volume context.
        Score scale: 0–100 (matches backtest exactly)
        """
        score = 0
        width = poi['top'] - poi['bottom']
        last_close = htf_data['close'].iloc[-1]

        # Size vs ATR (smaller is stronger)
        if width < current_atr:
            score += 10
        elif width < 2 * current_atr:
            score += 5

        # Proximity to current price
        dist = abs(last_close - poi['bottom'])
        if dist < 1.5 * current_atr:
            score += 10
        elif dist < 3 * current_atr:
            score += 5

        # Volume confirmation (if present on POI)
        if 'volume' in poi:
            avg_vol = htf_data['volume'].tail(20).mean()
            if poi['volume'] > avg_vol * 1.5:
                score += 10

        # Slight preference for OBs over FVGs
        if poi.get('source') == 'OB':
            score += 5
        elif poi.get('source') == 'FVG':
            score += 3

        return score

    async def _find_htf_setup_async(self, symbol: str, htf_data: pd.DataFrame, itf_data: pd.DataFrame):
        """Async version of _find_htf_setup with exact backtest POI logic."""
        market_trend, _, _ = analyze_market_structure(htf_data)
        
        # Enhanced MTF confirmation with all modes (Fix #2)
        if self.config['trading'].get("use_mtf_confirmation", True):
            itf_trend, _, _ = analyze_market_structure(itf_data)
            mode = self.config['trading'].get("mtf_confirmation_mode", "strict")
            
            aligned = False
            if mode == "strict":
                aligned = (market_trend == itf_trend)
            elif mode == "neutral_ok":
                aligned = (market_trend == itf_trend) or (itf_trend == "Neutral")
            elif mode == "htf_priority":
                aligned = (market_trend != "Neutral")
            
            if not aligned:
                logging.debug(f"[{symbol}] Skipping: MTF misalignment. HTF={market_trend}, ITF={itf_trend}")
                return
        
        if not self._df_has_rows(htf_data, 200):
            logging.warning(f"Insufficient HTF data for {symbol} setup analysis")
            return
        
        # Use exact backtest POI identification logic
        order_blocks = find_order_blocks(htf_data.tail(200))
        fvg_blocks = find_fair_value_gaps(htf_data.tail(200))
        current_price = htf_data['close'].iloc[-1]
        atr_series = calculate_atr(htf_data)
        if not self._df_has_rows(atr_series, 1):
            logging.warning(f"Could not calculate ATR for {symbol}")
            return
        current_atr = atr_series.iloc[-1]
        
        # Score all POIs like the backtest
        for ob in order_blocks:
            ob['type'] = ob.get('type', '').title()
            ob['source'] = "OB"
            ob['score'] = self._calculate_poi_score(ob, htf_data, current_atr)
        for fvg in fvg_blocks:
            fvg['type'] = fvg.get('type', '').title()
            fvg['source'] = "FVG"
            fvg['score'] = self._calculate_poi_score(fvg, htf_data, current_atr)
        
        valid_pois = []
        logging.debug(f"[{symbol}] Checking POIs | HTF Trend={market_trend} | Price={current_price:.2f} | ATR={current_atr:.2f}")

        def poi_passes_filters(poi, poi_type, trend):
            score = poi.get('score', 0)
            min_score = self.config['trading'].get('min_confluence_score_fvg', 10) if poi_type == "FVG" else self.config['trading'].get('min_confluence_score', 30)
            if score < min_score:
                logging.debug(f"[{symbol}] ❌ {poi_type} rej (Score {score} < {min_score})")
                return False
            
            anchor = poi['bottom'] if trend == "Bullish" else poi['top']
            distance = abs(current_price - anchor)
            max_distance = current_atr * self.config['trading'].get('max_atr_distance_for_poi', 6.0)  # Fix #6: Use backtest default
            if distance > max_distance:
                logging.debug(f"[{symbol}] ❌ {poi_type} rej (Dist {distance:.2f} > {max_distance:.2f})")
                return False
            
            if trend == "Bullish":
                if not (poi['type'] == "Bullish" and poi['top'] < current_price):
                    logging.debug(f"[{symbol}] ❌ {poi_type} rej (Invalid Bullish setup)")
                    return False
            else:
                if not (poi['type'] == "Bearish" and poi['bottom'] > current_price):
                    logging.debug(f"[{symbol}] ❌ {poi_type} rej (Invalid Bearish setup)")
                    return False
            
            logging.debug(f"[{symbol}] ✅ {poi_type} OK (Score {score}, Dist {distance:.2f})")
            return True

        # Apply backtest filtering logic
        for ob in order_blocks:
            if poi_passes_filters(ob, "OB", market_trend):
                valid_pois.append(ob)
        for fvg in fvg_blocks:
            if poi_passes_filters(fvg, "FVG", market_trend):
                valid_pois.append(fvg)

        if valid_pois:
            # Use backtest's selection criteria
            best_poi = max(valid_pois, key=lambda p: (p.get('score', 0), -abs(current_price - (p['bottom'] if market_trend == "Bullish" else p['top']))))
            sl_mult = self.config['trading'].get('atr_multiplier_for_sl', 1.5)  # Fix #6: Use backtest default
            stop_loss = best_poi['bottom'] - (current_atr * sl_mult) if market_trend == "Bullish" else best_poi['top'] + (current_atr * sl_mult)
            max_duration = (self.config['trading'].get('watchlist_ttl_bars', 20) * pd.to_timedelta(self.config['timeframes']['htf']).total_seconds())  # Fix #6: Use backtest default
            
            logging.info(f"[{symbol}] NEW POI | Type={best_poi.get('type')} | Score={best_poi.get('score', 0):.0f} | Range=({best_poi['bottom']:.2f}, {best_poi['top']:.2f}) | SL={stop_loss:.2f}")
            
            self.watchlist[symbol] = {
                'direction': 'buy' if market_trend == 'Bullish' else 'sell',
                'poi_range': (best_poi['bottom'], best_poi['top']),
                'htf_stop_loss': stop_loss,
                'timestamp': datetime.now(timezone.utc),
                'max_duration_seconds': max_duration,
                'state': 'awaiting_pullback',
                'score': best_poi.get('score', 0),  # Store POI score for later use
                'type': best_poi.get('type', "OB")
            }
            
            # Update shared state for dashboard
            with self.lock:
                self.shared_state['watchlist'] = dict(self.watchlist)
            
            # Save state after watchlist update
            await self._save_agent_state()
            
            logging.info(f"[{symbol}] New HTF setup added to watchlist. Direction: {self.watchlist[symbol]['direction']}.")
            notification_manager.notify_setup_found(symbol, self.watchlist[symbol]['direction'])
        else:
            logging.debug(f"[{symbol}] ⚠️ No valid POIs found after filtering")

    def _check_ltf_confirmation_enhanced(self, symbol: str, htf_data: pd.DataFrame, 
                                       ltf_data: pd.DataFrame, setup_info: dict) -> Optional[dict]:
        """
        Enhanced LTF confirmation that returns trade signals with confluence data.
        This matches the backtest's trade signal generation.
        """
        # Fix #3: Add stop loss invalidation check (matches backtest exactly)
        try:
            direction = setup_info['direction']
            htf_stop_loss = setup_info['htf_stop_loss']
            current_high = ltf_data['high'].iloc[-1]
            current_low = ltf_data['low'].iloc[-1]
        except (KeyError, IndexError) as e:
            logging.warning(f"[{symbol}] Missing required fields in setup_info: {e}")
            return None
        
        # Check for stop loss invalidation
        if (direction == 'buy' and current_low < htf_stop_loss) or \
           (direction == 'sell' and current_high > htf_stop_loss):
            logging.info(f"[{symbol}] Watchlist entry invalidated by SL breach")
            if symbol in self.watchlist:
                del self.watchlist[symbol]
                with self.lock:
                    self.shared_state['watchlist'] = dict(self.watchlist)
            return None
        
        # Existing timeout logic
        timestamp = setup_info['timestamp']
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except Exception:
                from dateutil.parser import isoparse
                timestamp = isoparse(timestamp)
        if (datetime.now(timezone.utc) - timestamp).total_seconds() > setup_info['max_duration_seconds']:
            if symbol in self.watchlist:
                del self.watchlist[symbol]
                # Update shared state for dashboard
                with self.lock:
                    self.shared_state['watchlist'] = dict(self.watchlist)
            return None
        
        direction = setup_info['direction']
        poi_bottom, poi_top = setup_info['poi_range']
        state = setup_info.get('state', 'awaiting_pullback')
        
        # State machine logic (matches backtest)
        if state == 'awaiting_pullback':
            current_high = ltf_data['high'].iloc[-1]
            current_low = ltf_data['low'].iloc[-1]
            
            pullback_complete = False
            if direction == 'buy' and current_low <= poi_top:
                pullback_complete = True
            elif direction == 'sell' and current_high >= poi_bottom:
                pullback_complete = True
            
            if pullback_complete:
                setup_info['state'] = 'awaiting_mss'
                logging.info(f"[{symbol}] Pullback complete, awaiting MSS")
                return None
        
        elif state == 'awaiting_mss':
            # Check for MSS with volume confirmation
            swing_orders_cfg = self.config['trading'].get('swing_orders', {})
            mss_result = find_mss(ltf_data, bias=direction, swing_orders=swing_orders_cfg)
            
            if mss_result and is_volume_significant(ltf_data, mss_result['break_candle_ts'], 
                                                  self.config['trading'].get('volume_factor', 1.5)):
                setup_info['state'] = 'awaiting_fvg_entry'
                setup_info['mss_timestamp'] = mss_result['break_candle_ts']
                logging.info(f"[{symbol}] MSS confirmed with volume")
                return None
        
        elif state == 'awaiting_fvg_entry':
            # Look for FVG entry opportunity
            mss_timestamp = setup_info.get('mss_timestamp')
            if mss_timestamp:
                ltf_since_mss = ltf_data[ltf_data.index >= mss_timestamp]
                fvg_gaps = find_fair_value_gaps(ltf_since_mss.head(20))
                
                best_fvg = None
                current_price = ltf_data['close'].iloc[-1]
                
                if direction == 'buy':
                    bullish_fvgs = [g for g in fvg_gaps if g['type'] == 'bullish' and g['top'] < current_price]
                    if bullish_fvgs:
                        best_fvg = max(bullish_fvgs, key=lambda f: f['bottom'])
                else:
                    bearish_fvgs = [g for g in fvg_gaps if g['type'] == 'bearish' and g['bottom'] > current_price]
                    if bearish_fvgs:
                        best_fvg = min(bearish_fvgs, key=lambda f: f['top'])
                
                if best_fvg:
                    # Generate trade signal
                    entry_price = best_fvg['top'] if direction == 'buy' else best_fvg['bottom']
                    
                    # Dynamic stop loss calculation
                    ltf_swing_highs, ltf_swing_lows = find_swing_points(ltf_since_mss, order=3)
                    dynamic_stop_loss = None
                    
                    if direction == 'buy' and not ltf_swing_lows.empty:
                        dynamic_stop_loss = ltf_swing_lows.iloc[-1]
                    elif direction == 'sell' and not ltf_swing_highs.empty:
                        dynamic_stop_loss = ltf_swing_highs.iloc[-1]
                    
                    stop_loss = dynamic_stop_loss if dynamic_stop_loss is not None else setup_info['htf_stop_loss']
                    
                    # Dynamic take profit calculation (matches backtest)
                    risk_dist = abs(entry_price - stop_loss)
                    min_dynamic_rr = self.config['trading'].get('min_dynamic_rr_ratio', 1.5)
                    
                    swing_orders_cfg = self.config['trading'].get('swing_orders', {})
                    dynamic_tp_target = get_dynamic_target(df=ltf_since_mss, current_price=entry_price, 
                                                         side=direction, swing_orders=swing_orders_cfg)
                    
                    take_profit = None
                    if dynamic_tp_target is not None and risk_dist > 1e-8:
                        reward_dist = abs(dynamic_tp_target - entry_price)
                        dynamic_rr = reward_dist / risk_dist
                        if dynamic_rr >= min_dynamic_rr:
                            take_profit = dynamic_tp_target
                    
                    # Fallback to fixed R:R
                    if take_profit is None:
                        min_fixed_rr = self.config['trading'].get('min_risk_to_reward_ratio', 1.5)
                        if direction == 'buy':
                            take_profit = entry_price + (risk_dist * min_fixed_rr)
                        else:
                            take_profit = entry_price - (risk_dist * min_fixed_rr)
                    
                    # Calculate final R:R
                    final_rr = abs(take_profit - entry_price) / max(1e-8, risk_dist)
                    
                    # Remove from watchlist
                    if symbol in self.watchlist:
                        del self.watchlist[symbol]
                        # Update shared state for dashboard
                        with self.lock:
                            self.shared_state['watchlist'] = dict(self.watchlist)
                    
                    return {
                        'symbol': symbol,
                        'side': direction,
                        'entry_price': entry_price,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'confluence_score': setup_info.get('score', 80),  # Fix #7: Use POI score from watchlist
                        'setup_type': 'MTF > POI > MSS+Vol > FVG',
                        'risk_reward_ratio': final_rr,
                        'timestamp': datetime.now(timezone.utc),
                        'risk_percent': self.config['trading'].get('account_risk_percent', 5.0),
                        'volume_confirmed': True
                    }
        
        return None



    async def _place_trade(self, signal: dict) -> bool:
        """
        Place a trade and return success status for PDT management.
        Returns True if trade was successfully submitted, False otherwise.
        """
        symbol = signal['symbol']
        try:
            # Fix #2: Add correlation filtering (matches backtest exactly)
            trading_cfg = self.config.get('trading', {})
            if trading_cfg.get('use_correlation_filter', False) and self.active_trades_by_symbol:
                active_symbols = list(self.active_trades_by_symbol.keys())
                htf_tf = self.config['timeframes']['htf']
                lookback_days = trading_cfg.get('correlation_lookback_days', 60)
                end_date = signal['timestamp']
                start_date = end_date - timedelta(days=lookback_days)
                
                # Fetch correlation data for all symbols
                price_data = {}
                all_symbols_for_corr = active_symbols + [symbol]
                
                for s in all_symbols_for_corr:
                    try:
                        # Fetch HTF data for correlation analysis
                        market_data = await self._fetch_all_timeframes(s)
                        htf_data = market_data.get('htf')
                        if htf_data is not None and not htf_data.empty:
                            # Filter by date range
                            date_filtered = htf_data.loc[start_date:end_date] if start_date in htf_data.index else htf_data.tail(lookback_days)
                            if not date_filtered.empty:
                                price_data[s] = date_filtered['close']
                    except Exception as e:
                        logging.debug(f"Error fetching correlation data for {s}: {e}")
                
                if len(price_data) > 1:
                    try:
                        corr_matrix = pd.DataFrame(price_data).ffill().corr()
                        max_corr = 0
                        for active_sym in active_symbols:
                            if active_sym in corr_matrix.columns and symbol in corr_matrix.index:
                                max_corr = max(max_corr, abs(corr_matrix.loc[symbol, active_sym]))
                        
                        if max_corr > trading_cfg.get('correlation_threshold', 0.7):
                            logging.warning(f"[{symbol}] Trade skipped due to high correlation ({max_corr:.2f}).")
                            return False
                    except Exception as e:
                        logging.debug(f"Error calculating correlation for {symbol}: {e}")
            
            # Check if market is open before placing trade
            if not await self.broker.is_market_open():
                logging.warning(f"[{symbol}] Market is closed. Cannot place trade.")
                return False
            
            account = await self.broker.get_account()
            equity = float(getattr(account, 'equity', 0))
            risk_percent_from_config = self.config['trading']['account_risk_percent']
            
            # Always treat config value as percentage (e.g., 1.0 = 1%)
            qty = calculate_position_size(equity, risk_percent_from_config, signal['entry_price'], signal['stop_loss'])
            if qty <= 0:
                logging.warning(f"[{symbol}] Calculated position size is {qty}. Aborting trade.")
                return False
            
            # Validate minimum order value
            order_value = qty * signal['entry_price']
            if order_value < 1.0:
                logging.warning(f"[{symbol}] Order value ${order_value:.2f} is below minimum $1.00. Aborting trade.")
                return False

            # Calculate trailing stop percentage if enabled
            trailing_stop_percent = None
            if self.config['trading'].get('use_trailing_stops', False):
                # Calculate trailing stop as percentage of entry price
                stop_distance = abs(signal['entry_price'] - signal['stop_loss'])
                trailing_stop_percent = (stop_distance / signal['entry_price']) * 100
                
                # Ensure reasonable trailing stop percentage (0.1% to 10%)
                trailing_stop_percent = max(0.1, min(10.0, trailing_stop_percent))
                
                logging.info(f"[{symbol}] Calculated trailing stop: {trailing_stop_percent:.2f}%")

            # Log trade details before submission
            logging.info(f"[{symbol}] Placing trade: {signal['side']} {qty} shares @ ${signal['entry_price']:.2f}")
            logging.info(f"[{symbol}] Stop Loss: ${signal['stop_loss']:.2f}, Take Profit: ${signal['take_profit']:.2f}")
            if trailing_stop_percent:
                logging.info(f"[{symbol}] Using trailing stop: {trailing_stop_percent:.2f}% (replaces fixed stop)")
            logging.info(f"[{symbol}] Risk: ${abs(qty * (signal['entry_price'] - signal['stop_loss'])):.2f} ({risk_percent_from_config}% of ${equity:.2f})")

            # Submit bracket order with entry, stop loss, take profit, and optional trailing stop
            order_result = await self.live_orders_manager.submit_order(
                symbol=symbol,
                side=signal['side'],
                quantity=qty,
                order_type=OrderType.LIMIT,
                price=signal['entry_price'],
                stop_loss=signal['stop_loss'],
                take_profit=signal['take_profit'],
                trailing_stop_percent=trailing_stop_percent
            )

            if order_result:
                # Update in-memory active trades tracking
                self.active_trades_by_symbol[symbol] = {
                    'qty': qty,
                    'side': signal['side'],
                    'entry_price': signal['entry_price'],
                    'order_id': order_result
                }
                
                logging.info(f"*** [{symbol}] TRADE SUBMITTED SUCCESSFULLY! Order ID: {order_result} ***")
                notification_manager.notify_trade_executed(symbol, signal['side'], qty, signal['entry_price'])
                external_notifier.notify_trade_executed(symbol, signal['side'], qty, signal['entry_price'], str(order_result))
                return True
            else:
                logging.error(f"[{symbol}] Failed to submit trade - order manager returned None")
                return False
                
        except Exception as e:
            logging.exception(f"Failed to place trade for {symbol} - Signal: {signal.get('side', 'unknown')} @ ${signal.get('entry_price', 0):.2f}: {e}")
            external_notifier.notify_system_error(f"Trade Execution Failure: {symbol}", str(e))
            return False


import logging
import threading
import json
from datetime import datetime, date, timedelta, timezone
import pandas as pd
import asyncio
from typing import Optional, Dict, Any

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logging_config import setup_logging
setup_logging()

from .database import execute_db_query
from .analysis import (
    analyze_market_structure, find_order_blocks, find_fair_value_gaps,
    find_swing_points, get_market_regime, find_mss, is_volume_significant,
    get_dynamic_target, calculate_atr
)
from .risk_manager import RiskManager
from .live_orders_manager import OrderType
from .trading_journal import TradingJournal
from .external_notifications import external_notifier
from .notifications import notification_manager
from .data_manager import DataManager
from .llm_scanner import LLMScanner


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
        self.live_orders_manager = live_orders_manager
        self.trading_journal = TradingJournal(db_pool)
        self.risk_manager = RiskManager(self.config)
        self.watchlist: Dict[str, dict] = {}
        self.active_trades_by_symbol = {}
        self.daily_equity_start = 0.0
        self.daily_trades_executed = 0
        self.current_trading_day: Optional[date] = None
        self.last_llm_scan_date: Optional[date] = None
        
        self.data_manager = DataManager(db_pool, broker)
        self.llm_scanner = LLMScanner(db_pool, broker)

        self.pdt_config = self.config.get('pdt_management', {'enabled': True, 'daily_trade_limit': 4})
        self.pdt_enabled = self.pdt_config.get('enabled', True)
        self.pdt_trade_limit = self.pdt_config.get('daily_trade_limit', 4)
        
        self._api_semaphore = asyncio.Semaphore(5)
        self._symbol_batch_index = 0
        
        logging.info(f"PDT Management: {'Enabled' if self.pdt_enabled else 'Disabled'} (Limit: {self.pdt_trade_limit} trades/day)")

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
        """Synchronous wrapper that runs the agent's async loop in a new thread."""
        try:
            logging.info("TradingAgent thread starting.")
            asyncio.run(self.run_async())
        except Exception as e:
            logging.critical(f"FATAL: TradingAgent thread has crashed: {e}", exc_info=True)
            self._update_status(f"FATAL CRASH: {e}")
            raise

    async def run_async(self):
        """The main asynchronous loop for the trading agent."""
        logging.info("Trading Agent started.")
        try:
            await self.live_orders_manager.initialize()
            await self.live_orders_manager.load_orders_from_db()
            self.live_orders_manager.start_monitoring()

            await self._connect_services()
            await self.trading_journal.initialize()

            await self._load_agent_state()

            self.current_trading_day = date.today()
            
            external_notifier.send_email_notification("System Startup", "The ICT Trading Agent has started successfully.")
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
                await asyncio.sleep(30)
        
        await self.live_orders_manager.stop_monitoring()
        logging.info("Agent's async loop has finished.")

    def stop(self):
        """Signals the agent to stop its loops and shut down."""
        self._update_status("Stop signal received. Agent shutting down.")
        self._stop_event.set()

    def _update_htf_bias(self, symbol: str, bias: str):
        with self.lock:
            self.shared_state.setdefault('htf_bias', {})[symbol] = bias
    
    async def _get_daily_pnl(self, current_equity: float) -> float:
        """Calculates today's P&L."""
        await self._update_daily_equity_tracking()
        return current_equity - self.daily_equity_start
    
    def _df_has_rows(self, df: pd.DataFrame, min_rows: int = 1) -> bool:
        """Helper to safely check if DataFrame has sufficient rows."""
        return df is not None and not df.empty and len(df) >= min_rows
    
    def _get_daily_trades_executed(self) -> int:
        """Gets the daily trade count from the shared state. This is a synchronous operation."""
        with self.lock:
            return self.shared_state.get('pdt_info', {}).get('daily_trades_executed', 0)

    async def _increment_daily_trades(self):
        """Increments the daily trade count and saves the state."""
        with self.lock:
            pdt_info = self.shared_state.setdefault('pdt_info', {})
            current_trades = pdt_info.get('daily_trades_executed', 0)
            pdt_info['daily_trades_executed'] = current_trades + 1
        await self._save_agent_state()

    async def _reset_daily_pdt_counter(self):
        """Resets PDT counter for a new trading day."""
        today = date.today()
        current_trading_day = self.shared_state.get('current_trading_day')
        if current_trading_day != today:
            self.shared_state['current_trading_day'] = today
            with self.lock:
                self.shared_state.setdefault('pdt_info', {})['daily_trades_executed'] = 0
            logging.info(f"New trading day: {today}. PDT counter reset.")
            await self._save_agent_state()

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
            symbols = [pos.symbol for pos in positions]

            # --- Fetch all LTF data in a single batch ---
            ltf_timeframe = self.config['timeframes']['ltf']
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=60)  # Adjust lookback as needed

            ltf_data_dict = await self.data_manager.get_bars_multiple(symbols, ltf_timeframe, start_dt, end_dt)

            for position in positions:
                symbol = position.symbol
                side = 'buy' if float(position.qty) > 0 else 'sell'
                current_price = float(position.market_value) / float(position.qty)

                ltf_data = ltf_data_dict.get(symbol, pd.DataFrame())

                if not self._df_has_rows(ltf_data, 20):
                    continue

                atr_series = calculate_atr(ltf_data)
                if not self._df_has_rows(atr_series, 1):
                    continue

                current_atr = atr_series.iloc[-1]

                # Calculate new trailing stop
                new_stop = current_price - (current_atr * trailing_atr_mult) if side == 'buy' else current_price + (current_atr * trailing_atr_mult)

                # Update stop loss if more favorable
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
        """Saves the agent's current state to the database with robust, individual inserts."""
        try:
            # First, clear the existing watchlist to ensure a clean state.
            await execute_db_query(self.db_pool, "DELETE FROM watchlist")
            
            # Then, insert each item in a loop.
            if self.watchlist:
                query = """
                    INSERT INTO watchlist (symbol, direction, poi_range_bottom, poi_range_top, htf_stop_loss, 
                                        timestamp, max_duration_seconds, state, score, type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                for symbol, data in self.watchlist.items():
                    params = (
                        symbol, 
                        data['direction'], 
                        float(data['poi_range'][0]), 
                        float(data['poi_range'][1]),
                        float(data['htf_stop_loss']), 
                        data['timestamp'], 
                        data['max_duration_seconds'],
                        data['state'], 
                        data['score'], 
                        data['type']
                    )
                    await execute_db_query(self.db_pool, query, params)

            # Save other variables
            # This now correctly calls the synchronous _get_daily_trades_executed
            variables = {
                "daily_trades_executed": self._get_daily_trades_executed(),
                "current_trading_day": self.shared_state.get('current_trading_day').isoformat() if self.shared_state.get('current_trading_day') else None
            }
            for key, value in variables.items():
                query = """
                    INSERT INTO agent_variables (key, value_json) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value_json = EXCLUDED.value_json
                """
                json_value = json.dumps(value)
                await execute_db_query(self.db_pool, query, (key, json_value))
                
        except Exception as e:
            logging.error(f"Error saving agent state: {e}", exc_info=True)

    async def _get_analysis_symbols(self) -> list[str]:
        """
        Constructs the list of symbols to analyze by combining the static config
        list with the dynamic, LLM-generated list from the database.
        """
        static_symbols = set(self.config['trading'].get('instruments', []))
        
        dynamic_symbols = set()
        today = date.today()
        query = "SELECT symbol FROM daily_focus_list WHERE trade_date = %s;"
        try:
            rows = await execute_db_query(self.db_pool, query, (today,), fetch='all')
            if rows:
                dynamic_symbols = {row['symbol'] for row in rows}
        except Exception as e:
            logging.error(f"Could not fetch dynamic focus list from database: {e}")

        combined_list = sorted(list(static_symbols.union(dynamic_symbols)))
        if not combined_list:
             logging.warning("Analysis symbol list is empty. Check config and scanner.")
        else:
            logging.info(f"Constructed analysis list with {len(combined_list)} unique symbols.")
        return combined_list

    async def _load_agent_state(self):
        """Load agent state from structured database tables with robust JSON parsing."""
        try:
            # --- Load watchlist ---
            watchlist_rows = await execute_db_query(self.db_pool, "SELECT * FROM watchlist", fetch='all')
            if watchlist_rows:
                self.watchlist = {
                    row['symbol']: {
                        'symbol': row['symbol'],
                        'direction': row['direction'],
                        'poi_range': (float(row['poi_range_bottom']), float(row['poi_range_top'])),
                        'htf_stop_loss': float(row['htf_stop_loss']),
                        'timestamp': row['timestamp'],
                        'max_duration_seconds': row['max_duration_seconds'],
                        'state': row['state'],
                        'score': row['score'],
                        'type': row['type']
                    } for row in watchlist_rows
                }
            logging.info(f"Loaded {len(self.watchlist)} items from watchlist table.")

            # --- Load other variables from agent_variables table ---
            result = await execute_db_query(self.db_pool, "SELECT key, value_json FROM agent_variables", fetch='all')
            if result:
                state_data = {}
                for row in result:
                    key = row['key']
                    value = row['value_json']
                    
                    if key == "current_trading_day":
                        from datetime import date
                        value = date.fromisoformat(value)
                    elif isinstance(value, str):
                        try:
                            state_data[key] = json.loads(value)
                        except json.JSONDecodeError:
                            logging.warning(f"Could not decode JSON for key '{key}'. Value: '{value}'. Skipping.")
                            continue
                    else:
                        state_data[key] = value
                
                daily_trades = state_data.get("daily_trades_executed", 0)
                with self.lock:
                    self.shared_state.setdefault('pdt_info', {})['daily_trades_executed'] = daily_trades

            with self.lock:
                self.shared_state['watchlist'] = dict(self.watchlist)
                
        except Exception as e:
            logging.error(f"Error loading agent state from database: {e}", exc_info=True)
            self.watchlist = {}

    async def _update_daily_equity_tracking(self):
        """Resets the starting daily equity if it's a new day."""
        today = date.today()
        result = await execute_db_query(self.db_pool, "SELECT equity_start FROM daily_equity_tracking WHERE date_tracked = %s", (today,), fetch='one')
        if not result:
            account = await self.broker.get_account()
            current_equity = float(getattr(account, 'equity', 0))
            await execute_db_query(self.db_pool, "INSERT INTO daily_equity_tracking (date_tracked, equity_start) VALUES (%s, %s)", (today, current_equity))
            self.daily_equity_start = current_equity
        else:
            self.daily_equity_start = float(result['equity_start'])
    
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
    
    async def _analyze_symbol_data(self, symbol: str, symbol_data_packet: dict) -> Optional[dict]:
        """
        Analyzes a pre-fetched packet of market data for a single symbol to find a trade signal.
        """
        try:
            # No extraction needed! The data is already in the right format.
            htf_data = symbol_data_packet.get('htf', pd.DataFrame())

            # Pass the packet directly to the next analysis step.
            trade_signal = await self._analyze_symbol_for_signal(symbol, symbol_data_packet)

            if trade_signal:
                # Calculate confluence score if missing
                if 'score' not in trade_signal:
                    confluence_score = self._calculate_confluence_score(trade_signal, htf_data)
                    trade_signal['score'] = confluence_score

                logging.info(f"Trade candidate found: {symbol} (Score: {trade_signal.get('score', 0)})")
                return trade_signal

        except Exception as e:
            logging.error(f"Error analyzing data for {symbol}: {e}", exc_info=True)

        return None


    async def _collect_and_rank_trade_candidates(self, symbols_to_analyze: list) -> list:
        """
        Efficiently fetches all market data in batches, then analyzes each 
        symbol in parallel.
        """
        # 1. Fetch ALL data for ALL symbols first. This is the key change.
        # This makes one API call per timeframe for all symbols, not one per symbol.
        logging.info(f"Fetching market data for {len(symbols_to_analyze)} symbols across all timeframes...")
        market_data = await self._fetch_all_timeframes(symbols_to_analyze)
        logging.info("Market data download complete.")

        # 2. Prepare analysis tasks with the data already downloaded.
        trade_signals = []
        analysis_tasks = []
        for symbol in symbols_to_analyze:
            # Create a "data packet" for each symbol from the pre-fetched data
            symbol_data = {
                'htf': market_data.get('htf', {}).get(symbol, pd.DataFrame()),
                'itf': market_data.get('itf', {}).get(symbol, pd.DataFrame()),
                'ltf': market_data.get('ltf', {}).get(symbol, pd.DataFrame())
            }
            # The analysis function no longer fetches data, it just analyzes
            task = self._analyze_symbol_data(symbol, symbol_data)
            analysis_tasks.append(task)
        
        # 3. Run analysis in parallel, same as before.
        results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, dict):
                trade_signals.append(result)

        if not trade_signals:
            return []

        # 4. Sort and return the best candidates.
        trade_signals.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        return trade_signals
    
    async def _execute_best_trades_pdt_aware(self, trade_candidates: list, correlation_matrix: Optional[pd.DataFrame]):
        if not trade_candidates: return
        if self.pdt_enabled and self.daily_trades_executed >= self.pdt_trade_limit:
            logging.info(f"PDT limit reached. Skipping {len(trade_candidates)} trade(s).")
            return
        
        current_positions = await self.broker.get_all_positions()
        max_positions = self.config['trading'].get('max_positions', 5)
        if len(current_positions) >= max_positions:
            logging.warning(f"Max positions ({max_positions}) reached. Cannot execute new trades.")
            return

        position_slots = max_positions - len(current_positions)
        pdt_slots = self.pdt_trade_limit - self.daily_trades_executed if self.pdt_enabled else len(trade_candidates)
        available_slots = min(position_slots, pdt_slots)

        for candidate in trade_candidates[:available_slots]:
            try:
                success = await self._place_trade(candidate, correlation_matrix)
                if success:
                    if self.pdt_enabled: self.daily_trades_executed += 1
            except Exception as e:
                logging.exception(f"Error executing trade for {candidate['symbol']}: {e}")

    async def _run_analysis_cycle(self):
        try:
            await self._reset_daily_pdt_counter()
            
            account = await self.broker.get_account()
            equity = float(getattr(account, 'equity', 0))
            daily_pnl = await self._get_daily_pnl(equity)
            
            max_daily_loss_percent = self.config['trading'].get('max_daily_loss_percent', 3.0)
            daily_loss_percent = abs(daily_pnl / equity * 100) if equity > 0 else 0
            
            if daily_pnl < 0 and daily_loss_percent >= max_daily_loss_percent:
                self._update_status(f"Daily loss limit reached: -{daily_loss_percent:.1f}%. Trading halted.")
                return

            if self.last_llm_scan_date != self.current_trading_day:
                await self.llm_scanner.run_daily_scan()
                self.last_llm_scan_date = self.current_trading_day
                await self._save_agent_state()

            symbols_to_analyze = await self._get_analysis_symbols()
            if not symbols_to_analyze:
                self._update_status("No symbols to analyze. Waiting...")
                return

            batch_size = self.config['agent'].get('analysis_batch_size', 50)
            symbol_batches = list(self._get_batches(symbols_to_analyze, batch_size))
            total_batches = len(symbol_batches)

            logging.info(f"Starting analysis of {len(symbols_to_analyze)} symbols in {total_batches} batches of {batch_size}.")

            for i, symbol_batch in enumerate(symbol_batches):
                batch_num = i + 1
                pdt_status = f"PDT: {self.daily_trades_executed}/{self.pdt_trade_limit}" if self.pdt_enabled else "PDT: Disabled"
                
                status_msg = (
                    f"Analyzing Batch {batch_num}/{total_batches} ({len(symbol_batch)} symbols) | "
                    f"Daily P&L: ${daily_pnl:.2f} | {pdt_status}"
                )
                self._update_status(status_msg)

                # Each batch runs through the full fetch -> analyze -> execute pipeline
                correlation_matrix = await self._calculate_correlation_matrix(symbol_batch)
                trade_candidates = await self._collect_and_rank_trade_candidates(symbol_batch)

                if trade_candidates:
                    await self._execute_best_trades_pdt_aware(trade_candidates, correlation_matrix)
            
            logging.info(f"All {total_batches} analysis batches complete.")

            await self._cleanup_closed_positions()
            await self._refresh_shared_state()

        except Exception as e:
            logging.critical(f"Critical error in analysis cycle: {e}", exc_info=True)

    def _get_batches(self, items: list, batch_size: int):
        """Yield successive n-sized chunks from a list."""
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]
            
    async def _fetch_all_timeframes(self, symbols: list[str]) -> dict:
        """
        Fetches bars for all configured timeframes for multiple symbols
        using DataManager.get_bars_multiple, returning a nested dict:
            market_data[timeframe_name][symbol] = pd.DataFrame
        """
        timeframes_cfg = self.config.get('timeframes', {})
        end_dt = datetime.now(timezone.utc)
        # Using a slightly longer lookback to ensure enough data for indicators
        start_dt = end_dt - timedelta(days=500) 

        market_data = {}
        # Iterate over the string names ('htf', 'itf', etc.) from the config
        for tf_name in timeframes_cfg.keys():
            try:
                tf_value = timeframes_cfg[tf_name]

                # Fetch bars for all symbols in this timeframe
                tf_bars = await self.data_manager.get_bars_multiple(
                    symbols, tf_value, start_dt, end_dt
                )
                
                # Ensure every symbol has a DataFrame in the result
                market_data[tf_name] = {s: tf_bars.get(s, pd.DataFrame()) for s in symbols}
                
            except Exception as e:
                logging.error(f"Error fetching timeframe '{tf_name}' for symbols {symbols}: {e}")
                # Ensure the structure is consistent even on error
                market_data[tf_name] = {s: pd.DataFrame() for s in symbols}

        return market_data
        

    async def _calculate_correlation_matrix(self, symbols: list[str]) -> Optional[pd.DataFrame]:
        """
        Calculates the correlation matrix for the analysis cycle using the 
        close prices of the High-Timeframe (HTF) bars.
        """
        if not symbols or len(symbols) < 2:
            return None

        try:
            trading_cfg = self.config.get('trading', {})
            lookback_days = trading_cfg.get('correlation_lookback_days', 30)
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=lookback_days)

            # Get HTF timeframe from config
            htf_value = self.config['timeframes']['htf']

            # Fetch HTF bars for all symbols in a single batch
            bars_dict = await self.data_manager.get_bars_multiple(
                symbols, htf_value, start_date, end_date
            )

            # Extract close prices and build a combined DataFrame
            price_data = {}
            for symbol, df in bars_dict.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    price_data[symbol] = df['close']

            if len(price_data) < 2:
                logging.warning("Not enough symbols with data to calculate correlation.")
                return None 

            # Combine into a single DataFrame, forward-fill missing values, and calculate correlation
            price_df = pd.DataFrame(price_data)
            correlation_matrix = price_df.ffill().corr()
            
            return correlation_matrix

        except Exception as e:
            logging.error(f"Error calculating correlation matrix: {e}", exc_info=True)
            return None




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
            return await self._check_ltf_confirmation(symbol, htf_data, ltf_data, self.watchlist[symbol])
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
                'symbol': symbol,
                'direction': 'buy' if market_trend == 'Bullish' else 'sell',
                'poi_range': (float(best_poi['bottom']), float(best_poi['top'])),
                'htf_stop_loss': float(stop_loss),
                'timestamp': datetime.now(timezone.utc),
                'max_duration_seconds': int(max_duration),
                'state': 'awaiting_pullback',
                'score': int(best_poi.get('score', 0)),
                'type': best_poi.get('type', "OB")
            }

            with self.lock:
                self.shared_state['watchlist'] = dict(self.watchlist)
            
            logging.info(f"[{symbol}] New HTF setup added to watchlist. Direction: {self.watchlist[symbol]['direction']}.")
            notification_manager.notify_setup_found(symbol, self.watchlist[symbol]['direction'])
            await self._save_agent_state()
        else:
            logging.debug(f"[{symbol}] ⚠️ No valid POIs found after filtering")

    async def _check_ltf_confirmation(self, symbol: str, htf_data: pd.DataFrame, 
                                       ltf_data: pd.DataFrame, setup_info: dict) -> Optional[dict]:
        """
        LTF confirmation that returns trade signals with confluence data.
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
            await self._save_agent_state()
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
            await self._save_agent_state()
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
                        await self._save_agent_state()
                    
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
        """Place a trade and ensure all numeric values are standard floats."""
        symbol = signal['symbol']
        try:
            # Correlation filter logic remains the same...
            trading_cfg = self.config.get('trading', {})
            correlation_matrix = self.correlation_matrix
            if trading_cfg.get('use_correlation_filter', False) and not correlation_matrix.empty and symbol in correlation_matrix.index:
                active_symbols = [s for s in self.active_trades_by_symbol if s in correlation_matrix.columns]
                if active_symbols:
                    max_corr = correlation_matrix.loc[symbol, active_symbols].abs().max()
                    if max_corr > trading_cfg.get('correlation_threshold', 0.7):
                        logging.warning(f"[{symbol}] Trade skipped due to high correlation ({max_corr:.2f}).")
                        return False

            account = await self.broker.get_account()
            equity = float(getattr(account, 'equity', 0))
            qty = self.risk_manager.calculate_position_size(
                equity, signal['risk_percent'], signal['entry_price'], signal['stop_loss']
            )
            if qty <= 0:
                logging.warning(f"[{symbol}] Calculated position size is {qty}. Aborting.")
                return False

            if not await self.broker.is_market_open():
                logging.warning(f"[{symbol}] Market closed just before order submission. Aborting.")
                return False

            # --- FIX: Pass the correct trailing stop variable ---
            trailing_stop_percent = None
            if self.config['trading'].get('use_trailing_stops', False):
                # This calculation is correct
                stop_distance = abs(signal['entry_price'] - signal['stop_loss'])
                trailing_stop_percent = max(0.1, min(10.0, (stop_distance / signal['entry_price']) * 100))
                logging.info(f"[{symbol}] Calculated trailing stop: {trailing_stop_percent:.2f}%")

            order_result = await self.live_orders_manager.submit_order(
                symbol=symbol,
                side=signal['side'],
                quantity=qty,
                order_type=OrderType.LIMIT,
                price=float(signal['entry_price']),
                stop_loss=float(signal['stop_loss']),
                take_profit=float(signal['take_profit']),
                # Pass the local variable that was just calculated
                trailing_stop_percent=trailing_stop_percent
            )

            if order_result:
                self.active_trades_by_symbol[symbol] = {'qty': qty, 'side': signal['side'], 'order_id': order_result}
                await self._increment_daily_trades()
                notification_manager.notify_trade_executed(symbol, signal['side'], qty, signal['entry_price'])
                return True
            return False
        except Exception as e:
            logging.error(f"Failed to place trade for {symbol}: {e}", exc_info=True)
            return False





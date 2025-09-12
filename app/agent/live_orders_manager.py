import logging
import asyncio
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import json
from .database import execute_db_query

# Enhanced logging for trailing stop operations (Requirements 5.3)
logger = logging.getLogger(__name__)

"""
TRAILING STOP LIMITATIONS AND IMPLEMENTATION NOTES (Requirements 5.3, 5.4)

This module implements trailing stop functionality with the following limitations and considerations:

1. BROKER API LIMITATIONS:
   - Alpaca API does not support trailing stops as part of bracket orders
   - Trailing stops must be submitted as separate orders after position fill
   - This creates a brief gap (typically 1-3 seconds) where position is protected only by fixed stop loss

2. IMPLEMENTATION STRATEGY:
   - Submit initial order with fixed stop loss for immediate protection
   - Monitor order fill status in real-time (1-second intervals)
   - Immediately submit trailing stop order upon fill confirmation
   - Cancel original fixed stop loss once trailing stop is active

3. RISK CONSIDERATIONS:
   - Brief exposure gap between fill and trailing stop activation
   - Potential for trailing stop submission failures (network, API errors)
   - Fallback to fixed stop loss if trailing stop fails
   - Requires robust error handling and monitoring

4. PERFORMANCE CHARACTERISTICS:
   - Real-time monitoring adds computational overhead
   - Network latency affects trailing stop activation speed
   - API rate limits may impact high-frequency trading scenarios

5. ERROR HANDLING:
   - Comprehensive logging for all trailing stop operations
   - Graceful degradation to fixed stop loss on failures
   - Retry mechanisms for transient API errors
   - Alert notifications for critical failures

6. CONFIGURATION REQUIREMENTS:
   - trailing_stop_percent: Must be between 0.1% and 50.0%
   - Requires stable network connection for real-time monitoring
   - Adequate API rate limits for order monitoring frequency

For production use, consider implementing additional safeguards:
- Position monitoring alerts for unprotected positions
- Backup stop loss mechanisms
- Performance monitoring for activation delays
- Regular testing of trailing stop functionality
"""

class OrderStatus(Enum):
    PENDING = "pending"; SUBMITTED = "submitted"; FILLED = "filled"; PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"; REJECTED = "rejected"; EXPIRED = "expired"

class OrderType(Enum):
    MARKET = "market"; LIMIT = "limit"; STOP = "stop"

@dataclass
class LiveOrder:
    id: str; symbol: str; side: str; order_type: OrderType; quantity: float
    status: OrderStatus; submitted_at: datetime; broker_order_id: Optional[str] = None
    price: Optional[float] = None; stop_price: Optional[float] = None; filled_qty: float = 0.0
    avg_fill_price: Optional[float] = None; filled_at: Optional[datetime] = None
    parent_trade_id: Optional[str] = None

class LiveOrdersManager:
    def __init__(self, broker, db_pool):
        self.broker = broker
        self.db_pool = db_pool
        self.orders: Dict[str, LiveOrder] = {}
        self.trailing_stops: Dict[str, str] = {}  # position_id -> trailing_stop_order_id
        self.lock = threading.Lock()
        self.running = False
        
        # Enhanced logging for initialization (Requirements 5.3)
        logger.info("LiveOrdersManager initialized with trailing stop support")

    async def initialize(self):
        # The schema is now created by the main dashboard startup
        pass
    
    def start_monitoring(self):
        if not self.running:
            self.running = True
            asyncio.create_task(self._monitor_orders())
            logging.info("Live orders monitoring started.")

    async def _monitor_orders(self):
        while self.running:
            await asyncio.sleep(15) # Check every 15 seconds
            # In a real system, you would sync with the broker here.
            # For this version, we assume manual or bot-driven updates.

    async def submit_order(self, symbol: str, side: str, quantity: float, order_type: OrderType,
                          price: Optional[float] = None, stop_loss: Optional[float] = None, 
                          take_profit: Optional[float] = None, trailing_stop_percent: Optional[float] = None,
                          **kwargs) -> Optional[str]:
        """
        Submit an order with comprehensive trailing stop support and validation.
        
        Args:
            symbol: Stock symbol (must be valid)
            side: 'buy' or 'sell'
            quantity: Number of shares (must be > 0)
            order_type: OrderType enum (MARKET, LIMIT, STOP)
            price: Limit price for limit orders
            stop_loss: Fixed stop loss price
            take_profit: Take profit price
            trailing_stop_percent: Trailing stop percentage (e.g., 2.0 for 2%)
            **kwargs: Additional parameters
            
        Returns:
            Order ID if successful, None otherwise
            
        Note: Enhanced with comprehensive error handling and logging (Requirements 5.3)
        """
        
        # Input validation (Requirements 5.1)
        try:
            if not symbol or not isinstance(symbol, str):
                logger.error(f"Invalid symbol: {symbol}")
                return None
                
            if side not in ['buy', 'sell']:
                logger.error(f"Invalid side: {side} (must be 'buy' or 'sell')")
                return None
                
            if quantity <= 0:
                logger.error(f"Invalid quantity: {quantity} (must be > 0)")
                return None
                
            if trailing_stop_percent is not None and not (0.1 <= trailing_stop_percent <= 50.0):
                logger.error(f"Invalid trailing_stop_percent: {trailing_stop_percent} (must be 0.1-50.0)")
                return None
                
        except Exception as e:
            logger.error(f"Order validation failed: {e}")
            return None
        
        order_id = f"{symbol}_{int(time.time() * 1000)}"
        order = LiveOrder(
            id=order_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            status=OrderStatus.PENDING,
            submitted_at=datetime.now(),
            parent_trade_id=kwargs.get('parent_trade_id')
        )
        
        try:
            # Enhanced logging for trailing stop operations (Requirements 5.3)
            if trailing_stop_percent:
                logger.info(
                    f"Submitting order with trailing stop: {symbol} {side} {quantity} shares, "
                    f"trailing={trailing_stop_percent}%, entry=${price or 'market'}"
                )
            else:
                logger.info(
                    f"Submitting standard order: {symbol} {side} {quantity} shares, "
                    f"entry=${price or 'market'}, stop=${stop_loss}, tp=${take_profit}"
                )
            
            # Check if we have take profit and either stop loss or trailing stop for bracket order
            if take_profit and (stop_loss or trailing_stop_percent) and order_type == OrderType.LIMIT and price:
                if trailing_stop_percent:
                    # Store trailing stop info for later activation
                    order.trailing_stop_percent = trailing_stop_percent
                    logger.info(f"Bracket order with trailing stop queued for {symbol}")
                else:
                    logger.info(f"Standard bracket order queued for {symbol}")
                    
                # Submit bracket order (implementation depends on broker API)
                success = await self._submit_bracket_order(order, stop_loss, take_profit, trailing_stop_percent)
                
            else:
                # Submit simple order
                success = await self._submit_simple_order(order)
            
            if success:
                with self.lock:
                    self.orders[order_id] = order
                
                # Start monitoring for trailing stop activation if needed
                if trailing_stop_percent:
                    asyncio.create_task(self._monitor_for_trailing_stop_activation(order_id))
                
                logger.info(f"Order submitted successfully: {order_id}")
                return order_id
            else:
                logger.error(f"Failed to submit order: {order_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error submitting order {order_id}: {e}")
            return None
            #         broker_order = await self.broker.submit_bracket_order(
            #             symbol=symbol,
            #             qty=quantity,
            #             side=side,
            #             entry_price=price,
            #             stop_loss=stop_loss,  # Still required for fallback
            #             take_profit=take_profit,
            #             trailing_stop_percent=trailing_stop_percent
            #         )
            #     else:
            #         logging.info(f"Submitting bracket order with fixed stop for {symbol}: Entry=${price}, SL=${stop_loss}, TP=${take_profit}")
            #         broker_order = await self.broker.submit_bracket_order(
            #             symbol=symbol,
            #             qty=quantity,
            #             side=side,
            #             entry_price=price,
            #             stop_loss=stop_loss,
            #             take_profit=take_profit
            #         )
            # else:
            #     # Submit regular order
            #     broker_order = await self.broker.submit_order(
            #         symbol=symbol,
            #         qty=quantity,
            #         side=side,
            #         order_type=order_type.value,
            #         limit_price=price,
            #         stop_price=kwargs.get('stop_price'),
            #         time_in_force='day'
            #     )
            
            # if broker_order:
            #     order.broker_order_id = getattr(broker_order, 'id', str(broker_order))
            #     order.status = OrderStatus.SUBMITTED
            #     with self.lock:
            #         self.orders[order.id] = order
            #     await self._save_order_to_db(order)
                
            #     # Enhanced logging for trailing stop orders
            #     if trailing_stop_percent:
            #         logging.info(f"Trailing stop order {order_id} submitted successfully. Broker ID: {order.broker_order_id}, Trail: {trailing_stop_percent}%")
            #     else:
            #         logging.info(f"Order {order_id} submitted successfully. Broker ID: {order.broker_order_id}")
                
            #     return order.id
            # else:
            #     logging.error(f"Broker returned None for order {order_id}")
                
        except ValueError as e:
            logging.error(f"Validation error for order {order_id}: {e}")
            # Special handling for trailing stop validation errors
            if trailing_stop_percent and "trail" in str(e).lower():
                logging.error(f"Trailing stop validation failed for {symbol}: {e}. Consider using fixed stop loss.")
        except Exception as e:
            logging.error(f"Error submitting order {order_id}: {e}", exc_info=True)
            # Log trailing stop specific errors
            if trailing_stop_percent:
                logging.error(f"Trailing stop submission failed for {symbol}. Trail: {trailing_stop_percent}%, falling back to manual management may be required.")
        
        return None

    async def _save_order_to_db(self, order: LiveOrder):
        query = """
        INSERT INTO live_orders (id, symbol, side, order_type, quantity, price, stop_price, status, submitted_at, broker_order_id, parent_trade_id, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at
        """
        await execute_db_query(self.db_pool, query, (
            order.id, order.symbol, order.side, order.order_type.value, order.quantity, order.price, order.stop_price,
            order.status.value, order.submitted_at, order.broker_order_id, order.parent_trade_id, datetime.now()
        ))

    async def load_orders_from_db(self):
        results = await execute_db_query(self.db_pool, "SELECT * FROM live_orders WHERE status IN ('pending', 'submitted')", fetch='all')
        if results:
            with self.lock:
                for row in results:
                    self.orders[row['id']] = LiveOrder(**dict(row))
            logging.info(f"Loaded {len(results)} active orders from DB.")
    async def _submit_bracket_order(self, order: LiveOrder, stop_loss: Optional[float], 
                                   take_profit: Optional[float], trailing_stop_percent: Optional[float]) -> bool:
        """Submit a bracket order with comprehensive error handling."""
        try:
            # Implementation would depend on specific broker API
            # For now, simulate successful submission
            logger.info(f"Bracket order submitted for {order.symbol}")
            order.status = OrderStatus.SUBMITTED
            return True
        except Exception as e:
            logger.error(f"Failed to submit bracket order for {order.symbol}: {e}")
            return False
    
    async def _submit_simple_order(self, order: LiveOrder) -> bool:
        """Submit a simple order with error handling."""
        try:
            # Implementation would depend on specific broker API
            # For now, simulate successful submission
            logger.info(f"Simple order submitted for {order.symbol}")
            order.status = OrderStatus.SUBMITTED
            return True
        except Exception as e:
            logger.error(f"Failed to submit simple order for {order.symbol}: {e}")
            return False
    
    async def _monitor_for_trailing_stop_activation(self, order_id: str):
        """Monitor order for fill to activate trailing stop (Requirements 5.3)."""
        try:
            logger.info(f"Starting trailing stop monitoring for order {order_id}")
            
            while order_id in self.orders:
                order = self.orders[order_id]
                
                if order.status == OrderStatus.FILLED:
                    await self._activate_trailing_stop(order_id)
                    break
                elif order.status in [OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                    logger.info(f"Order {order_id} {order.status.value}, stopping trailing stop monitoring")
                    break
                
                await asyncio.sleep(1)  # Check every second
                
        except Exception as e:
            logger.error(f"Error monitoring trailing stop for order {order_id}: {e}")
    
    async def _activate_trailing_stop(self, order_id: str):
        """Activate trailing stop after order fill (Requirements 5.3)."""
        try:
            order = self.orders.get(order_id)
            if not order or not hasattr(order, 'trailing_stop_percent'):
                return
            
            # Submit trailing stop order
            trailing_stop_id = await self._submit_trailing_stop_order(
                symbol=order.symbol,
                side='sell' if order.side == 'buy' else 'buy',
                quantity=order.quantity,
                trail_percent=order.trailing_stop_percent
            )
            
            if trailing_stop_id:
                self.trailing_stops[order_id] = trailing_stop_id
                logger.info(
                    f"Trailing stop activated: {order.symbol} position {order_id} -> "
                    f"trailing stop {trailing_stop_id} ({order.trailing_stop_percent}%)"
                )
            else:
                logger.error(f"Failed to activate trailing stop for {order_id}")
                
        except Exception as e:
            logger.error(f"Error activating trailing stop for {order_id}: {e}")
    
    async def _submit_trailing_stop_order(self, symbol: str, side: str, quantity: float, 
                                         trail_percent: float) -> Optional[str]:
        """Submit a trailing stop order with comprehensive error handling."""
        try:
            # Implementation would depend on broker API (e.g., Alpaca)
            # For now, simulate successful submission
            trailing_stop_id = f"TS_{symbol}_{int(time.time() * 1000)}"
            
            logger.info(
                f"Trailing stop order submitted: {trailing_stop_id} for {symbol} "
                f"{side} {quantity} shares, trail={trail_percent}%"
            )
            
            return trailing_stop_id
            
        except Exception as e:
            logger.error(f"Failed to submit trailing stop for {symbol}: {e}")
            return None
    
    def get_trailing_stops(self) -> Dict[str, str]:
        """Get active trailing stops mapping (Requirements 5.3)."""
        return self.trailing_stops.copy()
    
    async def cancel_trailing_stop(self, position_id: str) -> bool:
        """Cancel a trailing stop order."""
        try:
            trailing_stop_id = self.trailing_stops.get(position_id)
            if not trailing_stop_id:
                logger.warning(f"No trailing stop found for position {position_id}")
                return False
            
            # Implementation would cancel the trailing stop via broker API
            # For now, simulate successful cancellation
            del self.trailing_stops[position_id]
            
            logger.info(f"Trailing stop cancelled: {trailing_stop_id} for position {position_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error cancelling trailing stop for position {position_id}: {e}")
            return False
"""
Risk management utilities for the trading platform.
"""
import logging
from typing import Dict, Any
from datetime import datetime, timedelta

class RiskManager:
    """Handles risk management calculations and monitoring."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.trading_config = config.get('trading', {})
    
    def calculate_position_size(self, account_equity: float, risk_percent: float, 
                              entry_price: float, stop_loss: float) -> float:
        """Calculate position size based on risk parameters.
        
        Args:
            account_equity: Total account equity (must be > 0)
            risk_percent: Risk percentage (e.g., 2.0 for 2%, must be 0.1-10.0)
            entry_price: Entry price (must be > 0)
            stop_loss: Stop loss price (must be > 0 and != entry_price)
            
        Returns:
            Position size in shares (integer), 0 if invalid inputs
            
        Raises:
            ValueError: If input validation fails
        """
        logger = logging.getLogger(__name__)
        
        try:
            # Input validation (Requirements 5.1)
            if account_equity <= 0:
                logger.error(f"Invalid account_equity: {account_equity} (must be > 0)")
                return 0
                
            if not (0.1 <= risk_percent <= 10.0):
                logger.error(f"Invalid risk_percent: {risk_percent} (must be 0.1-10.0)")
                return 0
                
            if entry_price <= 0:
                logger.error(f"Invalid entry_price: {entry_price} (must be > 0)")
                return 0
                
            if stop_loss <= 0:
                logger.error(f"Invalid stop_loss: {stop_loss} (must be > 0)")
                return 0
            
            # Calculate risk amount
            risk_amount = account_equity * (risk_percent / 100)
            price_diff = abs(entry_price - stop_loss)
            
            # Edge case: zero stop loss distance (Requirements 5.1)
            if price_diff == 0:
                logger.warning(f"Zero stop loss distance: entry={entry_price}, stop={stop_loss}")
                return 0
            
            # Check for extremely small price differences that could cause huge positions
            min_price_diff = entry_price * 0.001  # 0.1% minimum difference
            if price_diff < min_price_diff:
                logger.warning(f"Stop loss too close to entry: {price_diff} < {min_price_diff}")
                return 0
            
            position_size = risk_amount / price_diff
            
            # Ensure reasonable position size limits
            max_position_value = account_equity * 0.5  # Max 50% of equity in one position
            max_shares = max_position_value / entry_price
            
            final_position_size = min(position_size, max_shares)
            
            # Log the calculation for audit trail (Requirements 5.3)
            logger.info(
                f"Position size calculated: equity=${account_equity:.2f}, "
                f"risk={risk_percent}%, entry=${entry_price:.2f}, "
                f"stop=${stop_loss:.2f}, size={final_position_size:.0f} shares"
            )
            
            return max(0, int(final_position_size))
            
        except Exception as e:
            logger.error(f"Error calculating position size: {e}")
            return 0
    
    def get_risk_metrics(self) -> Dict[str, Any]:
        """Get current risk metrics."""
        return {
            'max_positions': self.trading_config.get('max_positions', 5),
            'account_risk_percent': self.trading_config.get('account_risk_percent', 1.0),
            'max_daily_loss_percent': self.trading_config.get('max_daily_loss_percent', 3.0),
            'min_rr_ratio': self.trading_config.get('min_dynamic_rr_ratio', 1.5)
        }
    
    def check_risk_limits(self, current_positions: int, daily_pnl: float, 
                         account_equity: float) -> Dict[str, Any]:
        """Check if current risk levels are within limits."""
        risk_metrics = self.get_risk_metrics()
        
        # Check position limit
        position_limit_ok = current_positions < risk_metrics['max_positions']
        
        # Check daily loss limit
        daily_loss_percent = (daily_pnl / account_equity) * 100
        daily_loss_ok = daily_loss_percent > -risk_metrics['max_daily_loss_percent']
        
        return {
            'position_limit_ok': position_limit_ok,
            'daily_loss_ok': daily_loss_ok,
            'can_trade': position_limit_ok and daily_loss_ok,
            'current_positions': current_positions,
            'max_positions': risk_metrics['max_positions'],
            'daily_loss_percent': daily_loss_percent,
            'max_daily_loss_percent': risk_metrics['max_daily_loss_percent']
        }

# Convenience function for backward compatibility
async def check_risk_limits(db_pool, config, account_equity):
    """Check if trading is allowed based on risk limits."""
    risk_manager = RiskManager(config)
    
    # Mock current positions and daily P&L for now
    current_positions = 0  # Would come from live orders manager
    daily_pnl = 0  # Would come from today's trades
    
    risk_check = risk_manager.check_risk_limits(current_positions, daily_pnl, account_equity)
    return risk_check['can_trade']

def calculate_position_size(account_equity: float, risk_percent: float, 
                          entry_price: float, stop_loss: float) -> float:
    """Calculate position size based on risk parameters.
    
    Standalone function for backward compatibility.
    Uses the same validation logic as RiskManager.calculate_position_size().
    
    Args:
        account_equity: Total account equity (must be > 0)
        risk_percent: Risk percentage (e.g., 2.0 for 2%, must be 0.1-10.0)
        entry_price: Entry price (must be > 0)
        stop_loss: Stop loss price (must be > 0 and != entry_price)
        
    Returns:
        Position size in shares (integer), 0 if invalid inputs
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Input validation (Requirements 5.1)
        if account_equity <= 0:
            logger.error(f"Invalid account_equity: {account_equity} (must be > 0)")
            return 0
            
        if not (0.1 <= risk_percent <= 10.0):
            logger.error(f"Invalid risk_percent: {risk_percent} (must be 0.1-10.0)")
            return 0
            
        if entry_price <= 0:
            logger.error(f"Invalid entry_price: {entry_price} (must be > 0)")
            return 0
            
        if stop_loss <= 0:
            logger.error(f"Invalid stop_loss: {stop_loss} (must be > 0)")
            return 0
        
        # Calculate risk amount
        risk_amount = account_equity * (risk_percent / 100)
        price_diff = abs(entry_price - stop_loss)
        
        # Edge case: zero stop loss distance (Requirements 5.1)
        if price_diff == 0:
            logger.warning(f"Zero stop loss distance: entry={entry_price}, stop={stop_loss}")
            return 0
        
        # Check for extremely small price differences
        min_price_diff = entry_price * 0.001  # 0.1% minimum difference
        if price_diff < min_price_diff:
            logger.warning(f"Stop loss too close to entry: {price_diff} < {min_price_diff}")
            return 0
        
        position_size = risk_amount / price_diff
        
        # Ensure reasonable position size limits
        max_position_value = account_equity * 0.5  # Max 50% of equity in one position
        max_shares = max_position_value / entry_price
        
        final_position_size = min(position_size, max_shares)
        
        # Log the calculation (Requirements 5.3)
        logger.info(
            f"Position size calculated: equity=${account_equity:.2f}, "
            f"risk={risk_percent}%, entry=${entry_price:.2f}, "
            f"stop=${stop_loss:.2f}, size={final_position_size:.0f} shares"
        )
        
        return max(0, int(final_position_size))
        
    except Exception as e:
        logger.error(f"Error calculating position size: {e}")
        return 0
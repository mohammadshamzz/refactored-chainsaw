"""
Simple notification manager for the trading platform.
Handles system notifications, alerts, and messages.
"""
import logging
from datetime import datetime
from typing import List, Dict
from collections import deque

class NotificationManager:
    """Manages system notifications and alerts."""
    
    def __init__(self, max_notifications: int = 100):
        self.notifications = deque(maxlen=max_notifications)
        self.unread_count = 0
    
    def add_notification(self, title: str, message: str, level: str = "INFO"):
        """Add a new notification."""
        notification = {
            'id': len(self.notifications) + 1,
            'title': title,
            'message': message,
            'level': level,
            'timestamp': datetime.now(),
            'read': False
        }
        
        self.notifications.append(notification)
        self.unread_count += 1
        
        # Log the notification
        log_level = getattr(logging, level.upper(), logging.INFO)
        logging.log(log_level, f"Notification: {title} - {message}")
    
    def get_notifications(self, limit: int = 10) -> List[Dict]:
        """Get recent notifications."""
        return list(self.notifications)[-limit:]
    
    def get_unread_count(self) -> int:
        """Get count of unread notifications."""
        return self.unread_count
    
    def mark_all_read(self):
        """Mark all notifications as read."""
        for notification in self.notifications:
            notification['read'] = True
        self.unread_count = 0
    
    def clear_notifications(self):
        """Clear all notifications."""
        self.notifications.clear()
        self.unread_count = 0
    
    # Convenience methods for different notification types
    def notify_trade_executed(self, symbol: str, side: str, quantity: float, price: float):
        """Notify about trade execution."""
        self.add_notification(
            "Trade Executed",
            f"{side.upper()} {quantity} {symbol} @ ${price:.2f}",
            "SUCCESS"
        )
    
    def notify_setup_found(self, symbol: str, direction: str):
        """Notify about new trading setup."""
        self.add_notification(
            "Setup Found",
            f"{symbol} {direction} setup added to watchlist",
            "INFO"
        )
    
    def notify_risk_alert(self, message: str):
        """Notify about risk management alerts."""
        self.add_notification(
            "Risk Alert",
            message,
            "WARNING"
        )
    
    def notify_system_error(self, error_type: str, message: str):
        """Notify about system errors."""
        self.add_notification(
            f"System Error: {error_type}",
            message,
            "ERROR"
        )
    
    def notify_agent_status(self, status: str):
        """Notify about agent status changes."""
        self.add_notification(
            "Agent Status",
            f"Trading agent is now {status}",
            "INFO"
        )

# Global notification manager instance
notification_manager = NotificationManager()
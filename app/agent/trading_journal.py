from datetime import datetime, timedelta
from typing import Dict, List
from .database import execute_db_query
import logging

class TradingJournal:
    def __init__(self, db_pool):
        self.db_pool = db_pool
    
    async def initialize(self):
        # Schema is created by the main dashboard startup script
        pass
    
    async def add_journal_entry(self, trade_id: int, symbol: str, setup_type: str, entry_reason: str, **kwargs) -> int:
        query = "INSERT INTO trading_journal (trade_id, entry_date, symbol, setup_type, entry_reason, confluence_score, pre_trade_plan) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id"
        params = (trade_id, datetime.now().date(), symbol, setup_type, entry_reason, kwargs.get('confluence_score'), kwargs.get('pre_trade_plan'))
        result = await execute_db_query(self.db_pool, query, params, fetch='one')
        return result['id'] if result else None
    
    async def get_journal_entries(self, days: int = 30) -> List[Dict]:
        query = "SELECT j.*, t.pnl FROM trading_journal j LEFT JOIN trades t ON j.trade_id = t.id WHERE j.entry_date >= %s ORDER BY j.entry_date DESC"
        entries = await execute_db_query(self.db_pool, query, (datetime.now().date() - timedelta(days=days),), fetch='all')
        return [dict(e) for e in entries] if entries else []
        
    async def get_setup_performance(self, days: int = 90) -> List[Dict]:
        query = "SELECT j.setup_type, COUNT(*) as total_trades, AVG(t.pnl) as avg_pnl, SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate FROM trading_journal j JOIN trades t ON j.trade_id = t.id WHERE j.entry_date >= %s AND t.pnl IS NOT NULL GROUP BY j.setup_type ORDER BY avg_pnl DESC"
        results = await execute_db_query(self.db_pool, query, (datetime.now().date() - timedelta(days=days),), fetch='all')
        return [dict(r) for r in results] if results else []

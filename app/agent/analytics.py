import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any
from .database import execute_db_query

async def get_symbol_performance(db_pool, symbol: str, days: int = 30) -> Dict[str, Any]:
    # This function provides a detailed performance breakdown for a single symbol.
    try:
        query = """
        SELECT COUNT(*) as total, SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
               SUM(pnl) as total_pnl, AVG(pnl) as avg_pnl, MAX(pnl) as max_win, MIN(pnl) as max_loss,
               SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as gross_profit,
               SUM(CASE WHEN pnl < 0 THEN ABS(pnl) ELSE 0 END) as gross_loss
        FROM trades WHERE instrument = %s AND entry_time >= %s AND status = 'Closed' AND pnl IS NOT NULL
        """
        result = await execute_db_query(db_pool, query, (symbol, datetime.now() - timedelta(days=days)), fetch='one')
        if not result or result['total'] == 0: return {'symbol': symbol, 'total_trades': 0, 'win_rate': 0.0, 'total_pnl': 0.0}
        
        # Calculate derived metrics
        win_rate = (result['wins'] / result['total'] * 100) if result['total'] > 0 else 0
        profit_factor = (result['gross_profit'] / result['gross_loss']) if result['gross_loss'] > 0 else float('inf')
        return {'symbol': symbol, 'total_trades': result['total'], 'win_rate': win_rate, 'total_pnl': result['total_pnl'], 'profit_factor': profit_factor}
    except Exception as e:
        logging.error(f"Error getting performance for {symbol}: {e}")
        return {}

async def calculate_portfolio_metrics(db_pool, days: int = 30) -> Dict[str, Any]:
    # This function calculates key performance indicators for the entire portfolio.
    try:
        query = """
        SELECT COUNT(*) as total, SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
               SUM(pnl) as total_return, AVG(pnl) as avg_return,
               SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as gross_profit,
               SUM(CASE WHEN pnl < 0 THEN ABS(pnl) ELSE 0 END) as gross_loss,
               STDDEV(pnl) as pnl_std_dev FROM trades
        WHERE entry_time >= %s AND status = 'Closed' AND pnl IS NOT NULL
        """
        result = await execute_db_query(db_pool, query, (datetime.now() - timedelta(days=days),), fetch='one')
        if not result or result['total'] == 0: return {'total_trades': 0, 'win_rate': 0.0, 'total_return': 0.0, 'profit_factor': 0.0}

        win_rate = (result['wins'] / result['total'] * 100) if result['total'] > 0 else 0
        profit_factor = (result['gross_profit'] / result['gross_loss']) if result['gross_loss'] > 0 else float('inf')
        return {'total_trades': int(result['total']), 'win_rate': win_rate, 'total_return': float(result['total_return']), 'profit_factor': profit_factor}
    except Exception as e:
        logging.error(f"Error calculating portfolio metrics: {e}")
        return {}

async def get_performance_summary(db_pool, days: int = 30) -> Dict[str, Any]:
    # A wrapper function to get a full performance summary.
    portfolio_metrics = await calculate_portfolio_metrics(db_pool, days)
    return {'portfolio_metrics': portfolio_metrics}

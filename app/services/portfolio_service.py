from app.agent.agent_manager import agent_manager
from agent.risk_manager import RiskManager
from dataclasses import asdict

def get_positions():
    agent_info = agent_manager.get_agent_info()
    return agent_info.get('open_positions', [])

def get_orders():
    live_orders_manager = agent_manager.get_live_orders_manager()
    if live_orders_manager:
        return [asdict(order) for order in live_orders_manager.orders.values()]
    return []

def get_risk_info(config):
    risk_manager = RiskManager(config)
    agent_info = agent_manager.get_agent_info()
    
    account_info = agent_info.get('account_info', {})
    account_equity = float(account_info.get('equity', 0))
    
    # This is a simplification. In a real system, you would calculate the daily PnL.
    daily_pnl = 0 

    current_positions = len(agent_info.get('open_positions', []))

    risk_limits = risk_manager.check_risk_limits(current_positions, daily_pnl, account_equity)
    risk_metrics = risk_manager.get_risk_metrics()

    return {**risk_limits, **risk_metrics}

def get_watchlist():
    agent_info = agent_manager.get_agent_info()
    print(agent_info)
    return agent_info.get('watchlist', {})

import threading
import queue
import asyncio
import logging
from app.agent.agent import TradingAgent
from app.agent.broker_factory import get_broker
from app.agent.live_orders_manager import LiveOrdersManager

class AgentManager:
    """
    A singleton class to manage the lifecycle of the TradingAgent thread.
    This class is designed to be called from a synchronous environment like Streamlit.
    """
    _instance = None
    
    def __init__(self):
        if AgentManager._instance is not None:
            raise Exception("This class is a singleton!")
        self.agent_thread = None
        self.shared_state = {
            "agent_status": "Stopped", "last_update": None, "account_info": {},
            "open_positions": {}, "htf_bias": {}, "config": {}
        }
        self.lock = threading.Lock()
        AgentManager._instance = self

    @staticmethod
    def get_instance():
        if AgentManager._instance is None:
            AgentManager()
        return AgentManager._instance

    def start_agent(self, db_pool, config):
        """
        Creates and starts the TradingAgent in a new background thread.
        This method is synchronous and safe to call from the main UI thread.
        """
        if self.agent_thread and self.agent_thread.is_alive():
            return "Agent is already running."
        try:
            with self.lock:
                self.shared_state['config'] = config
            
            broker = get_broker(config)
            
            # Create the orders manager, but do not initialize it here.
            # All async setup will be handled by the agent's own thread.
            orders_manager = LiveOrdersManager(broker, db_pool)
            
            # Create the agent instance, passing all necessary components.
            self.agent_thread = TradingAgent(
                self.shared_state,
                self.lock,
                db_pool,
                broker,
                orders_manager
            )
            
            # Start the background thread. The agent's `run` method will be executed.
            self.agent_thread.start()
            
            with self.lock:
                self.shared_state["agent_status"] = "Running"
            return "Agent started successfully!"
        except Exception as e:
            # Catch any exceptions during the synchronous setup part
            logging.error(f"Failed to start agent thread: {e}", exc_info=True)
            return f"Failed to start agent: {e}"

    def stop_agent(self):
        """Signals the agent thread to stop and waits for it to terminate."""
        if self.agent_thread and self.agent_thread.is_alive():
            # The agent's stop() method is synchronous and just sets an event flag.
            self.agent_thread.stop()
            self.agent_thread.join(timeout=10) # Wait for the thread to finish
            
            if self.agent_thread.is_alive():
                 return "Agent failed to stop gracefully."
            
            self.agent_thread = None
            with self.lock:
                self.shared_state["agent_status"] = "Stopped"
            return "Agent stopped successfully."
        return "Agent is not running."

    def get_status(self):
        with self.lock:
            return self.shared_state["agent_status"]

    def is_running(self):
        return self.agent_thread and self.agent_thread.is_alive()
    
    def get_agent_info(self):
        """Get comprehensive agent information including account, positions, and watchlist."""
        with self.lock:
            open_positions_data = self.shared_state.get('open_positions', {})
            # Convert positions dict to list format for dashboard
            positions_list = []
            if isinstance(open_positions_data, dict):
                for symbol, position in open_positions_data.items():
                    if isinstance(position, dict):
                        positions_list.append({
                            'symbol': symbol,
                            'side': position.get('side', 'N/A'),
                            'qty': position.get('qty', 0),
                            'avg_entry_price': position.get('avg_entry_price', 0),
                            'market_value': position.get('market_value', 0),
                            'unrealized_pnl': position.get('unrealized_pnl', 0)
                        })
            
            return {
                'status': self.shared_state.get('agent_status', 'Unknown'),
                'last_update': self.shared_state.get('last_update'),
                'account_info': self.shared_state.get('account_info', {}),
                'open_positions': positions_list,
                'open_positions_count': len(open_positions_data),
                'watchlist': self.shared_state.get('watchlist', {}),
                'htf_bias': self.shared_state.get('htf_bias', {})
            }
    
    def get_live_orders_manager(self):
        if self.agent_thread and isinstance(self.agent_thread, TradingAgent):
            return self.agent_thread.live_orders_manager
        return None

    def restart_agent(self, db_pool, config):
        """Restart the agent by stopping and starting it."""
        self.stop_agent()
        return self.start_agent(db_pool, config)

# Global singleton instance for easy access from the UI
agent_manager = AgentManager.get_instance()



from app.agent.agent_manager import agent_manager;

def get_agent_status():
    return agent_manager.get_status()

def start_agent(db_pool, config):
    return agent_manager.start_agent(db_pool, config)

def stop_agent():
    return agent_manager.stop_agent()

def restart_agent(db_pool, config):
    return agent_manager.restart_agent(db_pool, config)

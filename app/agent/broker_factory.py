import logging
from .alpaca_broker import AlpacaBroker
from .broker_yfinance import YFinanceBroker

def get_broker(config: dict):
    """Factory function to get the appropriate broker instance based on config."""
    provider = config.get('data_provider', 'alpaca').lower()
    
    logging.info(f"Selected data provider: {provider}")
    
    if provider == 'alpaca':
        logging.info("Using Alpaca as the data and trading provider.")
        return AlpacaBroker(config)
    elif provider == 'yfinance':
        logging.info("Using Yahoo Finance as the data provider.")
        return YFinanceBroker(config)
    else:
        raise ValueError(f"Unsupported data provider: {provider}")

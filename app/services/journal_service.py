
from agent.trading_journal import TradingJournal

async def get_journal_entries(db_pool, days: int = 30):
    journal = TradingJournal(db_pool)
    return await journal.get_journal_entries(days)

async def get_setup_performance(db_pool, days: int = 90):
    journal = TradingJournal(db_pool)
    return await journal.get_setup_performance(days)


from agent.llm_market_analysis import LLMMarketAnalyzer

async def get_ai_sentiment(db_pool, config, symbols: list):
    llm_analyzer = LLMMarketAnalyzer(db_pool, config)
    return await llm_analyzer.analyze_portfolio_sentiment(symbols)

async def get_trade_signals(db_pool, config, symbols: list):
    llm_analyzer = LLMMarketAnalyzer(db_pool, config)
    return await llm_analyzer.get_trade_signals(symbols)

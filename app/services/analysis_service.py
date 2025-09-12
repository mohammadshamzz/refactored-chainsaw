
from agent.advanced_charting import AdvancedChartingEngine

async def get_ict_chart(db_pool, symbol: str, timeframe: str = '1d', days: int = 30):
    charting_engine = AdvancedChartingEngine(db_pool)
    fig = await charting_engine.create_ict_chart(symbol, timeframe, days)
    return fig.to_json()

async def get_multi_timeframe_chart(db_pool, symbol: str):
    charting_engine = AdvancedChartingEngine(db_pool)
    fig = await charting_engine.create_multi_timeframe_chart(symbol)
    return fig.to_json()

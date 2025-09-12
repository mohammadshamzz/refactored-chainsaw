from agent.analytics import get_performance_summary
from agent.advanced_charting import AdvancedChartingEngine

async def get_overview(db_pool, days: int = 30):
    return await get_performance_summary(db_pool, days)

def get_equity_curve(db_pool):
    charting_engine = AdvancedChartingEngine(db_pool)
    fig = charting_engine.create_equity_curve()
    return fig.to_json()

from fastapi import APIRouter, Request
from app.services.analysis_service import get_ict_chart, get_multi_timeframe_chart

router = APIRouter()

@router.get("/charts/{symbol}")
async def get_chart(symbol: str, request: Request, timeframe: str = '1d', chart_type: str = 'ict'):
    if chart_type == 'ict':
        return await get_ict_chart(request.app.state.db_pool, symbol, timeframe)
    elif chart_type == 'multi_timeframe':
        return await get_multi_timeframe_chart(request.app.state.db_pool, symbol)
    else:
        return {"message": "Invalid chart type"}

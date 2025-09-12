
from fastapi import APIRouter, Request
from app.services.ai_service import get_ai_sentiment, get_trade_signals

router = APIRouter()

@router.post("/ai-sentiment")
async def ai_sentiment(request: Request):
    symbols = request.app.state.config['trading']['instruments']
    return await get_ai_sentiment(request.app.state.db_pool, request.app.state.config, symbols)

@router.post("/trade-signals")
async def trade_signals(request: Request):
    symbols = request.app.state.config['trading']['instruments']
    return await get_trade_signals(request.app.state.db_pool, request.app.state.config, symbols)

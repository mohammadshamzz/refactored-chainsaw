from fastapi import APIRouter, Request
from app.services.dashboard_service import get_overview, get_equity_curve

router = APIRouter()

@router.get("/overview")
async def overview(request: Request, days: int = 30):
    return await get_overview(request.app.state.db_pool, days)

@router.get("/equity")
def equity(request: Request):
    return get_equity_curve(request.app.state.db_pool)
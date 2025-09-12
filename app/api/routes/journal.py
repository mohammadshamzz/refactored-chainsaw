
from fastapi import APIRouter, Request
from app.services.journal_service import get_journal_entries, get_setup_performance

router = APIRouter()

@router.get("/entries")
async def journal_entries(request: Request, days: int = 30):
    return await get_journal_entries(request.app.state.db_pool, days)

@router.get("/performance")
async def setup_performance(request: Request, days: int = 90):
    return await get_setup_performance(request.app.state.db_pool, days)

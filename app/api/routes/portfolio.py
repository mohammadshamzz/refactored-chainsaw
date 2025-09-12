from fastapi import APIRouter, Request
from app.services.portfolio_service import get_positions, get_orders, get_risk_info, get_watchlist

router = APIRouter()

@router.get("/positions")
def positions():
    return get_positions()

@router.get("/orders")
def orders():
    return get_orders()

@router.get("/risk")
def risk(request: Request):
    return get_risk_info(request.app.state.config)

@router.get("/watchlist")
def watchlist():
    return get_watchlist()
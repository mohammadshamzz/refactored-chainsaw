
from fastapi import APIRouter, Query
from app.services.log_service import get_recent_logs_tail, get_recent_logs_by_hours

router = APIRouter()

@router.get("/recent")
def recent_logs(log_type: str = Query("main", description="Type of log: main, error, recent_activity"), lines: int = 100):
    if log_type == "main":
        return get_recent_logs_tail("logs/trading_agent.log", lines)
    elif log_type == "error":
        return get_recent_logs_tail("logs/trading_errors.log", lines)
    elif log_type == "recent_activity":
        return get_recent_logs_by_hours(hours=1) # Default to 1 hour for recent activity
    else:
        return []


from fastapi import APIRouter, HTTPException, Request
from app.services.agent_service import get_agent_status, start_agent, stop_agent, restart_agent

router = APIRouter()

@router.get("/status")
def agent_status():
    return {"status": get_agent_status()}

@router.post("/start")
def start(request: Request):
    result = start_agent(request.app.state.db_pool, request.app.state.config)
    if "failed" in result.lower():
        raise HTTPException(status_code=400, detail=result)
    return {"message": result}

@router.post("/stop")
def stop():
    result = stop_agent()
    if "failed" in result.lower():
        raise HTTPException(status_code=400, detail=result)
    return {"message": result}

@router.post("/restart")
def restart(request: Request):
    result = restart_agent(request.app.state.db_pool, request.app.state.config)
    if "failed" in result.lower():
        raise HTTPException(status_code=400, detail=result)
    return {"message": result}

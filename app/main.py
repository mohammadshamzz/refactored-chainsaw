import os
import sys

import asyncio
import yaml
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import (
    agent as agent_router,
    dashboard as dashboard_router,
    portfolio as portfolio_router,
    analysis as analysis_router,
    instruments as instruments_router,
    ai as ai_router,
    journal as journal_router,
    logs as logs_router,
)
from app.agent.database import get_db_connection_pool

app = FastAPI()

@app.on_event("startup")
def startup_event():
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'live_config.yaml'))
    app.state.config = yaml.safe_load(open(config_path, 'r'))
    app.state.db_pool = get_db_connection_pool(app.state.config)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Basic Error Handling
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"message": f"An unexpected error occurred: {exc}"},
    )

app.include_router(agent_router.router, prefix="/api/agent", tags=["agent"])
app.include_router(dashboard_router.router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(portfolio_router.router, prefix="/api/portfolio", tags=["portfolio"])
app.include_router(analysis_router.router, prefix="/api/analysis", tags=["analysis"])
app.include_router(instruments_router.router, prefix="/api/instruments", tags=["instruments"])
app.include_router(ai_router.router, prefix="/api/ai", tags=["ai"])
app.include_router(journal_router.router, prefix="/api/journal", tags=["journal"])
app.include_router(logs_router.router, prefix="/api/logs", tags=["logs"])

log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'trading_agent.log'))

# Health Check Endpoint
@app.get("/health")
async def health_check():
    return {"status": "ok"}

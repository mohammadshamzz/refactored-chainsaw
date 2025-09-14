import os
import sys
import asyncio
import yaml
import logging
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
from app.agent.database_schema import setup_database_tables # 1. Import the new function

# Get the logger
logger = logging.getLogger(__name__)

app = FastAPI()

@app.on_event("startup")
async def startup_event(): # 2. Make the function async
    """
    Handles all application startup logic:
    1. Load configuration.
    2. Connect to the database.
    3. Ensure all database tables are created.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, 'live_config.yaml')
    
    try:
        with open(config_path, 'r') as f:
            app.state.config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.critical(f"FATAL: Configuration file not found at {config_path}")
        raise RuntimeError(f"live_config.yaml not found at {config_path}")
        
    app.state.db_pool = get_db_connection_pool(app.state.config)
    
    # 3. Call the setup function right after the pool is created
    await setup_database_tables(app.state.db_pool)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Secure Global Error Handling
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception for request {request.url.path}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"message": "An internal server error occurred."},
    )

# Include API routers
app.include_router(agent_router.router, prefix="/api/agent", tags=["agent"])
app.include_router(dashboard_router.router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(portfolio_router.router, prefix="/api/portfolio", tags=["portfolio"])
app.include_router(analysis_router.router, prefix="/api/analysis", tags=["analysis"])
app.include_router(instruments_router.router, prefix="/api/instruments", tags=["instruments"])
app.include_router(ai_router.router, prefix="/api/ai", tags=["ai"])
app.include_router(journal_router.router, prefix="/api/journal", tags=["journal"])
app.include_router(logs_router.router, prefix="/api/logs", tags=["logs"])

# Health Check Endpoint
@app.get("/health")
async def health_check():
    return {"status": "ok"}


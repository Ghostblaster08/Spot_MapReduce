import asyncio
import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from orchestrator.core.database import init_db, DB_PATH
from orchestrator.core.lease_monitor import lease_monitor_loop
from orchestrator.core.barrier_monitor import barrier_monitor_loop
from orchestrator.api.jobs import router as jobs_router
from orchestrator.api.tasks import router as tasks_router
from orchestrator.api.heartbeat import router as heartbeat_router

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("orchestrator.main")

background_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting up MapReduce Orchestrator...")
    await init_db()
    lease_task = asyncio.create_task(lease_monitor_loop(), name="lease_monitor")
    barrier_task = asyncio.create_task(barrier_monitor_loop(), name="barrier_monitor")
    background_tasks.extend([lease_task, barrier_task])
    log.info("Background monitor tasks initiated.")
    try:
        yield
    finally:
        log.info("Shutting down MapReduce Orchestrator...")
        for t in background_tasks:
            t.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
        log.info("Shutdown complete.")

app = FastAPI(
    title="Transient Spot-Instance MapReduce Orchestrator",
    description="Single-node master managing task scheduling, heartbeat leases, checkpoints, and stage transitions",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(jobs_router)
app.include_router(tasks_router)
app.include_router(heartbeat_router)

@app.get("/health", tags=["System"])
async def health_check():
    return {
        "status": "healthy",
        "service": "orchestrator",
        "db_path": DB_PATH
    }

@app.get("/metrics", tags=["System"])
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

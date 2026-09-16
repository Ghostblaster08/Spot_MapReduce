import os
import logging
from fastapi import FastAPI
from rss.api.partitions import router as partitions_router
from rss.api.health import router as health_router

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [rss] %(levelname)s: %(message)s"
)
log = logging.getLogger("rss.main")

app = FastAPI(
    title="Remote Shuffle Service (RSS)",
    description="Decoupled shuffle storage service accepting map partition chunks and streaming to reducers",
    version="1.0.0"
)

app.include_router(partitions_router)
app.include_router(health_router)

log.info("RSS Service ready.")

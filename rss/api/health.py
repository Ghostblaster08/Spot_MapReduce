import os
from fastapi import APIRouter, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from rss.core.storage import RSS_DATA_PATH

router = APIRouter(tags=["Health"])

@router.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "rss",
        "data_path": RSS_DATA_PATH
    }

@router.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

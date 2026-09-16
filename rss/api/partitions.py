from fastapi import APIRouter, Request, Query, HTTPException, status
from fastapi.responses import StreamingResponse
from rss.core.storage import write_chunk, stream_partition, get_job_summary, delete_job
import logging

router = APIRouter(prefix="/partitions", tags=["Partitions"])
log = logging.getLogger("rss.api.partitions")

@router.post("/{job_id}/{partition_id}")
async def push_chunk(
    job_id: str,
    partition_id: int,
    request: Request,
    task_id: str = Query(..., description="Source task ID"),
    attempt_id: int = Query(..., description="Task attempt ID"),
    chunk_seq: int = Query(..., description="Chunk sequence number")
):
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty chunk body")

    result = await write_chunk(
        job_id=job_id,
        partition_id=partition_id,
        task_id=task_id,
        attempt_id=attempt_id,
        chunk_seq=chunk_seq,
        data=body
    )
    return result

@router.get("/{job_id}/{partition_id}/stream")
async def stream_partition_data(job_id: str, partition_id: int):
    return StreamingResponse(
        stream_partition(job_id, partition_id),
        media_type="application/x-ndjson"
    )

@router.get("/{job_id}/manifest")
async def get_manifest(job_id: str):
    return get_job_summary(job_id)

@router.delete("/{job_id}")
async def clear_job(job_id: str):
    return delete_job(job_id)

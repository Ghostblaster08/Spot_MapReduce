from fastapi import APIRouter, HTTPException, status
from orchestrator.models.job import JobSubmitRequest, JobResponse, JobStatusResponse
from orchestrator.core.scheduler import create_job_and_map_tasks, utcnow_iso
from orchestrator.core.database import get_db
import logging

router = APIRouter(prefix="/jobs", tags=["Jobs"])
log = logging.getLogger("orchestrator.api.jobs")

@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=JobResponse)
async def submit_job(req: JobSubmitRequest):
    try:
        job_id = await create_job_and_map_tasks(
            name=req.name,
            map_fn=req.map_fn,
            reduce_fn=req.reduce_fn,
            input_path=req.input_path,
            output_path=req.output_path,
            num_mappers=req.num_mappers,
            num_reducers=req.num_reducers
        )
        return JobResponse(
            job_id=job_id,
            name=req.name,
            state="MAP_RUNNING",
            num_mappers=req.num_mappers,
            num_reducers=req.num_reducers,
            submitted_at=utcnow_iso()
        )
    except Exception as e:
        log.error(f"Failed to submit job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    db = await get_db()
    try:
        cursor = await db.execute("""
        SELECT job_id, name, state, submitted_at, started_at, completed_at, error_message
        FROM jobs WHERE job_id = ?
        """, (job_id,))
        job = await cursor.fetchone()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        # Query task counts
        t_cur = await db.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN state = 'RUNNING' THEN 1 ELSE 0 END) as running,
            SUM(CASE WHEN state = 'QUEUED' THEN 1 ELSE 0 END) as queued,
            SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failed
        FROM tasks WHERE job_id = ?
        """, (job_id,))
        counts = await t_cur.fetchone()

        return JobStatusResponse(
            job_id=job["job_id"],
            name=job["name"],
            state=job["state"],
            tasks_total=counts["total"] or 0,
            tasks_completed=counts["completed"] or 0,
            tasks_running=counts["running"] or 0,
            tasks_queued=counts["queued"] or 0,
            tasks_failed=counts["failed"] or 0,
            submitted_at=job["submitted_at"],
            started_at=job["started_at"],
            completed_at=job["completed_at"],
            error_message=job["error_message"]
        )
    finally:
        await db.close()

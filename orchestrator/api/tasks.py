import uuid
import json
import logging
from fastapi import APIRouter, HTTPException, Query, Response, status
from orchestrator.models.task import (
    TaskResponse,
    CheckpointRequest,
    TaskCompleteRequest,
    TaskDrainRequest,
    TaskFailRequest
)
from orchestrator.core.scheduler import (
    poll_task,
    mark_task_complete,
    drain_and_requeue_task,
    fail_task,
    utcnow_iso
)
from orchestrator.core.database import get_db
from orchestrator.metrics import MAPREDUCE_CHECKPOINT_COMMITS_TOTAL

router = APIRouter(prefix="/tasks", tags=["Tasks"])
log = logging.getLogger("orchestrator.api.tasks")

@router.get("/poll", response_model=TaskResponse)
async def poll_for_task(worker_id: str = Query(..., description="Unique worker identifier")):
    task = await poll_task(worker_id)
    if not task:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return task

@router.post("/{task_id}/checkpoint")
async def record_checkpoint(task_id: str, req: CheckpointRequest):
    db = await get_db()
    try:
        # Check task existence and validate fencing token (attempt_id)
        cursor = await db.execute("SELECT attempt_id, state FROM tasks WHERE task_id = ?", (task_id,))
        row = await cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Task not found")

        current_attempt = row["attempt_id"]
        if req.attempt_id != current_attempt:
            log.warning(f"Fenced stale checkpoint commit for task {task_id}: req_attempt={req.attempt_id} vs current={current_attempt}")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"FENCED: task {task_id} current attempt is {current_attempt}"
            )

        now = utcnow_iso()
        ckpt_id = f"ckpt-{uuid.uuid4().hex[:12]}"
        manifest_json = json.dumps(req.partition_manifest) if req.partition_manifest else None

        await db.execute("""
        INSERT INTO checkpoints (
            checkpoint_id, task_id, job_id, attempt_id, byte_offset,
            records_processed, partition_manifest, is_final_drain, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ckpt_id, task_id, req.job_id, req.attempt_id, req.byte_offset,
              req.records_processed, manifest_json, req.is_final_drain, now))

        await db.execute("""
        UPDATE tasks
        SET last_checkpoint_offset = ?,
            last_checkpoint_seq = ?,
            last_checkpoint_at = ?
        WHERE task_id = ?
        """, (req.byte_offset, req.records_processed, now, task_id))

        await db.commit()
        MAPREDUCE_CHECKPOINT_COMMITS_TOTAL.labels(is_final_drain=str(req.is_final_drain)).inc()
        log.info(f"Checkpoint {ckpt_id} committed for {task_id} (offset={req.byte_offset}, records={req.records_processed})")

        return {"checkpoint_id": ckpt_id, "accepted": True}
    finally:
        await db.close()

@router.post("/{task_id}/complete")
async def complete_task(task_id: str, req: TaskCompleteRequest):
    success = await mark_task_complete(
        task_id=task_id,
        attempt_id=req.attempt_id,
        final_byte_offset=req.final_byte_offset,
        final_records_processed=req.final_records_processed
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Failed to complete task {task_id}: attempt_id mismatch or not in RUNNING state"
        )
    return {"status": "ACCEPTED", "task_state": "COMPLETED"}

@router.post("/{task_id}/drain")
async def drain_task(task_id: str, req: TaskDrainRequest):
    success = await drain_and_requeue_task(
        task_id=task_id,
        attempt_id=req.attempt_id,
        final_byte_offset=req.final_byte_offset,
        final_records_processed=req.final_records_processed
    )
    if not success:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"status": "ACKNOWLEDGED", "task_will_be_requeued": True}

@router.post("/{task_id}/fail")
async def report_task_failure(task_id: str, req: TaskFailRequest):
    will_retry, new_attempt = await fail_task(
        task_id=task_id,
        attempt_id=req.attempt_id,
        error_message=req.error_message
    )
    return {
        "status": "ACKNOWLEDGED",
        "will_retry": will_retry,
        "new_attempt_id": new_attempt
    }

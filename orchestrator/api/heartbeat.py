import os
import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, HTTPException, status
from orchestrator.models.task import HeartbeatRequest, HeartbeatResponse
from orchestrator.core.database import get_db

router = APIRouter(tags=["Heartbeat"])
log = logging.getLogger("orchestrator.api.heartbeat")

HEARTBEAT_TIMEOUT_SECONDS = int(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "5"))

@router.post("/heartbeat", response_model=HeartbeatResponse)
async def process_heartbeat(req: HeartbeatRequest):
    db = await get_db()
    try:
        cursor = await db.execute("""
        SELECT state, attempt_id, assigned_worker
        FROM tasks
        WHERE task_id = ?
        """, (req.task_id,))
        row = await cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Task not found")

        # Zombie writer / stale attempt check
        if row["attempt_id"] != req.attempt_id:
            log.warning(
                f"[FENCED] Stale heartbeat from worker {req.worker_id} for task {req.task_id}: "
                f"req_attempt={req.attempt_id} vs db_attempt={row['attempt_id']}"
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"FENCED: task attempt is now {row['attempt_id']}"
            )

        if row["state"] != "RUNNING":
            return HeartbeatResponse(
                status="STOP",
                continue_task=False,
                message=f"Task is in state {row['state']}; worker should stop"
            )

        # Renew lease deadline
        new_deadline = (datetime.now(timezone.utc) + timedelta(seconds=HEARTBEAT_TIMEOUT_SECONDS)).isoformat()
        await db.execute("""
        UPDATE tasks
        SET lease_deadline = ?
        WHERE task_id = ? AND attempt_id = ?
        """, (new_deadline, req.task_id, req.attempt_id))
        await db.commit()

        return HeartbeatResponse(status="OK", continue_task=True)
    finally:
        await db.close()

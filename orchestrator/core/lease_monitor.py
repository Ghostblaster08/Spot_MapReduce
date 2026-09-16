import asyncio
import os
import logging
from datetime import datetime, timezone
from orchestrator.core.database import get_db
from orchestrator.metrics import (
    MAPREDUCE_HEARTBEAT_TIMEOUTS_TOTAL,
    MAPREDUCE_CRASHES_TOTAL,
    MAPREDUCE_TASKS_RUNNING,
    MAPREDUCE_TASKS_QUEUED
)

log = logging.getLogger("orchestrator.lease_monitor")

LEASE_CHECK_INTERVAL_SECONDS = float(os.environ.get("LEASE_CHECK_INTERVAL_SECONDS", "1.0"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))

async def lease_monitor_loop():
    """Continuously monitors tasks for worker heartbeat timeouts."""
    log.info("Starting task lease monitor loop...")
    while True:
        try:
            await check_expired_leases()
        except asyncio.CancelledError:
            log.info("Lease monitor loop stopped.")
            break
        except Exception as e:
            log.error(f"Error in lease monitor loop: {e}", exc_info=True)
        await asyncio.sleep(LEASE_CHECK_INTERVAL_SECONDS)

async def check_expired_leases():
    db = await get_db()
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        cursor = await db.execute("""
        SELECT task_id, job_id, task_type, attempt_id, assigned_worker, lease_deadline
        FROM tasks
        WHERE state = 'RUNNING' AND lease_deadline IS NOT NULL AND lease_deadline < ?
        """, (now_iso,))
        expired_tasks = await cursor.fetchall()

        for t in expired_tasks:
            task_id = t["task_id"]
            job_id = t["job_id"]
            task_type = t["task_type"]
            cur_attempt = t["attempt_id"]
            worker = t["assigned_worker"]
            new_attempt = cur_attempt + 1

            log.warning(
                f"[LEASE EXPIRED] Worker {worker} timed out on task {task_id} "
                f"(attempt {cur_attempt}, deadline {t['lease_deadline']})"
            )
            MAPREDUCE_HEARTBEAT_TIMEOUTS_TOTAL.inc()
            MAPREDUCE_CRASHES_TOTAL.inc()

            if new_attempt < MAX_ATTEMPTS:
                await db.execute("""
                UPDATE tasks
                SET state = 'QUEUED',
                    assigned_worker = NULL,
                    attempt_id = ?,
                    lease_deadline = NULL
                WHERE task_id = ? AND state = 'RUNNING'
                """, (new_attempt, task_id))
                MAPREDUCE_TASKS_RUNNING.labels(task_type=task_type).dec()
                MAPREDUCE_TASKS_QUEUED.labels(task_type=task_type).inc()
                log.info(f"Requeued task {task_id} with attempt {new_attempt}")
            else:
                await db.execute("""
                UPDATE tasks
                SET state = 'FAILED',
                    error_message = 'Heartbeat lease expired repeatedly',
                    completed_at = ?
                WHERE task_id = ?
                """, (now_iso, task_id))
                await db.execute("""
                UPDATE jobs
                SET state = 'FAILED',
                    error_message = ?,
                    completed_at = ?
                WHERE job_id = ?
                """, (f"Task {task_id} failed after lease timeouts", now_iso, job_id))
                MAPREDUCE_TASKS_RUNNING.labels(task_type=task_type).dec()
                log.error(f"Task {task_id} permanently failed after lease timeouts")

        if expired_tasks:
            await db.commit()
    finally:
        await db.close()

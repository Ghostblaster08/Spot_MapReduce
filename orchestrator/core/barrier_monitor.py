import asyncio
import os
import logging
from datetime import datetime, timezone
from orchestrator.core.database import get_db
from orchestrator.metrics import (
    MAPREDUCE_JOBS_TOTAL,
    MAPREDUCE_TASKS_QUEUED
)

log = logging.getLogger("orchestrator.barrier_monitor")

BARRIER_CHECK_INTERVAL_SECONDS = 1.0

async def barrier_monitor_loop():
    """Continuously monitors stage completion and triggers stage transitions."""
    log.info("Starting stage barrier monitor loop...")
    while True:
        try:
            await check_stage_barriers()
        except asyncio.CancelledError:
            log.info("Barrier monitor loop stopped.")
            break
        except Exception as e:
            log.error(f"Error in barrier monitor loop: {e}", exc_info=True)
        await asyncio.sleep(BARRIER_CHECK_INTERVAL_SECONDS)

async def check_stage_barriers():
    db = await get_db()
    try:
        now_iso = datetime.now(timezone.utc).isoformat()

        # ── 1. Check MAP stage completion ─────────────────────────────
        cursor = await db.execute("""
        SELECT j.job_id, j.num_reducers, j.num_mappers, j.output_path, j.reduce_fn
        FROM jobs j
        WHERE j.state = 'MAP_RUNNING'
        """)
        active_map_jobs = await cursor.fetchall()

        for j in active_map_jobs:
            job_id = j["job_id"]
            num_reducers = j["num_reducers"]

            # Count pending map tasks
            t_cur = await db.execute("""
            SELECT COUNT(*) as unfinished
            FROM tasks
            WHERE job_id = ? AND task_type = 'MAP' AND state != 'COMPLETED'
            """, (job_id,))
            res = await t_cur.fetchone()

            if res["unfinished"] == 0:
                log.info(f"All MAP tasks complete for job {job_id}. Transitioning to REDUCE_RUNNING...")

                # Update job state
                await db.execute("""
                UPDATE jobs SET state = 'REDUCE_RUNNING' WHERE job_id = ?
                """, (job_id,))

                # Generate Reduce tasks
                for p in range(num_reducers):
                    task_id = f"task-reduce-{job_id[:8]}-{p:02d}"
                    await db.execute("""
                    INSERT INTO tasks (
                        task_id, job_id, task_type, state, partition_id, attempt_id, created_at
                    ) VALUES (?, ?, 'REDUCE', 'QUEUED', ?, 0, ?)
                    """, (task_id, job_id, p, now_iso))

                await db.commit()
                MAPREDUCE_TASKS_QUEUED.labels(task_type="REDUCE").inc(num_reducers)
                log.info(f"Enqueued {num_reducers} REDUCE tasks for job {job_id}")

        # ── 2. Check REDUCE stage completion ──────────────────────────
        cursor = await db.execute("""
        SELECT j.job_id, j.output_path
        FROM jobs j
        WHERE j.state = 'REDUCE_RUNNING'
        """)
        active_reduce_jobs = await cursor.fetchall()

        for j in active_reduce_jobs:
            job_id = j["job_id"]
            output_path = j["output_path"]

            t_cur = await db.execute("""
            SELECT COUNT(*) as unfinished
            FROM tasks
            WHERE job_id = ? AND task_type = 'REDUCE' AND state != 'COMPLETED'
            """, (job_id,))
            res = await t_cur.fetchone()

            if res["unfinished"] == 0:
                log.info(f"All REDUCE tasks complete for job {job_id}. Marking COMPLETED...")
                await db.execute("""
                UPDATE jobs
                SET state = 'COMPLETED', completed_at = ?
                WHERE job_id = ?
                """, (now_iso, job_id))
                await db.commit()

                # Write _SUCCESS marker file
                try:
                    job_out_dir = os.path.join(output_path, job_id)
                    os.makedirs(job_out_dir, exist_ok=True)
                    success_file = os.path.join(job_out_dir, "_SUCCESS")
                    with open(success_file, "w") as f:
                        f.write(f"Job {job_id} finished successfully at {now_iso}\n")
                    log.info(f"Wrote completion marker: {success_file}")
                except Exception as e:
                    log.warning(f"Could not write _SUCCESS marker: {e}")

                MAPREDUCE_JOBS_TOTAL.labels(state="COMPLETED").inc()

    finally:
        await db.close()

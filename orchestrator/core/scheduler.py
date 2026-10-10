import os
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from orchestrator.core.database import get_db, emit
from orchestrator.models.task import TaskResponse
from orchestrator.metrics import (
    MAPREDUCE_TASKS_RUNNING,
    MAPREDUCE_TASKS_QUEUED,
    MAPREDUCE_TASKS_COMPLETED_TOTAL,
    MAPREDUCE_PREEMPTIONS_TOTAL
)

log = logging.getLogger("orchestrator.scheduler")

HEARTBEAT_TIMEOUT_SECONDS = int(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "5"))
RSS_URL = os.environ.get("RSS_URL", "http://rss:8001")
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def calculate_splits(file_path: str, num_mappers: int) -> List[tuple[int, int]]:
    """Calculates byte ranges for N mappers based on file size."""
    try:
        total_size = os.path.getsize(file_path)
    except OSError:
        log.warning(f"Could not stat {file_path}, assuming 10MB default size for splits")
        total_size = 10 * 1024 * 1024

    if total_size == 0 or num_mappers <= 1:
        return [(0, max(total_size, 0))]

    chunk_size = total_size // num_mappers
    splits = []
    for i in range(num_mappers):
        start = i * chunk_size
        end = total_size if i == num_mappers - 1 else (i + 1) * chunk_size
        splits.append((start, end))
    return splits

async def create_job_and_map_tasks(
    name: str,
    map_fn: str,
    reduce_fn: str,
    input_path: str,
    output_path: str,
    num_mappers: int,
    num_reducers: int
) -> str:
    job_id = f"job-{uuid.uuid4().hex[:12]}"
    now = utcnow_iso()
    splits = calculate_splits(input_path, num_mappers)

    db = await get_db()
    try:
        await db.execute("""
        INSERT INTO jobs (
            job_id, name, state, map_fn, reduce_fn, input_path, output_path,
            num_mappers, num_reducers, submitted_at, started_at
        ) VALUES (?, ?, 'MAP_RUNNING', ?, ?, ?, ?, ?, ?, ?, ?)
        """, (job_id, name, map_fn, reduce_fn, input_path, output_path,
              num_mappers, num_reducers, now, now))

        for idx, (s_start, s_end) in enumerate(splits):
            task_id = f"task-map-{job_id[:8]}-{idx:02d}"
            await db.execute("""
            INSERT INTO tasks (
                task_id, job_id, task_type, state, split_start, split_end,
                attempt_id, created_at
            ) VALUES (?, ?, 'MAP', 'QUEUED', ?, ?, 0, ?)
            """, (task_id, job_id, s_start, s_end, now))

        await emit(db, "JOB_RECEIVED", "MASTER",
                   f"Job '{name}' received: input {os.path.basename(input_path)} "
                   f"({sum(e - s_ for s_, e in splits):,} bytes)", job_id=job_id)
        await emit(db, "TASKS_CREATED", "MASTER",
                   f"Scheduler split input into {len(splits)} byte-range MAP tasks ({num_reducers} REDUCE tasks follow after the barrier)",
                   job_id=job_id)
        await db.commit()
        MAPREDUCE_TASKS_QUEUED.labels(task_type="MAP").inc(len(splits))
        log.info(f"Created job {job_id} with {len(splits)} MAP tasks")
    finally:
        await db.close()

    return job_id

DOCKER_SOCKET = "/var/run/docker.sock"
_SPARE_CONTAINER_IDS = set()
_ACTIVE_CONTAINER_IDS = set()
_LAST_ROLE_REFRESH = 0.0

async def _get_worker_role(worker_id: str) -> str:
    """Identifies whether a worker is in the active pool (Nodes 1-3) or standby spare pool (Nodes 4-5)."""
    global _SPARE_CONTAINER_IDS, _ACTIVE_CONTAINER_IDS, _LAST_ROLE_REFRESH
    import time
    now = time.time()
    if now - _LAST_ROLE_REFRESH > 5.0 or not _ACTIVE_CONTAINER_IDS or not _SPARE_CONTAINER_IDS:
        try:
            import httpx
            if os.path.exists(DOCKER_SOCKET):
                transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET)
                async with httpx.AsyncClient(transport=transport, base_url="http://docker", timeout=2.0) as client:
                    resp = await client.get("/containers/json?all=1")
                    if resp.status_code == 200:
                        spare_ids = set()
                        active_ids = set()
                        for c in resp.json():
                            cid = c.get("Id", "")[:12]
                            for n in c.get("Names", []):
                                if any(n.endswith(f"worker-{x}") for x in (4, 5)):
                                    spare_ids.add(cid)
                                elif any(n.endswith(f"worker-{x}") for x in (1, 2, 3)):
                                    active_ids.add(cid)
                        if spare_ids:
                            _SPARE_CONTAINER_IDS = spare_ids
                        if active_ids:
                            _ACTIVE_CONTAINER_IDS = active_ids
                        _LAST_ROLE_REFRESH = now
        except Exception as ex:
            log.warning(f"Error querying docker socket for worker roles: {ex}")

    # Extract container hostname from worker_id: worker-{cid}-{uuid}
    parts = worker_id.split("-")
    target_cid = parts[1] if len(parts) >= 2 else worker_id

    for sid in _SPARE_CONTAINER_IDS:
        if sid.startswith(target_cid) or target_cid.startswith(sid):
            return "spare"
    return "active"

async def poll_task(worker_id: str) -> Optional[TaskResponse]:
    """Atomically assign an available QUEUED task to worker.
    
    Active workers (Nodes 1-3) process initial map tasks (attempt 0).
    Standby spare workers (Nodes 4-5) only claim evicted tasks (attempt > 0)
    or assist in the reduce merge phase.
    """
    role = await _get_worker_role(worker_id)
    is_spare = (role == "spare")

    db = await get_db()
    try:
        # Check active jobs first
        if is_spare:
            cursor = await db.execute("""
            SELECT t.task_id, t.job_id, t.task_type, t.attempt_id, t.split_start, t.split_end,
                   t.partition_id, t.last_checkpoint_offset, t.last_checkpoint_seq,
                   j.map_fn, j.reduce_fn, j.input_path, j.output_path, j.num_reducers
            FROM tasks t
            JOIN jobs j ON t.job_id = j.job_id
            WHERE t.state = 'QUEUED' AND j.state IN ('MAP_RUNNING', 'REDUCE_RUNNING')
              AND (t.attempt_id > 0 OR t.task_type = 'REDUCE')
            ORDER BY CASE WHEN t.task_type = 'MAP' THEN 1 ELSE 2 END, t.attempt_id DESC, t.created_at ASC
            LIMIT 1
            """)
        else:
            cursor = await db.execute("""
            SELECT t.task_id, t.job_id, t.task_type, t.attempt_id, t.split_start, t.split_end,
                   t.partition_id, t.last_checkpoint_offset, t.last_checkpoint_seq,
                   j.map_fn, j.reduce_fn, j.input_path, j.output_path, j.num_reducers
            FROM tasks t
            JOIN jobs j ON t.job_id = j.job_id
            WHERE t.state = 'QUEUED' AND j.state IN ('MAP_RUNNING', 'REDUCE_RUNNING')
            ORDER BY CASE WHEN t.task_type = 'MAP' THEN 1 ELSE 2 END, t.created_at ASC
            LIMIT 1
            """)
        row = await cursor.fetchone()
        if not row:
            return None

        task_id = row["task_id"]
        task_type = row["task_type"]
        now = datetime.now(timezone.utc)
        deadline = (now + timedelta(seconds=HEARTBEAT_TIMEOUT_SECONDS)).isoformat()

        # Atomically update task to RUNNING
        res = await db.execute("""
        UPDATE tasks
        SET state = 'RUNNING',
            assigned_worker = ?,
            lease_deadline = ?,
            started_at = COALESCE(started_at, ?)
        WHERE task_id = ? AND state = 'QUEUED'
        """, (worker_id, deadline, now.isoformat(), task_id))
        await db.commit()

        if res.rowcount == 0:
            # Another worker grabbed it concurrently
            return None

        MAPREDUCE_TASKS_QUEUED.labels(task_type=task_type).dec()
        MAPREDUCE_TASKS_RUNNING.labels(task_type=task_type).inc()

        if is_spare:
            await emit(db, "SPARE_SPAWNED", "AUTOSCALER",
                       f"Standby Spare {worker_id} SPAWNED IN as replacement for {task_type} {task_id} (attempt {row['attempt_id']})"
                       + (f", resuming from Redis CAS byte {row['last_checkpoint_offset']:,}" if (row["last_checkpoint_offset"] or 0) > (row["split_start"] or 0) else ""),
                       job_id=row["job_id"], task_id=task_id, worker_id=worker_id,
                       meta={"type": task_type, "attempt": row["attempt_id"],
                             "resume_offset": row["last_checkpoint_offset"] or 0, "is_spare": True})
        else:
            await emit(db, "TASK_ASSIGNED", "MASTER",
                       f"Assigned {task_type} {task_id} (attempt {row['attempt_id']}) to {worker_id}"
                       + (f", resuming from byte {row['last_checkpoint_offset']:,}" if (row["last_checkpoint_offset"] or 0) > (row["split_start"] or 0) else ""),
                       job_id=row["job_id"], task_id=task_id, worker_id=worker_id,
                       meta={"type": task_type, "attempt": row["attempt_id"],
                             "resume_offset": row["last_checkpoint_offset"] or 0})
        await db.commit()

        log.info(f"Assigned {task_type} task {task_id} (attempt {row['attempt_id']}, spare={is_spare}) to {worker_id}")

        return TaskResponse(
            task_id=task_id,
            job_id=row["job_id"],
            task_type=task_type,
            attempt_id=row["attempt_id"],
            split_start=row["split_start"],
            split_end=row["split_end"],
            input_path=row["input_path"],
            map_fn=row["map_fn"],
            partition_id=row["partition_id"],
            reduce_fn=row["reduce_fn"],
            output_path=row["output_path"],
            num_reducers=row["num_reducers"],
            rss_url=RSS_URL,
            resume_from_offset=row["last_checkpoint_offset"] or 0,
            resume_from_seq=row["last_checkpoint_seq"] or 0
        )
    finally:
        await db.close()

async def mark_task_complete(
    task_id: str,
    attempt_id: int,
    final_byte_offset: int,
    final_records_processed: int
) -> bool:
    db = await get_db()
    try:
        now = utcnow_iso()
        cursor = await db.execute("""
        SELECT task_type, state, job_id, assigned_worker FROM tasks WHERE task_id = ? AND attempt_id = ?
        """, (task_id, attempt_id))
        row = await cursor.fetchone()
        if not row or row["state"] != "RUNNING":
            log.warning(f"Rejecting complete for task {task_id}: attempt_id mismatch or not RUNNING")
            return False

        task_type = row["task_type"]

        await db.execute("""
        UPDATE tasks
        SET state = 'COMPLETED',
            last_checkpoint_offset = ?,
            last_checkpoint_seq = ?,
            completed_at = ?
        WHERE task_id = ? AND attempt_id = ?
        """, (final_byte_offset, final_records_processed, now, task_id, attempt_id))
        await emit(db, "TASK_COMPLETED", "WORKER",
                   f"{task_type} {task_id} completed by {row['assigned_worker']}",
                   job_id=row["job_id"], task_id=task_id, worker_id=row["assigned_worker"])
        await db.commit()

        MAPREDUCE_TASKS_RUNNING.labels(task_type=task_type).dec()
        MAPREDUCE_TASKS_COMPLETED_TOTAL.labels(task_type=task_type).inc()
        log.info(f"Task {task_id} completed successfully")
        return True
    finally:
        await db.close()

async def drain_and_requeue_task(
    task_id: str,
    attempt_id: int,
    final_byte_offset: int,
    final_records_processed: int
) -> bool:
    """Invoked when worker drains cleanly due to SIGTERM preemption."""
    db = await get_db()
    try:
        cursor = await db.execute("""
        SELECT task_type, attempt_id, job_id, assigned_worker FROM tasks WHERE task_id = ?
        """, (task_id,))
        row = await cursor.fetchone()
        if not row:
            return False

        task_type = row["task_type"]
        new_attempt = row["attempt_id"] + 1
        await emit(db, "DRAIN_RECEIVED", "WORKER",
                   f"{row['assigned_worker']} intercepted the spot warning and drained {task_id}: "
                   f"flushed to RSS, checkpointed at byte {final_byte_offset:,}",
                   job_id=row["job_id"], task_id=task_id, worker_id=row["assigned_worker"],
                   meta={"offset": final_byte_offset})

        await db.execute("""
        UPDATE tasks
        SET state = 'QUEUED',
            assigned_worker = NULL,
            attempt_id = ?,
            lease_deadline = NULL,
            last_checkpoint_offset = ?,
            last_checkpoint_seq = ?
        WHERE task_id = ?
        """, (new_attempt, final_byte_offset, final_records_processed, task_id))
        await emit(db, "TASK_REQUEUED", "MASTER",
                   f"Task {task_id} requeued as attempt {new_attempt}; next free node resumes from byte {final_byte_offset:,}",
                   job_id=row["job_id"], task_id=task_id, meta={"attempt": new_attempt, "offset": final_byte_offset})
        await db.commit()

        MAPREDUCE_TASKS_RUNNING.labels(task_type=task_type).dec()
        MAPREDUCE_TASKS_QUEUED.labels(task_type=task_type).inc()
        MAPREDUCE_PREEMPTIONS_TOTAL.inc()

        log.warning(f"Task {task_id} cleanly drained and requeued with attempt={new_attempt} (offset={final_byte_offset})")
        return True
    finally:
        await db.close()

async def fail_task(task_id: str, attempt_id: int, error_message: str) -> tuple[bool, int]:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT task_type, attempt_id, job_id FROM tasks WHERE task_id = ?", (task_id,))
        row = await cursor.fetchone()
        if not row:
            return False, 0

        task_type = row["task_type"]
        cur_attempt = row["attempt_id"]
        new_attempt = cur_attempt + 1

        if new_attempt < MAX_ATTEMPTS:
            # Requeue
            await db.execute("""
            UPDATE tasks
            SET state = 'QUEUED',
                assigned_worker = NULL,
                attempt_id = ?,
                lease_deadline = NULL,
                error_message = ?
            WHERE task_id = ?
            """, (new_attempt, error_message, task_id))
            await db.commit()
            MAPREDUCE_TASKS_RUNNING.labels(task_type=task_type).dec()
            MAPREDUCE_TASKS_QUEUED.labels(task_type=task_type).inc()
            log.warning(f"Task {task_id} failed: {error_message}. Requeuing attempt {new_attempt}")
            return True, new_attempt
        else:
            # Fatal task failure
            now = utcnow_iso()
            await db.execute("""
            UPDATE tasks
            SET state = 'FAILED',
                error_message = ?,
                completed_at = ?
            WHERE task_id = ?
            """, (error_message, now, task_id))
            # Mark job failed
            await db.execute("UPDATE jobs SET state = 'FAILED', error_message = ? WHERE job_id = ?",
                             (f"Task {task_id} failed after {MAX_ATTEMPTS} attempts: {error_message}", row["job_id"]))
            await db.commit()
            MAPREDUCE_TASKS_RUNNING.labels(task_type=task_type).dec()
            log.error(f"Task {task_id} permanently failed after {MAX_ATTEMPTS} attempts.")
            return False, cur_attempt
    finally:
        await db.close()

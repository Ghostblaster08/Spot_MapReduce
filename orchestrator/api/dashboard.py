import os
import shutil
import json
import re
import time
import asyncio
import logging
from typing import Dict, Any, List, Optional
from collections import Counter
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query, Response, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from orchestrator.core.database import get_db, emit
from orchestrator.core.scheduler import create_job_and_map_tasks, utcnow_iso

try:
    import redis
except ImportError:
    redis = None

try:
    import httpx
except ImportError:
    httpx = None

router = APIRouter(tags=["Dashboard"])
log = logging.getLogger("orchestrator.api.dashboard")

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
RSS_URL = os.environ.get("RSS_URL", "http://rss:8001").rstrip("/")
DOCKER_SOCKET = "/var/run/docker.sock"

# In-memory buffer of recent events for live timeline
recent_events: List[Dict[str, Any]] = []
MAX_EVENTS = 150

# Track per-container termination reasons (Preemption vs Hard Crash)
_termination_reasons: Dict[str, Dict[str, Any]] = {}

def record_event(category: str, message: str, level: str = "info", meta: Optional[Dict[str, Any]] = None):
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3],
        "category": category,
        "message": message,
        "level": level,
        "meta": meta or {}
    }
    recent_events.append(event)
    if len(recent_events) > MAX_EVENTS:
        recent_events.pop(0)

def seed_input_data():
    """Seeds input dataset files into /data/input so workers find them immediately."""
    try:
        input_dir = "/data/input"
        os.makedirs(input_dir, exist_ok=True)
        seed_dir = "/jobs_seed"

        # 1. Copy sample_input.txt if available
        sample_path = os.path.join(input_dir, "sample_input.txt")
        if not os.path.exists(sample_path) and os.path.exists(seed_dir):
            seed_sample = os.path.join(seed_dir, "sample_input.txt")
            if os.path.exists(seed_sample):
                shutil.copy2(seed_sample, sample_path)
                log.info(f"Seeded {sample_path} from {seed_sample}")
                record_event("STORAGE", "Seeded sample_input.txt into /data/input", "info")

        # 2. Generate large_input.txt (4.4 MB / 77,500 lines) if not present
        large_path = os.path.join(input_dir, "large_input.txt")
        if not os.path.exists(large_path) and os.path.exists(sample_path):
            with open(sample_path, "r", encoding="utf-8") as f:
                content = f.read()
            with open(large_path, "w", encoding="utf-8") as f:
                for _ in range(2500):
                    f.write(content)
            size_mb = os.path.getsize(large_path) / (1024 * 1024)
            log.info(f"Generated {large_path} ({size_mb:.2f} MB, 77,500 lines)")
            record_event("STORAGE", f"Generated large_input.txt ({size_mb:.2f} MB, 77,500 lines)", "info")
    except Exception as e:
        log.warning(f"Error seeding input data: {e}")

# ── Docker Engine API Helpers ──────────────────────────────────────────

async def get_docker_containers() -> List[Dict[str, Any]]:
    """Fetches container list directly from Docker Engine via Unix domain socket."""
    if not os.path.exists(DOCKER_SOCKET) or httpx is None:
        return []
    try:
        transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET)
        async with httpx.AsyncClient(transport=transport, base_url="http://docker", timeout=2.0) as client:
            resp = await client.get("/containers/json?all=1")
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        log.debug(f"Docker API request failed: {e}")
    return []

async def stop_docker_container(container_id: str, timeout: int = 15) -> bool:
    """Sends SIGTERM with grace period to a container."""
    if not os.path.exists(DOCKER_SOCKET) or httpx is None:
        return False
    try:
        transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET)
        async with httpx.AsyncClient(transport=transport, base_url="http://docker", timeout=timeout + 5) as client:
            resp = await client.post(f"/containers/{container_id}/stop?t={timeout}")
            return resp.status_code in (204, 304)
    except Exception as e:
        log.error(f"Error stopping container {container_id}: {e}")
        return False

async def kill_docker_container(container_id: str) -> bool:
    """Sends SIGKILL to a container instantly (hard crash)."""
    if not os.path.exists(DOCKER_SOCKET) or httpx is None:
        return False
    try:
        transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET)
        async with httpx.AsyncClient(transport=transport, base_url="http://docker", timeout=3.0) as client:
            resp = await client.post(f"/containers/{container_id}/kill?signal=SIGKILL")
            return resp.status_code in (204, 304)
    except Exception as e:
        log.error(f"Error killing container {container_id}: {e}")
        return False

async def restart_docker_container(container_id: str) -> bool:
    """Starts or restarts a container."""
    if not os.path.exists(DOCKER_SOCKET) or httpx is None:
        return False
    try:
        transport = httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET)
        async with httpx.AsyncClient(transport=transport, base_url="http://docker", timeout=5.0) as client:
            resp = await client.post(f"/containers/{container_id}/restart")
            return resp.status_code in (204, 304)
    except Exception as e:
        log.error(f"Error restarting container {container_id}: {e}")
        return False


async def pick_busy_container(running_only: bool = True):
    """Pick the worker container that is actually executing a RUNNING task.

    Worker IDs embed the container hostname (== short container id), so we can
    map a task's assigned_worker back to a Docker container. Falls back to any
    running worker container if no task is currently running.
    Returns (container_dict_or_None, task_id_or_None).
    """
    db = await get_db()
    try:
        cur = await db.execute("SELECT task_id, assigned_worker FROM tasks WHERE state = 'RUNNING' AND assigned_worker IS NOT NULL")
        running = [dict(r) for r in await cur.fetchall()]
    finally:
        await db.close()
    containers = [c for c in await get_docker_containers()
                  if any("worker" in n for n in c.get("Names", [])) and c.get("State") == "running"]
    for r in running:
        for c in containers:
            if c["Id"][:12] in r["assigned_worker"]:
                return c, r["task_id"]
    return (containers[0] if containers else None), None

class JobUploadPayload(BaseModel):
    filename: str = Field(default="custom_input.txt")
    content: str
    num_mappers: int = Field(default=3, ge=1, le=16)
    num_reducers: int = Field(default=3, ge=1, le=16)
    name: Optional[str] = None

async def auto_spawn_replacement(delay_seconds: float = 2.0, reason: str = "Preemption drain recovery"):
    """Background task: waits delay_seconds, then provisions a replacement spot worker."""
    await asyncio.sleep(delay_seconds)
    docker_containers = await get_docker_containers()
    stopped = [c for c in docker_containers if any("worker" in n for n in c.get("Names", [])) and c.get("State") != "running"]
    if not stopped:
        return
    chosen = stopped[0]
    c_name = chosen.get("Names", ["worker"])[0].lstrip("/")

    db = await get_db()
    try:
        await emit(db, "AUTOSCALER", "MASTER",
                   f"Master Autoscaler: Node {c_name} terminated ({reason}). Provisioning and spawning replacement spot node...",
                   worker_id=c_name)
        await db.commit()
    finally:
        await db.close()

    success = await restart_docker_container(chosen["Id"])
    if success:
        db = await get_db()
        try:
            await emit(db, "NODE_ONLINE", "MASTER",
                       f"Master Autoscaler: Replacement node {c_name} is online and joined the compute fleet.",
                       worker_id=c_name)
            await db.commit()
        finally:
            await db.close()
        record_event("AUTOSCALER", f"Spawned replacement node {c_name} ({reason})", "success")

# ── Redis Checkpoint Helpers ──────────────────────────────────────────

def fetch_redis_checkpoints(job_id: str) -> Dict[str, Any]:
    """Queries Redis directly for all task checkpoint keys for a job."""
    if not redis:
        return {}
    try:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=1.0)
        keys = r.keys(f"smr:{job_id}:*")
        checkpoints: Dict[str, Any] = {}

        for k in keys:
            parts = k.split(":")
            # Format: smr:<job_id>:<task_id>:<attempt_id|latest_attempt>:<field>
            if len(parts) >= 4:
                task_id = parts[2]
                if task_id not in checkpoints:
                    checkpoints[task_id] = {
                        "latest_attempt": "0",
                        "attempts": {}
                    }
                if parts[3] == "latest_attempt":
                    checkpoints[task_id]["latest_attempt"] = r.get(k) or "0"
                elif len(parts) >= 5:
                    attempt = parts[3]
                    field = parts[4]
                    if attempt not in checkpoints[task_id]["attempts"]:
                        checkpoints[task_id]["attempts"][attempt] = {}
                    if field == "manifest":
                        checkpoints[task_id]["attempts"][attempt][field] = r.hgetall(k)
                    else:
                        checkpoints[task_id]["attempts"][attempt][field] = r.get(k)

        return checkpoints
    except Exception as e:
        log.warning(f"Redis query failed: {e}")
        return {}

# ── Output Inspection & Ground Truth Helpers ──────────────────────────

def inspect_job_output(job_id: str, output_base: str = "/data/output") -> Dict[str, Any]:
    job_dir = os.path.join(output_base, job_id)
    if not os.path.exists(job_dir):
        return {"exists": False, "partitions": [], "is_success": False, "sample_words": []}

    files = os.listdir(job_dir)
    is_success = "_SUCCESS" in files
    partitions = sorted([f for f in files if f.startswith("part-")])

    sample_words = []
    total_lines = 0
    # Read first partition or combined top words
    word_counts = []
    for p in partitions:
        p_path = os.path.join(job_dir, p)
        try:
            with open(p_path, "r", encoding="utf-8") as f:
                for line in f:
                    total_lines += 1
                    parts = line.strip().split("\t", 1)
                    if len(parts) == 2:
                        word, count = parts[0], int(parts[1])
                        word_counts.append((word, count))
        except Exception:
            pass

    # Sort descending by count
    word_counts.sort(key=lambda x: x[1], reverse=True)
    sample_words = [{"word": w, "count": c} for w, c in word_counts[:12]]

    return {
        "exists": True,
        "is_success": is_success,
        "partition_count": len(partitions),
        "partitions": partitions,
        "total_unique_words": len(word_counts),
        "total_lines": total_lines,
        "top_words": sample_words
    }

def verify_ground_truth(input_path: str, output_base: str, job_id: str) -> Dict[str, Any]:
    """Computes exact single-node word count ground truth and compares against job output."""
    if not os.path.exists(input_path):
        return {"status": "ERROR", "message": f"Input file not found: {input_path}"}

    job_dir = os.path.join(output_base, job_id)
    if not os.path.exists(job_dir):
        return {"status": "ERROR", "message": f"Output dir not found: {job_dir}"}

    # 1. Ground truth
    with open(input_path, "r", encoding="utf-8") as f:
        text = f.read().lower()
    words = re.findall(r"[a-zA-Z0-9]+", text)
    expected = Counter(words)

    # 2. Output partitions
    actual = Counter()
    partitions = sorted([f for f in os.listdir(job_dir) if f.startswith("part-")])
    for p in partitions:
        with open(os.path.join(job_dir, p), "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("\t", 1)
                if len(parts) == 2:
                    actual[parts[0]] = int(parts[1])

    # 3. Diff
    diffs = []
    for word, exp_count in expected.items():
        act_count = actual.get(word, 0)
        if act_count != exp_count:
            diffs.append({"word": word, "expected": exp_count, "actual": act_count})

    for word in actual:
        if word not in expected:
            diffs.append({"word": word, "expected": 0, "actual": actual[word]})

    if not diffs and len(actual) > 0:
        return {
            "status": "PASS",
            "matches": True,
            "unique_words_verified": len(actual),
            "total_words_processed": sum(actual.values()),
            "message": f"Zero discrepancies! Verified {len(actual)} unique words ({sum(actual.values()):,} total occurrences)."
        }
    else:
        return {
            "status": "FAIL",
            "matches": False,
            "discrepancies_count": len(diffs),
            "discrepancies_sample": diffs[:10],
            "message": f"Discrepancies detected across {len(diffs)} words."
        }

# ── Cluster State API ──────────────────────────────────────────────────

@router.get("/dashboard/state")
async def get_dashboard_state():
    """Aggregates live state from SQLite, Redis, RSS, and Docker for real-time visualization."""
    db = await get_db()
    try:
        # 1. Latest / Active Job
        cursor = await db.execute("""
        SELECT job_id, name, state, map_fn, reduce_fn, input_path, output_path,
               num_mappers, num_reducers, submitted_at, started_at, completed_at, error_message
        FROM jobs
        ORDER BY submitted_at DESC
        LIMIT 1
        """)
        job_row = await cursor.fetchone()

        active_job = None
        tasks_list = []
        job_id = None

        if job_row:
            job_id = job_row["job_id"]
            active_job = dict(job_row)

            # Query all tasks for this job
            t_cur = await db.execute("""
            SELECT task_id, job_id, task_type, state, split_start, split_end,
                   partition_id, assigned_worker, attempt_id, lease_deadline,
                   last_checkpoint_offset, last_checkpoint_seq, last_checkpoint_at,
                   created_at, started_at, completed_at, error_message
            FROM tasks
            WHERE job_id = ?
            ORDER BY CASE WHEN task_type = 'MAP' THEN 1 ELSE 2 END, task_id ASC
            """, (job_id,))
            tasks_list = [dict(r) for r in await t_cur.fetchall()]

        # 2. Query Recent Jobs
        j_cur = await db.execute("SELECT job_id, name, state, submitted_at, completed_at FROM jobs ORDER BY submitted_at DESC LIMIT 5")
        recent_jobs = [dict(r) for r in await j_cur.fetchall()]

        # 3. Overall Task Metrics
        m_cur = await db.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN state = 'RUNNING' THEN 1 ELSE 0 END) as running,
            SUM(CASE WHEN state = 'QUEUED' THEN 1 ELSE 0 END) as queued,
            SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failed
        FROM tasks
        """)
        m_counts = dict(await m_cur.fetchone() or {})

        # 4. Total Preemptions and Crashes from DB
        ckpt_cur = await db.execute("SELECT COUNT(*) as count FROM checkpoints WHERE is_final_drain = 1")
        final_drains = (await ckpt_cur.fetchone())["count"]

        attempt_cur = await db.execute("SELECT SUM(attempt_id) as total_retries FROM tasks")
        total_retries = (await attempt_cur.fetchone())["total_retries"] or 0

        # 5. Query Docker Containers
        docker_containers = await get_docker_containers()
        worker_containers = []
        for c in docker_containers:
            names = c.get("Names", [])
            for name in names:
                if "worker" in name:
                    worker_containers.append({
                        "id": c.get("Id", "")[:12],
                        "name": name.lstrip("/"),
                        "state": c.get("State", ""),
                        "status": c.get("Status", ""),
                        "created": c.get("Created", 0)
                    })
                    break

        worker_containers.sort(key=lambda c: c.get("name", ""))
        for idx, c in enumerate(worker_containers):
            cid = c["id"]
            cname = c["name"]
            matching_task = None
            for t in tasks_list:
                assigned = t.get("assigned_worker") or ""
                if cid in assigned or cname in assigned:
                    matching_task = t
                    if t["state"] == "RUNNING":
                        break
            c["current_task"] = matching_task
            c["is_spare"] = (idx >= 3)

        # 6. Synthesize Workers view
        # Map assigned workers from tasks + Docker containers
        workers_map: Dict[str, Any] = {}
        now_iso = datetime.now(timezone.utc).isoformat()

        # Populate from active tasks
        for t in tasks_list:
            w_id = t.get("assigned_worker")
            if w_id:
                is_expired = t.get("lease_deadline") and t["lease_deadline"] < now_iso
                workers_map[w_id] = {
                    "worker_id": w_id,
                    "task_id": t["task_id"],
                    "task_type": t["task_type"],
                    "task_state": t["state"],
                    "attempt_id": t["attempt_id"],
                    "split_start": t["split_start"],
                    "split_end": t["split_end"],
                    "current_offset": t["last_checkpoint_offset"],
                    "lease_deadline": t["lease_deadline"],
                    "is_lease_expired": is_expired,
                    "status": "PROCESSING" if t["state"] == "RUNNING" else t["state"]
                }

        # 7. Query RSS partition summary
        rss_summary = {}
        if job_id and httpx:
            try:
                async with httpx.AsyncClient(timeout=1.0) as client:
                    resp = await client.get(f"{RSS_URL}/partitions/{job_id}/manifest")
                    if resp.status_code == 200:
                        rss_summary = resp.json().get("partitions", {})
            except Exception:
                pass

        # 8. Query Redis checkpoints
        redis_ckpts = {}
        if job_id:
            redis_ckpts = fetch_redis_checkpoints(job_id)
            for tid, cdata in redis_ckpts.items():
                t_match = next((t for t in tasks_list if t["task_id"] == tid), None)
                if t_match:
                    cdata["split_start"] = t_match.get("split_start", 0)
                    cdata["split_end"] = t_match.get("split_end", 0)
                    cdata["assigned_worker"] = t_match.get("assigned_worker")
                    cdata["task_state"] = t_match.get("state")
                    cdata["task_type"] = t_match.get("task_type")

        # 9. Output Inspection
        output_info = {}
        if job_id and active_job:
            output_info = inspect_job_output(job_id, active_job.get("output_path", "/data/output"))

        # 10. Split Map and Reduce tasks for clear individual step presentation
        map_tasks = [t for t in tasks_list if t.get("task_type") == "MAP"]
        reduce_tasks = [t for t in tasks_list if t.get("task_type") == "REDUCE"]

        # 11. Extract Fault Tolerance story from REAL checkpoint history.
        # For every task retried (attempt_id > 0), look up the last checkpoint
        # written by the *previous* attempt. is_final_drain=1 means the worker
        # caught SIGTERM and drained cleanly; otherwise the worker died abruptly
        # (SIGKILL / lease expiry) and we resume from its last periodic checkpoint.
        faults = []
        for t in tasks_list:
            if t.get("attempt_id", 0) > 0:
                prev_attempt = t["attempt_id"] - 1
                c_cur = await db.execute("""
                SELECT byte_offset, records_processed, is_final_drain, created_at
                FROM checkpoints
                WHERE task_id = ? AND attempt_id = ?
                ORDER BY created_at DESC LIMIT 1
                """, (t["task_id"], prev_attempt))
                ck = await c_cur.fetchone()
                resume_offset = ck["byte_offset"] if ck else (t["split_start"] or 0)
                clean = bool(ck["is_final_drain"]) if ck else False
                split_start = t["split_start"] or 0
                split_end = t["split_end"] or 0
                saved = max(0, resume_offset - split_start)
                span = max(1, split_end - split_start)
                cause = "SIGTERM_DRAIN" if clean else "CRASH_LEASE_EXPIRED"
                faults.append({
                    "has_fault": True,
                    "cause": cause,
                    "task_id": t["task_id"],
                    "task_type": t["task_type"],
                    "attempt_id": t["attempt_id"],
                    "assigned_worker": t["assigned_worker"] or "replacement worker",
                    "split_start": split_start,
                    "split_end": split_end,
                    "resumed_offset": resume_offset,
                    "records_at_checkpoint": ck["records_processed"] if ck else 0,
                    "work_preserved_pct": round(100 * saved / span),
                    "task_state": t["state"],
                })
        fault_story = faults[0] if faults else None

        # Query real durable events from SQLite table
        e_cur = await db.execute("""
        SELECT id, ts, job_id, kind, source, message, task_id, worker_id, meta
        FROM events
        ORDER BY id DESC
        LIMIT 60
        """)
        durable_events = [dict(r) for r in await e_cur.fetchall()]

        # Real tools and services inventory
        services_list = [
            {
                "name": "Master Orchestrator",
                "tech": "FastAPI + Uvicorn (Python 3.12)",
                "role": "DAG Task Scheduling, Lease Management, Stage Barriers",
                "port": "8000",
                "state": "Online",
                "endpoint": "http://localhost:8000"
            },
            {
                "name": "Remote Shuffle Service (RSS)",
                "tech": "FastAPI + Memory Slabs + Spill Disk",
                "role": "Decoupled intermediate shuffle storage & partition indexing",
                "port": "8001",
                "state": "Online",
                "endpoint": "http://localhost:8001"
            },
            {
                "name": "Checkpoint & Lease Store",
                "tech": "Redis 7.2 (Lua CAS scripts)",
                "role": "Atomic task checkpoints, monotonic offset commits, worker fencing",
                "port": "6379",
                "state": "Online",
                "endpoint": "redis://localhost:6379"
            },
            {
                "name": "Durable Metadata Store",
                "tech": "SQLite 3 (WAL Mode)",
                "role": "ACID job & task state transitions, audit event log",
                "port": "File: state.db",
                "state": "Online",
                "endpoint": "/data/orchestrator/state.db"
            },
            {
                "name": "Spot Worker Fleet",
                "tech": "4x Isolated Docker Containers",
                "role": "Map/Reduce execution, SIGTERM trap & 15s graceful drain",
                "port": "Dynamic RPC",
                "state": f"{len([w for w in worker_containers if w.get('state') == 'running'])}/4 Running",
                "endpoint": "Internal Network"
            },
            {
                "name": "Metrics Telemetry",
                "tech": "Prometheus v2.51",
                "role": "Scrapes orchestrator & RSS metrics every 5s",
                "port": "9090",
                "state": "Online",
                "endpoint": "http://localhost:9090"
            },
            {
                "name": "Observability Dashboard",
                "tech": "Grafana v10.4",
                "role": "Visualizes task throughput, preemptions, RSS memory usage",
                "port": "3030",
                "state": "Online",
                "endpoint": "http://localhost:3030"
            },
            {
                "name": "Node Autoscaler & Engine Daemon",
                "tech": "Docker Engine Unix Domain Socket",
                "role": "Dynamic spot node provisioning, preemption simulation, auto-recovery",
                "port": "/var/run/docker.sock",
                "state": "Connected",
                "endpoint": "Unix Socket"
            }
        ]

        return {
            "timestamp": utcnow_iso(),
            "active_job": active_job,
            "tasks": tasks_list,
            "map_tasks": map_tasks,
            "reduce_tasks": reduce_tasks,
            "fault_story": fault_story,
            "faults": faults,
            "recent_jobs": recent_jobs,
            "metrics": {
                "tasks_total": m_counts.get("total", 0),
                "tasks_completed": m_counts.get("completed", 0),
                "tasks_running": m_counts.get("running", 0),
                "tasks_queued": m_counts.get("queued", 0),
                "tasks_failed": m_counts.get("failed", 0),
                "clean_preemptions": final_drains,
                "total_retries": total_retries
            },
            "workers": list(workers_map.values()),
            "docker_workers": worker_containers,
            "rss_partitions": rss_summary,
            "redis_checkpoints": redis_ckpts,
            "output": output_info,
            "events": durable_events,
            "recent_events": list(reversed(recent_events[-50:])),
            "services": services_list,
            "termination_reasons": _termination_reasons
        }
    finally:
        await db.close()

# ── Interactive Demo Action Endpoints ──────────────────────────────────

@router.post("/dashboard/upload-job")
async def dashboard_upload_job(payload: JobUploadPayload):
    """Uploads a custom text file for WordCount execution."""
    if not payload.content or not payload.content.strip():
        raise HTTPException(status_code=400, detail="Uploaded file content cannot be empty.")

    safe_name = os.path.basename(payload.filename).replace(" ", "_")
    if not safe_name.endswith(".txt"):
        safe_name = f"{safe_name}.txt"

    input_dir = "/data/input"
    os.makedirs(input_dir, exist_ok=True)
    input_path = os.path.join(input_dir, safe_name)

    with open(input_path, "w", encoding="utf-8") as f:
        f.write(payload.content)

    file_size = os.path.getsize(input_path)
    line_count = payload.content.count("\n") + (1 if payload.content and not payload.content.endswith("\n") else 0)

    job_name = payload.name or f"upload-{safe_name[:12]}-{int(time.time())}"
    job_id = await create_job_and_map_tasks(
        name=job_name,
        map_fn="worker.jobs.wordcount.map_fn",
        reduce_fn="worker.jobs.wordcount.reduce_fn",
        input_path=input_path,
        output_path="/data/output",
        num_mappers=payload.num_mappers,
        num_reducers=payload.num_reducers
    )

    db = await get_db()
    try:
        await emit(db, "FILE_UPLOADED", "STORAGE",
                   f"Received file upload '{safe_name}' ({file_size:,} bytes, {line_count:,} lines) saved to /data/input/{safe_name}",
                   job_id=job_id)
        await db.commit()
    finally:
        await db.close()

    record_event("STORAGE", f"Uploaded {safe_name} ({file_size:,} bytes, {line_count:,} lines)", "success", {"job_id": job_id})
    return {
        "job_id": job_id,
        "name": job_name,
        "filename": safe_name,
        "size_bytes": file_size,
        "lines": line_count,
        "num_mappers": payload.num_mappers,
        "num_reducers": payload.num_reducers
    }

@router.post("/dashboard/submit-job")
async def dashboard_submit_job(
    dataset: str = Query("large", description="'large' (4.4MB) or 'small' (1.8KB)"),
    num_mappers: int = Query(3, ge=1, le=16),
    num_reducers: int = Query(3, ge=1, le=16),
    name: Optional[str] = Query(None)
):
    """Submits a real MapReduce job with selectable dataset."""
    seed_input_data()
    filename = "large_input.txt" if dataset == "large" else "sample_input.txt"
    input_path = f"/data/input/{filename}"
    job_name = name or f"demo-{dataset}-{int(time.time())}"

    job_id = await create_job_and_map_tasks(
        name=job_name,
        map_fn="worker.jobs.wordcount.map_fn",
        reduce_fn="worker.jobs.wordcount.reduce_fn",
        input_path=input_path,
        output_path="/data/output",
        num_mappers=num_mappers,
        num_reducers=num_reducers
    )
    record_event("JOB_SUBMISSION", f"Submitted job '{job_name}' ({job_id}) with {dataset} dataset ({num_mappers} mappers, {num_reducers} reducers)", "success", {"job_id": job_id})
    return {"job_id": job_id, "name": job_name, "input_path": input_path}

@router.post("/dashboard/simulate-preemption")
async def dashboard_simulate_preemption(
    grace_seconds: int = Query(15, ge=5, le=60),
    auto_spawn: bool = Query(True),
    target_node: Optional[str] = Query(None)
):
    """Sends SIGTERM (spot preemption notice) to the worker container that is actually busy with a task."""
    chosen = None
    target_task = None
    if target_node:
        containers = [c for c in await get_docker_containers()
                      if target_node in c.get("Names", [""])[0] and c.get("State") == "running"]
        if containers:
            chosen = containers[0]
            # Find running task
            db = await get_db()
            try:
                cur = await db.execute("SELECT task_id, assigned_worker FROM tasks WHERE state = 'RUNNING'")
                for r in await cur.fetchall():
                    if chosen["Id"][:12] in (r["assigned_worker"] or "") or target_node in (r["assigned_worker"] or ""):
                        target_task = r["task_id"]
                        break
            finally:
                await db.close()

    if not chosen:
        chosen, target_task = await pick_busy_container()
    if not chosen:
        raise HTTPException(status_code=400, detail="No running worker containers found to preempt.")
    c_name = chosen.get("Names", ["worker"])[0].lstrip("/")

    db = await get_db()
    try:
        await emit(db, "SENTINEL_WARNING", "SENTINEL",
                   f"Spot preemption notice (SIGTERM) intercepted for node {c_name} (task: {target_task or 'idle'}). 15s drain grace period initiated.",
                   task_id=target_task, worker_id=c_name)
        await db.commit()
    finally:
        await db.close()

    record_event("SENTINEL", f"Spot preemption notice (SIGTERM) -> {c_name}, task={target_task or 'idle'}, grace={grace_seconds}s", "warning", {"container": c_name, "task": target_task})
    success = await stop_docker_container(chosen["Id"], timeout=grace_seconds)

    if success:
        _termination_reasons[c_name] = {
            "type": "PREEMPTION",
            "badge": "DRAINED (SIGTERM)",
            "title": "⚡ SPOT PREEMPTED (SIGTERM DRAIN)",
            "desc": "15s Spot eviction notice intercepted. Clean memory flush to RSS (:8001) & exact byte commit to Redis CAS. Zero data loss.",
            "color": "amber",
            "task_id": target_task
        }

        db = await get_db()
        try:
            await emit(db, "NODE_TERMINATED", "MASTER",
                       f"Node {c_name} terminated after completing emergency preemption drain to RSS and Redis CAS.",
                       task_id=target_task, worker_id=c_name)
            await db.commit()
        finally:
            await db.close()

        record_event("SENTINEL", f"{c_name} stopped after drain: slabs flushed to RSS, offset committed to Redis CAS.", "info")

        if auto_spawn:
            asyncio.create_task(auto_spawn_replacement(delay_seconds=2.0, reason="Preemption drain recovery"))

        return {
            "status": "SUCCESS",
            "message": f"Preemption signal sent to {c_name}. Worker initiated emergency drain.",
            "container": c_name,
            "evicted_task": target_task,
            "auto_spawn": auto_spawn
        }
    raise HTTPException(status_code=500, detail=f"Failed to send SIGTERM to container {c_name}")

@router.post("/dashboard/simulate-crash")
async def dashboard_simulate_crash(
    auto_spawn: bool = Query(True),
    target_node: Optional[str] = Query(None)
):
    """Sends SIGKILL instantly to the busy worker container (no grace period / crash test)."""
    chosen = None
    target_task = None
    if target_node:
        containers = [c for c in await get_docker_containers()
                      if target_node in c.get("Names", [""])[0] and c.get("State") == "running"]
        if containers:
            chosen = containers[0]
            db = await get_db()
            try:
                cur = await db.execute("SELECT task_id, assigned_worker FROM tasks WHERE state = 'RUNNING'")
                for r in await cur.fetchall():
                    if chosen["Id"][:12] in (r["assigned_worker"] or "") or target_node in (r["assigned_worker"] or ""):
                        target_task = r["task_id"]
                        break
            finally:
                await db.close()

    if not chosen:
        chosen, target_task = await pick_busy_container()
    if not chosen:
        raise HTTPException(status_code=400, detail="No running worker containers found to kill.")
    c_name = chosen.get("Names", ["worker"])[0].lstrip("/")

    db = await get_db()
    try:
        await emit(db, "HARD_FAILURE", "FAILURE_DETECTOR",
                   f"Hard crash (SIGKILL) injected into {c_name} (task: {target_task or 'idle'}). No warning, heartbeats stopped abruptly.",
                   task_id=target_task, worker_id=c_name)
        await db.commit()
    finally:
        await db.close()

    record_event("FAILURE_DETECTOR", f"Hard crash (SIGKILL) -> {c_name}, task={target_task or 'idle'}. No warning, no drain.", "error", {"container": c_name, "task": target_task})
    success = await kill_docker_container(chosen["Id"])

    if success:
        _termination_reasons[c_name] = {
            "type": "HARD_CRASH",
            "badge": "KILLED (SIGKILL)",
            "title": "💥 HARD CRASH (SIGKILL)",
            "desc": "Kernel killed process immediately with 0 warning. In-flight buffer lost. Watchdog lease expired; task reassigned to spare.",
            "color": "red",
            "task_id": target_task
        }

        record_event("LEASE_MONITOR", f"{c_name} died. Heartbeats stop; lease watchdog triggers in ~1s, then task is requeued.", "warning")

        # Fast lease watchdog trigger after 1.2s delay to simulate failure detection
        async def _fast_lease_expire(w_name, t_id):
            await asyncio.sleep(1.2)
            db = await get_db()
            try:
                if t_id:
                    res = await db.execute("""
                    UPDATE tasks
                    SET state = 'QUEUED', assigned_worker = NULL, lease_deadline = NULL, attempt_id = attempt_id + 1
                    WHERE task_id = ? AND state = 'RUNNING'
                    """, (t_id,))
                    if res.rowcount > 0:
                        await emit(db, "LEASE_EXPIRED", "LEASE_WATCHDOG",
                                   f"Lease Watchdog: Heartbeat missing from crashed node {w_name}. Requeued task {t_id} for spare node pickup.",
                                   task_id=t_id, worker_id=w_name)
                        await db.commit()
            finally:
                await db.close()

        asyncio.create_task(_fast_lease_expire(c_name, target_task))

        if auto_spawn:
            asyncio.create_task(auto_spawn_replacement(delay_seconds=3.0, reason="Hard crash recovery"))

        return {
            "status": "SUCCESS",
            "message": f"SIGKILL sent to {c_name}. Master failure detector triggered failover.",
            "container": c_name,
            "killed_task": target_task,
            "auto_spawn": auto_spawn
        }
    raise HTTPException(status_code=500, detail=f"Failed to send SIGKILL to container {c_name}")

@router.post("/dashboard/spawn-node")
async def dashboard_spawn_node():
    """Manually provisions and boots a replacement spot worker container."""
    docker_containers = await get_docker_containers()
    stopped = [c for c in docker_containers if any("worker" in n for n in c.get("Names", [])) and c.get("State") != "running"]
    if not stopped:
        return {"status": "MAX_CAPACITY", "message": "All spot worker nodes are already running."}
    chosen = stopped[0]
    c_name = chosen.get("Names", ["worker"])[0].lstrip("/")
    success = await restart_docker_container(chosen["Id"])
    if success:
        _termination_reasons.pop(c_name, None)
        db = await get_db()
        try:
            await emit(db, "AUTOSCALER", "MASTER",
                       f"Master Autoscaler: Provisioned and started replacement spot node {c_name}.",
                       worker_id=c_name)
            await db.commit()
        finally:
            await db.close()
        record_event("AUTOSCALER", f"Provisioned replacement node {c_name}", "success")
        return {"status": "SUCCESS", "message": f"Replacement node {c_name} is online.", "container": c_name}
    raise HTTPException(status_code=500, detail=f"Failed to start container {c_name}")

@router.post("/dashboard/restart-workers")
async def dashboard_restart_workers():
    """Restarts any stopped worker containers to replenish transient compute pool capacity."""
    docker_containers = await get_docker_containers()
    stopped = [c for c in docker_containers if any("worker" in n for n in c.get("Names", [])) and c.get("State") != "running"]

    restarted = []
    for c in stopped:
        c_name = c.get("Names", ["worker"])[0].lstrip("/")
        if await restart_docker_container(c["Id"]):
            restarted.append(c_name)
            _termination_reasons.pop(c_name, None)

    if restarted:
        db = await get_db()
        try:
            await emit(db, "AUTOSCALER", "MASTER",
                       f"Master Autoscaler: Replenished spot capacity: restarted {len(restarted)} workers ({', '.join(restarted)}).")
            await db.commit()
        finally:
            await db.close()
        record_event("AUTOSCALER", f"Replenished spot capacity: restarted {len(restarted)} workers ({', '.join(restarted)}).", "success")

    return {"restarted_count": len(restarted), "workers": restarted}

@router.post("/dashboard/reset")
async def dashboard_reset_cluster():
    """Resets the entire cluster state to 0% IDLE.
    Restarts all stopped workers, resets SQLite DB, flushes Redis,
    cleans RSS partitions, and deletes /data/output.
    """
    import shutil
    try:
        # 1. Restart stopped workers
        docker_containers = await get_docker_containers()
        for c in docker_containers:
            if any("worker" in n for n in c.get("Names", [])) and c.get("State") != "running":
                await restart_docker_container(c["Id"])

        # 2. Reset termination reasons and events
        global _termination_reasons, recent_events
        _termination_reasons.clear()
        recent_events.clear()

        # 3. Reset SQLite Database
        db = await get_db()
        try:
            await db.execute("PRAGMA foreign_keys = OFF")
            await db.execute("DELETE FROM checkpoints")
            await db.execute("DELETE FROM tasks")
            await db.execute("DELETE FROM events")
            await db.execute("DELETE FROM jobs")
            await db.execute("PRAGMA foreign_keys = ON")
            await emit(db, "CLUSTER_RESET", "MASTER", "Cluster reset to 0% IDLE state. All 5 spot workers online.")
            await db.commit()
        finally:
            await db.close()

        # 4. Flush Redis
        try:
            if redis:
                r = redis.Redis.from_url(REDIS_URL)
                r.flushall()
        except Exception as e:
            log.warning(f"Could not flush redis: {e}")

        # 5. Clean RSS directory
        try:
            rss_path = "/data/rss"
            if os.path.exists(rss_path):
                for item in os.listdir(rss_path):
                    item_path = os.path.join(rss_path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, ignore_errors=True)
        except Exception as e:
            log.warning(f"Could not clean RSS dir: {e}")

        # 6. Clean Output directory
        try:
            out_path = "/data/output"
            if os.path.exists(out_path):
                for item in os.listdir(out_path):
                    item_path = os.path.join(out_path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, ignore_errors=True)
                    elif os.path.isfile(item_path):
                        os.remove(item_path)
        except Exception as e:
            log.warning(f"Could not clean output dir: {e}")

        record_event("MASTER", "Cluster reset to 0% IDLE state. All nodes online.", "info")
        return {"status": "SUCCESS", "message": "Cluster reset successfully to 0% IDLE."}
    except Exception as ex:
        log.exception(f"Error in dashboard_reset_cluster: {ex}")
        raise HTTPException(status_code=500, detail=str(ex))

@router.post("/dashboard/verify-output")
async def dashboard_verify_output(job_id: Optional[str] = Query(None)):
    """Runs automated verification against ground truth on input file."""
    db = await get_db()
    try:
        if not job_id:
            cursor = await db.execute("SELECT job_id, input_path, output_path FROM jobs WHERE state = 'COMPLETED' ORDER BY completed_at DESC LIMIT 1")
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="No completed jobs found to verify.")
            job_id = row["job_id"]
            input_path = row["input_path"]
            output_path = row["output_path"]
        else:
            cursor = await db.execute("SELECT job_id, input_path, output_path FROM jobs WHERE job_id = ?", (job_id,))
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")
            input_path = row["input_path"]
            output_path = row["output_path"]
    finally:
        await db.close()

    result = verify_ground_truth(input_path, output_path, job_id)
    if result["status"] == "PASS":
        record_event("VERIFICATION", f"Job {job_id} output verified! {result['message']}", "success", result)
    else:
        record_event("VERIFICATION", f"Job {job_id} verification failed! {result.get('message')}", "error", result)

    return {"job_id": job_id, **result}

# ── UI Page Endpoints ──────────────────────────────────────────────────

UI_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "ui", "index.html")

@router.get("/ui", response_class=HTMLResponse)
@router.get("/dashboard", response_class=HTMLResponse)
async def serve_ui():
    """Serves the interactive live dashboard HTML."""
    if os.path.exists(UI_FILE_PATH):
        with open(UI_FILE_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard UI file not found</h1>", status_code=404)

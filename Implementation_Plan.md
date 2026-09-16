# Transient Spot-Instance MapReduce Framework
## Comprehensive Implementation Plan

**Project:** Transient Spot-Instance MapReduce Framework (ACC Mini Project)  
**Strategy:** Local Docker Compose → AWS Production  
**Simplification:** Single Master Orchestrator (no Raft consensus), SQLite for state  

> [!IMPORTANT]
> This document is the **single source of truth** for implementation. Every agent or developer following it should be able to reach a fully working system from scratch by executing these steps in order.

---

## TL;DR

**What are we building?** A simplified MapReduce framework that tolerates spot-instance preemptions — workers can be killed mid-task and a replacement picks up exactly where they left off, with zero data loss.

**Core idea in three steps:**
1. A single **Orchestrator** process tracks jobs and tasks in SQLite. Workers poll it for tasks, send heartbeats every 2 s, and commit byte-offset checkpoints to Redis.
2. Map workers push their output partitions to a **Remote Shuffle Service (RSS)** instead of writing to local disk — so the data survives even if the worker dies.
3. When a worker gets a `SIGTERM` (locally: `docker stop`; on AWS: the IMDSv2 termination notice), it flushes its in-memory buffers to RSS, commits a final checkpoint, and exits cleanly. The orchestrator re-queues the task and a replacement resumes from the last checkpoint offset.

**Two-phase rollout:**

| Phase | Where it runs | Key tools |
| :--- | :--- | :--- |
| **Phase 1 — Local** | Docker Compose on your laptop | Python + FastAPI + SQLite + Redis + MinIO |
| **Phase 2 — AWS** | EC2 Spot Fleet + managed services | S3 + ElastiCache + IMDSv2 sentinel |

**What is deliberately NOT built:**
- No Raft consensus — a single orchestrator process with SQLite WAL is sufficient
- No JVM / Netty — pure Python everywhere
- No custom binary protocol locally — NDJSON over HTTP (upgrade to binary for AWS)

**Estimated build time:** ~30 hours end-to-end (17 ordered steps in [§8](#8-implementation-order-for-a-single-developer--agent))  
**Estimated AWS cost:** ~$99/month (55% cheaper than all-On-Demand) for a 9-worker cluster running 8 h/day

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Simplification Contract](#2-simplification-contract)
3. [Full Tech Stack](#3-full-tech-stack)
4. [Phase 1 — Local Setup (Docker Compose)](#4-phase-1--local-setup-docker-compose)
   - 4.1 Component List
   - 4.2 Directory Structure
   - 4.3 Docker Compose Configuration
   - 4.4 Data Structures & SQLite Schema
   - 4.5 API Contract (Orchestrator & RSS)
5. [Phase 2 — Core Engine Implementation](#5-phase-2--core-engine-implementation)
   - 5.1 Worker Lifecycle
   - 5.2 Map Executor
   - 5.3 Checkpointing Logic
   - 5.4 Shuffle Partition Protocol & Wire Format
   - 5.5 Reduce Executor
   - 5.6 SIGTERM Handler & Emergency Drain
6. [Phase 3 — Testing & Fault Injection](#6-phase-3--testing--fault-injection)
   - 6.1 End-to-End Smoke Test
   - 6.2 Preemption Simulation Scripts
   - 6.3 Fault Injection Test Matrix
7. [Phase 4 — AWS Migration](#7-phase-4--aws-migration)
   - 7.1 Local → AWS Component Mapping
   - 7.2 VPC & Networking
   - 7.3 EC2 Spot Fleet Configuration
   - 7.4 IMDSv2 Preemption Sentinel (Full Code)
   - 7.5 S3 Integration (Replacing MinIO)
   - 7.6 ElastiCache Redis (Replacing Local Redis)
   - 7.7 Step-by-Step Migration Checklist
   - 7.8 Cost Estimate
8. [Implementation Order for a Single Developer / Agent](#8-implementation-order-for-a-single-developer--agent)

---

## 1. Architecture Overview

The framework has **three decoupled tiers**:

| Tier | Local Deployment | AWS Deployment | Core Job |
| :--- | :--- | :--- | :--- |
| **Control Plane** | `orchestrator` container (FastAPI + SQLite) | EC2 On-Demand `t3.medium` | Job tracking, task scheduling, heartbeat monitoring, stage barrier coordination |
| **Compute Plane** | `worker` containers (scalable via Docker Compose `--scale`) | EC2 Spot Fleet (multi-AZ, multi-family) | Execute Map / Reduce tasks, checkpoint to store, handle preemption gracefully |
| **Storage Plane** | `rss` container (FastAPI + local volume) + local filesystem | EC2 On-Demand `r6i.2xlarge` RSS nodes + S3 + ElastiCache Redis | Store shuffle partitions durably, serve them to reducers, hold checkpoint offsets |

### Data Flow in One Sentence

> A job is submitted → orchestrator splits input into N byte ranges → workers claim Map tasks and push key-value partitions to the RSS → orchestrator detects all maps done → workers claim Reduce tasks, fetch their partition stream from RSS, merge-sort, apply reduce fn, write output to disk.

### Fault Tolerance in One Sentence

> Workers install a SIGTERM handler; on `docker stop` (or real AWS preemption notice), they flush in-memory buffers to RSS, commit their current byte offset to the checkpoint store, notify the orchestrator, and exit cleanly — the orchestrator re-queues the task so a replacement worker resumes from the last checkpoint with zero data loss.

---

## 2. Simplification Contract

The following full-design features are deliberately simplified for the student implementation:

| Full Design Feature | Simplified Replacement | Equivalent Guarantee |
| :--- | :--- | :--- |
| 3-node Raft consensus quorum | Single `orchestrator` process + **SQLite with WAL mode** | Orchestrator can crash and restart; WAL ensures no committed state is lost |
| gRPC heartbeats (mTLS, bidirectional streaming) | HTTP POST `/heartbeat` every 2 seconds | Same liveness semantics — lease expires if no heartbeat in 5 s |
| Netty/JVM RSS with sendfile64 | Python FastAPI RSS server + Docker volume filesystem | Sufficient for local demo; filesystem = "NVMe tier" |
| IMDSv2 polling (cloud metadata) | SIGTERM handler inside worker container | `docker stop` sends SIGTERM → identical drain sequence |
| Off-heap Jemalloc partition slabs | Python `bytearray` in-memory buffers (per partition) | Same logical structure, no JVM or native memory needed |
| S3 / GCS overflow spill | Local Docker volume `/data/rss/` | Volume persists across container restarts |
| Fencing tokens `(RaftTerm << 32) \| CommitIndex` | Integer `attempt_id` column in SQLite `tasks` table | Monotonically increasing — same zombie-writer protection |
| gRPC preemption notice to master | HTTP POST `/tasks/{id}/drain` | Same semantics over REST |

---

## 3. Full Tech Stack

| Tool | Version | Component | Purpose |
| :--- | :--- | :--- | :--- |
| Python | 3.12 | All services | Primary implementation language |
| FastAPI | 0.111.0 | `orchestrator`, `rss` | REST API framework |
| Uvicorn | 0.29.0 | `orchestrator`, `rss` | ASGI server |
| aiosqlite | 0.20.0 | `orchestrator` | Async SQLite driver |
| SQLite (WAL mode) | 3.45+ | `orchestrator` | Persistent state store — replaces Raft |
| Redis (local phase) | 7.2 | `orchestrator`, `worker` | Checkpoint offset CAS store (optional alongside SQLite) |
| requests | 2.32.0 | `worker`, `job-client` | HTTP client for REST calls |
| prometheus-client | 0.20.0 | All services | `/metrics` exposition |
| pydantic | 2.7.0 | `orchestrator`, `rss` | Request/response validation |
| click | 8.1.7 | `job-client` | CLI argument parsing |
| xxhash | 3.4.1 | `worker` | Fast deterministic key partitioning |
| crc32c | 2.4 | `worker`, `rss` | Chunk integrity checksums |
| Docker Engine | 26.x | Infrastructure | Container runtime |
| Docker Compose | v2.27.x | Infrastructure | Multi-service orchestration |
| Prometheus | 2.51.2 | Monitoring | Time-series metrics |
| Grafana | 10.4.2 | Monitoring | Dashboard visualization |
| MinIO | latest | Local object storage | S3-compatible store for local phase |
| pytest | 8.2.0 | Testing | Unit and integration tests |
| boto3 | 1.34.x | AWS phase | AWS SDK — S3, EC2, ElastiCache |

---

## 4. Phase 1 — Local Setup (Docker Compose)

### 4.1 Component List

| # | Service Name | Framework | Port (internal → host) | Role |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `orchestrator` | Python 3.12 + FastAPI + SQLite | 8000 → 8000 | Single Master: job tracking, task assignment, heartbeat monitoring, stage transitions |
| 2 | `rss` | Python 3.12 + FastAPI | 8001 → 8001 | Remote Shuffle Service: receives map output partitions, serves them to reducers |
| 3 | `worker` (×N) | Python 3.12 (no web server) | — | Spot worker: runs map or reduce tasks, sends heartbeats, handles SIGTERM graceful drain |
| 4 | `redis` | Redis 7.2 | 6379 → 6379 | Checkpoint offset store (CAS via Lua scripts) |
| 5 | `minio` | MinIO | 9000 → 9000 | S3-compatible object storage for input/output (AWS phase: replace with real S3) |
| 6 | `job-client` | Python 3.12 + click | — | One-shot CLI container that submits a job and exits |
| 7 | `prometheus` | Prometheus 2.51.2 | 9090 → 9090 | Scrapes `/metrics` from all services |
| 8 | `grafana` | Grafana 10.4.2 | 3000 → 3000 | Visualizes job progress and preemption events |

### 4.2 Directory Structure

```
spot-mapreduce/
│
├── docker-compose.yml
├── .env                               # WORKER_COUNT=4, CHECKPOINT_INTERVAL=10, etc.
├── README.md
│
├── orchestrator/
│   ├── Dockerfile
│   ├── requirements.txt               # fastapi, uvicorn, aiosqlite, prometheus-client, pydantic
│   ├── main.py                        # FastAPI app, startup event (DB init, background threads)
│   ├── api/
│   │   ├── jobs.py                    # POST /jobs, GET /jobs/{job_id}
│   │   ├── tasks.py                   # GET /tasks/poll, POST /tasks/{id}/complete|checkpoint|drain|fail
│   │   └── heartbeat.py               # POST /heartbeat
│   ├── core/
│   │   ├── database.py                # SQLite connection pool, schema CREATE, WAL PRAGMA
│   │   ├── scheduler.py               # Input split calculation, task assignment logic
│   │   ├── lease_monitor.py           # Background thread: expire leases, re-queue failed tasks
│   │   └── barrier_monitor.py         # Background thread: Map→Shuffle→Reduce stage transitions
│   ├── models/
│   │   ├── job.py                     # Pydantic: JobSubmitRequest, JobResponse
│   │   └── task.py                    # Pydantic: TaskResponse, CheckpointRequest, HeartbeatRequest
│   └── metrics.py                     # Prometheus counters/gauges
│
├── rss/
│   ├── Dockerfile
│   ├── requirements.txt               # fastapi, uvicorn, prometheus-client, pydantic
│   ├── main.py
│   ├── api/
│   │   ├── partitions.py              # POST /partitions/{job_id}/{part_id}
│   │   │                              # GET  /partitions/{job_id}/{part_id}/stream
│   │   │                              # GET  /partitions/{job_id}/manifest
│   │   │                              # DELETE /partitions/{job_id}
│   │   └── health.py
│   └── core/
│       └── storage.py                 # File I/O: write chunks, read manifest, stream partitions
│
├── worker/
│   ├── Dockerfile
│   ├── requirements.txt               # requests, prometheus-client, xxhash, crc32c, redis
│   ├── worker.py                      # Main entrypoint: config parse, register, poll loop
│   ├── executor/
│   │   ├── map_executor.py            # Map task loop with micro-batch checkpointing
│   │   └── reduce_executor.py         # Reduce task loop: fetch RSS, k-way merge, reduce fn
│   ├── sentinel/
│   │   └── preemption_handler.py      # SIGTERM/SIGUSR1 handler + drain pipeline
│   ├── checkpoint/
│   │   └── store.py                   # Redis CAS checkpoint commit/read
│   ├── rss_client/
│   │   └── client.py                  # HTTP client for pushing chunks to RSS
│   └── jobs/
│       ├── wordcount.py               # map_fn + reduce_fn for WordCount (first test job)
│       └── terasort.py                # map_fn + reduce_fn stub for TeraSort
│
├── job-client/
│   ├── Dockerfile
│   ├── requirements.txt               # requests, click
│   ├── client.py                      # CLI: submit, status, cancel
│   └── jobs/
│       ├── wordcount_job.json         # Job definition (used by client.py)
│       └── sample_input.txt           # 10 MB sample text (WordCount input)
│
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
│       ├── datasource.yml
│       └── dashboards/
│           └── spot-mapreduce.json
│
├── scripts/
│   ├── simulate_preemption.sh         # docker stop <worker> after delay
│   ├── spot_storm.sh                  # Kill 40% of workers simultaneously
│   ├── crash_worker.sh                # docker kill (no warning — SIGKILL)
│   └── verify_output.sh               # Word count correctness check
│
└── data/                              # Host-mounted volume root (git-ignored)
    ├── orchestrator/state.db          # SQLite DB
    ├── rss/{job_id}/{partition_id}/
    │   ├── chunks/chunk-XXXX.ndjson
    │   └── manifest.json
    ├── input/sample_input.txt
    └── output/{job_id}/
        ├── part-00000.txt
        └── _SUCCESS
```

### 4.3 Docker Compose Configuration

```yaml
# docker-compose.yml
version: "3.9"

networks:
  mapreduce-net:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/24

volumes:
  orchestrator-data:
  rss-data:
  input-data:
  output-data:
  redis-data:
  minio-data:

services:

  orchestrator:
    build: ./orchestrator
    container_name: orchestrator
    hostname: orchestrator
    ports: ["8000:8000"]
    volumes:
      - orchestrator-data:/data/orchestrator
      - input-data:/data/input:ro
      - output-data:/data/output
    environment:
      - DB_PATH=/data/orchestrator/state.db
      - HEARTBEAT_TIMEOUT_SECONDS=5
      - LEASE_CHECK_INTERVAL_SECONDS=1
      - LOG_LEVEL=INFO
    networks:
      mapreduce-net:
        ipv4_address: 172.20.0.2
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 5s
      timeout: 3s
      retries: 5
    restart: on-failure   # Simulates "can crash and restart from WAL state"

  rss:
    build: ./rss
    container_name: rss
    hostname: rss
    ports: ["8001:8001"]
    volumes:
      - rss-data:/data/rss
    environment:
      - RSS_DATA_PATH=/data/rss
      - LOG_LEVEL=INFO
    networks:
      mapreduce-net:
        ipv4_address: 172.20.0.3
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8001/health"]
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped

  redis:
    image: redis:7.2-alpine
    container_name: redis
    hostname: redis
    ports: ["6379:6379"]
    volumes:
      - redis-data:/data
    command: ["redis-server", "--appendonly", "yes", "--save", "60", "1"]
    networks:
      mapreduce-net:
        ipv4_address: 172.20.0.4
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 2s
      retries: 5
    restart: unless-stopped

  minio:
    image: minio/minio:latest
    container_name: minio
    hostname: minio
    ports: ["9000:9000", "9001:9001"]
    volumes:
      - minio-data:/data
    environment:
      - MINIO_ROOT_USER=minioadmin
      - MINIO_ROOT_PASSWORD=minioadmin
    command: server /data --console-address ":9001"
    networks:
      mapreduce-net:
        ipv4_address: 172.20.0.5
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  worker:
    build: ./worker
    deploy:
      replicas: 4
      restart_policy:
        condition: on-failure
        delay: 2s
        max_attempts: 10
    volumes:
      - input-data:/data/input:ro
      - output-data:/data/output
    environment:
      - ORCHESTRATOR_URL=http://orchestrator:8000
      - RSS_URL=http://rss:8001
      - REDIS_URL=redis://redis:6379/0
      - HEARTBEAT_INTERVAL_SECONDS=2
      - CHECKPOINT_INTERVAL_SECONDS=10    # T_opt ≈ 19s from Young-Daly; use 10s locally
      - CHECKPOINT_N_RECORDS=1000
      - LOG_LEVEL=INFO
    networks:
      - mapreduce-net
    depends_on:
      orchestrator:
        condition: service_healthy
      rss:
        condition: service_healthy
      redis:
        condition: service_healthy
    stop_grace_period: 30s   # Docker waits 30s before SIGKILL → worker has 30s to drain
    stop_signal: SIGTERM     # Simulates spot preemption warning

  job-client:
    build: ./job-client
    container_name: job-client
    volumes:
      - input-data:/data/input
    environment:
      - ORCHESTRATOR_URL=http://orchestrator:8000
    networks:
      - mapreduce-net
    depends_on:
      orchestrator:
        condition: service_healthy
    restart: "no"

  prometheus:
    image: prom/prometheus:v2.51.2
    container_name: prometheus
    ports: ["9090:9090"]
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    networks:
      - mapreduce-net
    restart: unless-stopped

  grafana:
    image: grafana/grafana:10.4.2
    container_name: grafana
    ports: ["3000:3000"]
    volumes:
      - ./monitoring/grafana/datasource.yml:/etc/grafana/provisioning/datasources/ds.yml:ro
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards:ro
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    networks:
      - mapreduce-net
    depends_on: [prometheus]
    restart: unless-stopped
```

### 4.4 Data Structures & SQLite Schema

```sql
-- orchestrator/core/database.py — run once on startup

PRAGMA journal_mode = WAL;      -- Write-Ahead Logging: crash-safe, concurrent reads
PRAGMA synchronous = NORMAL;    -- Balanced durability/performance
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING',
    -- PENDING | MAP_RUNNING | MAP_COMPLETE | SHUFFLING
    -- | REDUCE_QUEUED | REDUCE_RUNNING | COMPLETED | FAILED
    map_fn          TEXT NOT NULL,       -- "jobs.wordcount.map_fn"
    reduce_fn       TEXT NOT NULL,       -- "jobs.wordcount.reduce_fn"
    input_path      TEXT NOT NULL,       -- "/data/input/sample_input.txt"
    output_path     TEXT NOT NULL,       -- "/data/output/"
    num_mappers     INTEGER NOT NULL,
    num_reducers    INTEGER NOT NULL,
    submitted_at    TEXT NOT NULL,       -- ISO8601 UTC
    started_at      TEXT,
    completed_at    TEXT,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id             TEXT PRIMARY KEY,
    job_id              TEXT NOT NULL REFERENCES jobs(job_id),
    task_type           TEXT NOT NULL,   -- "MAP" | "REDUCE"
    state               TEXT NOT NULL DEFAULT 'QUEUED',
    -- QUEUED | RUNNING | DRAINED | COMPLETED | FAILED
    split_start         INTEGER,         -- byte offset start (MAP only)
    split_end           INTEGER,         -- byte offset end (MAP only)
    partition_id        INTEGER,         -- reducer index (REDUCE only)
    assigned_worker     TEXT,
    attempt_id          INTEGER NOT NULL DEFAULT 0,  -- fencing token
    lease_deadline      TEXT,            -- ISO8601; heartbeat must arrive before this
    last_checkpoint_offset   INTEGER DEFAULT 0,
    last_checkpoint_seq      INTEGER DEFAULT 0,
    last_checkpoint_at       TEXT,
    created_at          TEXT NOT NULL,
    started_at          TEXT,
    completed_at        TEXT,
    error_message       TEXT
);

CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id       TEXT PRIMARY KEY,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id),
    job_id              TEXT NOT NULL,
    attempt_id          INTEGER NOT NULL,
    byte_offset         INTEGER NOT NULL,
    records_processed   INTEGER NOT NULL,
    partition_manifest  TEXT,            -- JSON: {"0": 14, "1": 9, ...}
    is_final_drain      BOOLEAN DEFAULT FALSE,
    created_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_state_type ON tasks(state, task_type);
CREATE INDEX IF NOT EXISTS idx_tasks_job ON tasks(job_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_task ON checkpoints(task_id);
```

**RSS Partition Manifest** (`/data/rss/{job_id}/{partition_id}/manifest.json`):

```json
{
  "job_id": "job-550e8400",
  "partition_id": 2,
  "chunks": [
    {
      "chunk_id": "chunk-0001",
      "filename": "chunk-0001.ndjson",
      "source_task_id": "task-map-001",
      "attempt_id": 0,
      "record_count": 312,
      "byte_size": 8192,
      "checksum_md5": "d41d8cd98f00b204e9800998ecf8427e",
      "committed_at": "2026-09-16T09:52:10Z"
    }
  ],
  "total_records": 312,
  "is_sealed": false
}
```

**RSS Chunk Format** — each line in `chunk-XXXX.ndjson` is one key-value pair:

```
{"key": "the", "value": 1}
{"key": "quick", "value": 1}
{"key": "the", "value": 1}
```

### 4.5 API Contract

#### Orchestrator (Port 8000)

| Method | Path | Request Body | Response | Description |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/jobs` | `JobSubmitRequest` | `202 JobResponse` | Submit new MapReduce job |
| `GET` | `/jobs/{job_id}` | — | `200 JobStatusResponse` | Poll job status |
| `GET` | `/tasks/poll?worker_id=X` | — | `200 TaskResponse` or `204` | Worker claims next available task |
| `POST` | `/heartbeat` | `HeartbeatRequest` | `200 {status, continue}` or `409 FENCED` | Worker liveness ping |
| `POST` | `/tasks/{id}/checkpoint` | `CheckpointRequest` | `200` or `409 FENCED` | Commit progress checkpoint |
| `POST` | `/tasks/{id}/complete` | `CompleteRequest` | `200` | Report task finished |
| `POST` | `/tasks/{id}/drain` | `DrainRequest` | `200` | Clean SIGTERM drain complete |
| `POST` | `/tasks/{id}/fail` | `FailRequest` | `200 {will_retry, new_attempt_id}` | Report unrecoverable error |
| `GET` | `/health` | — | `200` | Liveness check |
| `GET` | `/metrics` | — | Prometheus text | Metrics exposition |

**Key request/response shapes:**

```python
# models/job.py
class JobSubmitRequest(BaseModel):
    name: str
    map_fn: str           # "jobs.wordcount.map_fn"
    reduce_fn: str        # "jobs.wordcount.reduce_fn"
    input_path: str       # "/data/input/sample_input.txt"
    output_path: str      # "/data/output/"
    num_mappers: int = 4
    num_reducers: int = 4

# models/task.py
class TaskResponse(BaseModel):
    task_id: str
    job_id: str
    task_type: Literal["MAP", "REDUCE"]
    attempt_id: int
    split_start: Optional[int] = None   # MAP only
    split_end: Optional[int] = None     # MAP only
    input_path: Optional[str] = None    # MAP only
    map_fn: Optional[str] = None        # MAP only
    partition_id: Optional[int] = None  # REDUCE only
    reduce_fn: Optional[str] = None     # REDUCE only
    num_reducers: int
    rss_url: str
    resume_from_offset: int = 0         # >0 if resuming from checkpoint
    resume_from_seq: int = 0

class HeartbeatRequest(BaseModel):
    task_id: str
    worker_id: str
    attempt_id: int                     # Fencing: 409 if stale
    records_processed: int
    timestamp: str

class CheckpointRequest(BaseModel):
    task_id: str
    job_id: str
    attempt_id: int
    byte_offset: int
    records_processed: int
    partition_manifest: Optional[dict[str, int]] = None
    is_final_drain: bool = False
```

#### RSS — Remote Shuffle Service (Port 8001)

| Method | Path | Description |
| :--- | :--- | :--- |
| `POST` | `/partitions/{job_id}/{partition_id}?task_id=X&attempt_id=Y&chunk_seq=Z` | Mapper pushes a chunk (NDJSON body) |
| `GET` | `/partitions/{job_id}/{partition_id}/stream` | Reducer streams all chunks for this partition |
| `GET` | `/partitions/{job_id}/manifest` | Orchestrator queries partition completeness |
| `DELETE` | `/partitions/{job_id}` | Cleanup after job completion |
| `GET` | `/health` | Liveness check |
| `GET` | `/metrics` | Prometheus metrics |

---

## 5. Phase 2 — Core Engine Implementation

### 5.1 Worker Lifecycle

```
WORKER BOOT
  │
  ├─► Parse environment: ORCHESTRATOR_URL, RSS_URL, REDIS_URL
  ├─► Generate worker_id = f"worker-{uuid4().hex[:8]}"
  ├─► Connect to Redis (verify reachability)
  ├─► Install signal handlers: SIGTERM → graceful_shutdown, SIGUSR1 → preemption_warning
  ├─► Start heartbeat thread (background, every 2s)
  │
  └─► POLL LOOP:
        while not shutdown:
          response = GET /tasks/poll?worker_id=...
          if 204 No Content → sleep(1s), continue
          if 200 → task received
            if task.task_type == "MAP"    → run_map_task(task)
            if task.task_type == "REDUCE" → run_reduce_task(task)
```

### 5.2 Map Executor

```python
# worker/executor/map_executor.py

import time, importlib, xxhash, logging
from worker.checkpoint.store import commit_checkpoint, load_checkpoint
from worker.rss_client.client import RSSClient

SYNC_MAGIC = b'\xFF\x53\x59\x4E'   # Envelope alignment marker
T_OPT_SECONDS = 10                  # Local: 10s. AWS: compute dynamically via Young-Daly

def run_map_task(task: dict, state, rss: RSSClient, redis_client):
    # ── 1. Load user function ──────────────────────────────────────
    module_path, fn_name = task["map_fn"].rsplit(".", 1)
    map_fn = getattr(importlib.import_module(module_path), fn_name)

    # ── 2. Recovery or fresh start ─────────────────────────────────
    chk = load_checkpoint(redis_client, task["job_id"], task["task_id"])
    resume_offset = chk.byte_offset if chk else task["split_start"]
    resume_seq    = chk.record_id   if chk else -1
    ckpt_seq      = chk.seq         if chk else 0

    # ── 3. Init partition buffers ──────────────────────────────────
    n = task["num_reducers"]
    buffers:  dict[int, list] = {pid: [] for pid in range(n)}
    counters: dict[int, int]  = {pid: (chk.manifest.get(pid, 0) if chk else 0)
                                  for pid in range(n)}

    # ── 4. Open input at resume_offset ────────────────────────────
    last_ckpt_time = time.monotonic()
    record_id = resume_seq

    with open(task["input_path"], "rb") as f:
        f.seek(resume_offset)
        byte_pos = resume_offset

        for raw_line in f:
            # ── Preemption check (first thing in loop) ────────────
            if state.is_preempting:
                _flush(buffers, counters, task, state, rss)
                commit_checkpoint(redis_client, task, state, byte_pos,
                                  record_id, ckpt_seq + 1, counters,
                                  is_final_drain=True)
                _notify_drain(task, byte_pos, record_id)
                return

            # ── Skip already-processed records on recovery ────────
            if record_id <= resume_seq and byte_pos <= resume_offset:
                byte_pos += len(raw_line)
                continue

            byte_pos += len(raw_line)
            line = raw_line.decode("utf-8").strip()
            if not line:
                continue

            # ── Apply user map function ───────────────────────────
            for key, value in map_fn(line):
                pid = xxhash.xxh32(key.encode()).intdigest() % n
                buffers[pid].append({"key": key, "value": value})
                if len(buffers[pid]) >= 500:          # flush slab at 500 records
                    _flush_partition(pid, buffers, counters, task, state, rss)
            record_id += 1

            # ── Checkpoint boundary ───────────────────────────────
            if time.monotonic() - last_ckpt_time >= T_OPT_SECONDS:
                _flush(buffers, counters, task, state, rss)
                ckpt_seq += 1
                commit_checkpoint(redis_client, task, state, byte_pos,
                                  record_id, ckpt_seq, counters)
                last_ckpt_time = time.monotonic()

            if byte_pos >= task["split_end"]:
                break

    # ── 5. Final flush & complete ──────────────────────────────────
    _flush(buffers, counters, task, state, rss)
    _report_complete(task, byte_pos, record_id, counters)


def _flush(buffers, counters, task, state, rss):
    for pid in range(task["num_reducers"]):
        if buffers[pid]:
            _flush_partition(pid, buffers, counters, task, state, rss)

def _flush_partition(pid, buffers, counters, task, state, rss):
    import json
    payload = "\n".join(json.dumps(r) for r in buffers[pid]) + "\n"
    rss.push_chunk(
        job_id=task["job_id"], partition_id=pid,
        task_id=task["task_id"], attempt_id=task["attempt_id"],
        chunk_seq=counters[pid], payload=payload.encode()
    )
    counters[pid] += 1
    buffers[pid].clear()
```

### 5.3 Checkpointing Logic

**Redis Key Schema:**

| Key | Type | Value |
| :--- | :--- | :--- |
| `smr:{job_id}:{task_id}:{attempt_id}:offset` | STRING | Byte position in input file |
| `smr:{job_id}:{task_id}:{attempt_id}:record_id` | STRING | Last fully committed record ID |
| `smr:{job_id}:{task_id}:{attempt_id}:seq` | STRING | Checkpoint sequence number |
| `smr:{job_id}:{task_id}:{attempt_id}:status` | STRING | `RUNNING` \| `SEALED` \| `COMPLETE` |
| `smr:{job_id}:{task_id}:{attempt_id}:fencing_token` | STRING | `attempt_id` integer |
| `smr:{job_id}:{task_id}:{attempt_id}:manifest` | HASH | `{partition_id: chunks_acked}` |
| `smr:{job_id}:{task_id}:latest_attempt` | STRING | Highest valid `attempt_id` |

**Checkpoint commit — atomic Lua CAS:**

```python
# worker/checkpoint/store.py
import redis, time, logging

log = logging.getLogger(__name__)

CHECKPOINT_LUA = """
local pfx = KEYS[1]
local cur_token = redis.call('GET', pfx .. ':fencing_token')
if cur_token and tonumber(cur_token) > tonumber(ARGV[1]) then
    return {err='STALE_FENCING_TOKEN'}
end
redis.call('MSET',
    pfx .. ':offset',        ARGV[2],
    pfx .. ':record_id',     ARGV[3],
    pfx .. ':seq',           ARGV[4],
    pfx .. ':status',        ARGV[5],
    pfx .. ':fencing_token', ARGV[1]
)
if tonumber(ARGV[6]) > 0 then
    -- manifest is passed as alternating field/value pairs starting at ARGV[7]
    redis.call('DEL', pfx .. ':manifest')
    for i = 7, #ARGV, 2 do
        redis.call('HSET', pfx .. ':manifest', ARGV[i], ARGV[i+1])
    end
end
redis.call('SET', KEYS[2], ARGV[1])   -- update latest_attempt
for _, k in ipairs({':offset',':record_id',':seq',':status',':fencing_token',':manifest'}) do
    redis.call('EXPIRE', pfx .. k, 604800)  -- 7 days TTL
end
return 'OK'
"""

def commit_checkpoint(r: redis.Redis, task: dict, state, byte_offset: int,
                      record_id: int, seq: int, manifest: dict,
                      status: str = "RUNNING", is_final_drain: bool = False):
    if is_final_drain:
        status = "SEALED"
    pfx  = f"smr:{task['job_id']}:{task['task_id']}:{task['attempt_id']}"
    lkey = f"smr:{task['job_id']}:{task['task_id']}:latest_attempt"
    mf_args = []
    for k, v in manifest.items():
        mf_args += [str(k), str(v)]
    args = [
        str(task["attempt_id"]),
        str(byte_offset), str(record_id), str(seq), status,
        str(len(manifest))
    ] + mf_args
    result = r.eval(CHECKPOINT_LUA, 2, pfx, lkey, *args)
    if result != b"OK" and result != "OK":
        raise RuntimeError(f"Checkpoint rejected: {result}")
    log.info(f"Checkpoint committed seq={seq} offset={byte_offset} status={status}")

def load_checkpoint(r: redis.Redis, job_id: str, task_id: str):
    latest = r.get(f"smr:{job_id}:{task_id}:latest_attempt")
    if latest is None:
        return None
    att = int(latest)
    pfx = f"smr:{job_id}:{task_id}:{att}"
    vals = r.mget(f"{pfx}:offset", f"{pfx}:record_id",
                  f"{pfx}:seq",    f"{pfx}:status",    f"{pfx}:fencing_token")
    if vals[0] is None:
        return None
    manifest = {int(k): int(v) for k, v in r.hgetall(f"{pfx}:manifest").items()}
    from dataclasses import dataclass
    @dataclass
    class Ckpt:
        byte_offset: int; record_id: int; seq: int
        status: str; fencing_token: int; manifest: dict
    return Ckpt(int(vals[0]), int(vals[1]), int(vals[2]),
                vals[3].decode(), int(vals[4]), manifest)
```

### 5.4 Shuffle Partition Protocol & Wire Format

The local phase uses **NDJSON over HTTP** (simple to implement, easy to debug). The AWS phase can upgrade to the binary protocol below for performance.

**Binary Chunk Frame (32-byte header) — for AWS / performance:**

```
Byte  0– 3:  Magic        0x52535331  ("RSS1")
Byte  4– 7:  Job ID hash  uint32
Byte  8–11:  Shuffle ID   uint32
Byte 12–13:  Partition ID uint16
Byte 14–15:  Attempt ID   uint16
Byte 16–19:  Chunk Seq    uint32
Byte 20–23:  Payload len  uint32
Byte 24–27:  CRC32C       uint32  (over bytes 0–23 + payload)
Byte 28–35:  Fencing token uint64
Byte 36+  :  Payload      [KV records]
```

Each KV record within the payload:

```
[4B key_len][key bytes][4B val_len][val bytes][4B crc32c]
```

**Partition Index File** (`part-{id}.index`) — 48 bytes per record:

```
[0:4]   chunk_seq      uint32
[4:8]   partition_id   uint32
[8:16]  data_offset    uint64  (byte offset in .data file)
[16:24] data_length    uint64
[24:28] record_count   uint32
[28:32] attempt_id     uint32
[32:40] first_key_hash uint64  (xxh64 of first key)
[40:44] last_key_hash  uint32
[44:48] crc32c         uint32  (over bytes 0–44)
```

**Key hashing for partition assignment:**

```python
import xxhash

def get_partition_id(key: str, n_reducers: int) -> int:
    return xxhash.xxh32(key.encode("utf-8")).intdigest() % n_reducers
```

### 5.5 Reduce Executor

```python
# worker/executor/reduce_executor.py
import heapq, itertools, json, importlib, logging, requests, time

def run_reduce_task(task: dict, state, orchestrator_url: str):
    # ── 1. Load user reduce function ──────────────────────────────
    module_path, fn_name = task["reduce_fn"].rsplit(".", 1)
    reduce_fn = getattr(importlib.import_module(module_path), fn_name)

    # ── 2. Fetch all partition chunks from RSS ─────────────────────
    rss_url = task["rss_url"]
    partition_id = task["partition_id"]
    job_id = task["job_id"]

    all_records = []  # Collect all (key, value) pairs for this partition
    stream_url = f"{rss_url}/partitions/{job_id}/{partition_id}/stream"
    with requests.get(stream_url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if line:
                record = json.loads(line)
                all_records.append((record["key"], record["value"]))

    # ── 3. Sort by key ─────────────────────────────────────────────
    all_records.sort(key=lambda x: x[0])

    # ── 4. Group by key and apply reduce fn ───────────────────────
    output_path = f"/data/output/{job_id}/part-{partition_id:05d}.txt"
    import os; os.makedirs(f"/data/output/{job_id}", exist_ok=True)

    with open(output_path, "w") as out:
        for key, group in itertools.groupby(all_records, key=lambda x: x[0]):
            if state.is_preempting:
                break                          # Drain handled by sentinel
            values = [v for _, v in group]
            result = reduce_fn(key, values)
            out.write(f"{key}\t{result}\n")

    # ── 5. Report complete ─────────────────────────────────────────
    requests.post(f"{orchestrator_url}/tasks/{task['task_id']}/complete",
                  json={"task_id": task["task_id"], "job_id": job_id,
                        "attempt_id": task["attempt_id"],
                        "final_byte_offset": 0, "final_records_processed": len(all_records)},
                  timeout=10)
```

### 5.6 SIGTERM Handler & Emergency Drain

```python
# worker/sentinel/preemption_handler.py
import signal, threading, logging, time, sys

log = logging.getLogger(__name__)

class PreemptionHandler:
    def __init__(self, state, rss_client, redis_client, orchestrator_url):
        self.state = state
        self.rss = rss_client
        self.redis = redis_client
        self.orch_url = orchestrator_url
        self._preempt_event = threading.Event()
        self._install()

    def _install(self):
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGUSR1, self._handle)   # AWS IMDSv2 sentinel path
        signal.signal(signal.SIGINT,  self._handle)
        t = threading.Thread(target=self._drain_monitor, daemon=True, name="drain-monitor")
        t.start()

    def _handle(self, signum, frame):
        # Signal handler must be fast — just set flags
        log.critical(f"[SENTINEL] Signal {signum} received — preemption starting")
        self.state.is_preempting = True
        self._preempt_event.set()

    def _drain_monitor(self):
        self._preempt_event.wait()   # Block until preemption signal
        t0 = time.monotonic()
        log.critical("[DRAIN] Emergency drain pipeline starting")

        import requests
        try:
            # Step 1: All in-memory buffers already being flushed by compute loop
            # (compute loop checks state.is_preempting and calls _flush before exiting)
            time.sleep(1.0)   # Give compute loop 1s to finish flushing

            # Step 2: Notify orchestrator of clean drain
            if self.state.current_task_id:
                requests.post(
                    f"{self.orch_url}/tasks/{self.state.current_task_id}/drain",
                    json={
                        "task_id":            self.state.current_task_id,
                        "job_id":             self.state.current_job_id,
                        "attempt_id":         self.state.current_attempt_id,
                        "drain_status":       "CLEANLY_MIGRATED",
                        "final_byte_offset":  self.state.last_byte_offset,
                        "final_records_processed": self.state.last_record_id,
                    },
                    timeout=5
                )

            elapsed = (time.monotonic() - t0) * 1000
            log.critical(f"[DRAIN] Complete in {elapsed:.0f}ms. Exiting.")
        except Exception as e:
            log.error(f"[DRAIN] Error during drain: {e}", exc_info=True)
        finally:
            sys.exit(0)
```

---

## 6. Phase 3 — Testing & Fault Injection

### 6.1 End-to-End Smoke Test

```bash
# 1. Start the full stack
docker compose up --build -d

# 2. Wait for health (all services healthy)
docker compose ps   # all should show "healthy"

# 3. Copy sample input
docker cp job-client/jobs/sample_input.txt orchestrator:/data/input/sample_input.txt

# 4. Submit a WordCount job
docker compose run --rm job-client python client.py submit \
  --name wordcount-run-1 \
  --map-fn jobs.wordcount.map_fn \
  --reduce-fn jobs.wordcount.reduce_fn \
  --input /data/input/sample_input.txt \
  --output /data/output/ \
  --num-mappers 4 \
  --num-reducers 4

# 5. Watch job progress
watch -n 2 "curl -s http://localhost:8000/jobs/job-XXXX | python -m json.tool"

# 6. When COMPLETED, verify output
docker exec orchestrator cat /data/output/job-XXXX/part-00000.txt | head -20

# 7. Open Grafana dashboard
open http://localhost:3000   # admin/admin
```

### 6.2 Preemption Simulation Scripts

```bash
#!/usr/bin/env bash
# scripts/simulate_preemption.sh
# Simulates AWS 120s Spot preemption (locally: 30s grace via Docker stop_grace_period)
WORKER=${1:-"spot-mapreduce-worker-1"}
GRACE=${2:-30}
echo "==> [SENTINEL] SIGTERM → ${WORKER} (grace=${GRACE}s)"
docker stop --time "${GRACE}" "${WORKER}"
echo "==> Done. Check orchestrator for task requeue."
```

```bash
#!/usr/bin/env bash
# scripts/spot_storm.sh
# Kills 40% of workers simultaneously (Spot Storm simulation)
WORKERS=$(docker ps --filter "name=spot-mapreduce-worker" --format "{{.Names}}")
TOTAL=$(echo "$WORKERS" | wc -l)
KILL_COUNT=$(( TOTAL * 40 / 100 ))
echo "$WORKERS" | shuf | head -n "${KILL_COUNT}" | while read -r W; do
    docker stop --time 10 "${W}" &
done
wait
echo "==> Storm complete. ${KILL_COUNT}/${TOTAL} workers stopped."
```

```bash
#!/usr/bin/env bash
# scripts/crash_worker.sh
# Simulates sudden hardware failure (SIGKILL, no drain possible)
WORKER=${1:-"spot-mapreduce-worker-1"}
echo "==> [CRASH] SIGKILL → ${WORKER} (no grace, no drain)"
docker kill "${WORKER}"
echo "==> Heartbeat timeout will trigger re-queue in ~5s"
```

### 6.3 Fault Injection Test Matrix

| Test ID | Scenario | How to Inject | Expected Behavior | Verify With |
| :--- | :--- | :--- | :--- | :--- |
| FI-01 | Graceful SIGTERM mid-map | `./scripts/simulate_preemption.sh worker-X` | Worker drains, checkpoints, orchestrator re-queues; new worker resumes from offset | Output has 100% key coverage |
| FI-02 | Sudden SIGKILL mid-map | `./scripts/crash_worker.sh worker-X` | Heartbeat expires in 5s, task re-queued from last checkpoint offset (≤10s of re-work) | Output correct; re-work bounded |
| FI-03 | Spot Storm (40% kill) | `./scripts/spot_storm.sh` | All tasks re-queued and claimed by surviving/restarted workers; job completes | Grafana: preemptions spike, then job completes |
| FI-04 | Orchestrator restart | `docker restart orchestrator` | SQLite WAL preserves all state; workers reconnect and continue heartbeating | No tasks lost; job completes |
| FI-05 | RSS restart | `docker restart rss` | Workers retry chunk pushes (exponential backoff); RSS reloads manifests from disk | No data loss; partitions intact |
| FI-06 | Redis restart | `docker restart redis` | Checkpoint commits retry with backoff; upon reconnect, workers continue | Checkpoint seq resumes correctly |
| FI-07 | Zombie duplicate writer | Two workers claim same task_id+attempt_id | Fencing token (attempt_id) in Redis rejects lower-attempt writes | Only latest attempt's data survives |
| FI-08 | SIGTERM mid-reduce | `./scripts/simulate_preemption.sh reducer-X` | Reducer commits output byte_offset to Redis; replacement resumes without re-fetching RSS | Final output files have no duplicate lines |

---

## 7. Phase 4 — AWS Migration

### 7.1 Local → AWS Component Mapping

| Local Component | AWS Replacement | Configuration |
| :--- | :--- | :--- |
| `orchestrator` Docker container | EC2 On-Demand `t3.medium` + Elastic IP | Auto Recovery enabled; 20 GB EBS gp3 for SQLite WAL |
| `rss` Docker container | EC2 On-Demand `r6i.2xlarge` (×2) | NVMe instance store for hot buffers; EBS gp3 for cold chunks |
| `redis` container | Amazon ElastiCache for Redis 7.x (`cache.r7g.large`) | Single primary + Multi-AZ replica; TLS + AUTH enabled |
| `minio` container | Amazon S3 Standard + S3 Express One Zone | S3 Standard for input/output; S3 Express for hot RSS overflow spill |
| `worker` containers | EC2 Spot Fleet (multi-AZ, multi-family) | `priceCapacityOptimized`; 20% On-Demand fallback |
| Docker bridge network | VPC (10.10.0.0/16) with private subnets per AZ | No public IPs on workers; NAT Gateway for outbound |
| Local Prometheus/Grafana | Amazon Managed Grafana + AMP | Native AWS observability |
| Docker SIGTERM simulation | IMDSv2 `spot/instance-action` polling + SIGUSR1 | 500ms polling; 120s grace window |

### 7.2 VPC & Networking

```
VPC: 10.10.0.0/16 (us-east-1)

Private Subnets (workers, no public IPs):
  10.10.1.0/24  → AZ us-east-1a  (c* instance family workers)
  10.10.2.0/24  → AZ us-east-1b  (m* instance family workers)
  10.10.3.0/24  → AZ us-east-1c  (r* instance family workers)

Master Subnets:
  10.10.11.0/28 → AZ us-east-1a  (Master Orchestrator — On-Demand)

RSS Subnets:
  10.10.21.0/28 → AZ us-east-1a  (RSS Node 0 — On-Demand)
  10.10.22.0/28 → AZ us-east-1b  (RSS Node 1 — On-Demand)

Public Subnet (NAT Gateway only):
  10.10.100.0/24 → AZ us-east-1a

VPC Endpoints (keep traffic off internet):
  - com.amazonaws.us-east-1.s3          (Gateway — free)
  - com.amazonaws.us-east-1.elasticache (Interface)
```

**Security Groups:**

| Group | Inbound | Outbound |
| :--- | :--- | :--- |
| Master SG | TCP 8000 from Worker SG (heartbeat/task); TCP 22 from Bastion | TCP 6379 to Redis SG; TCP 443 to VPC Endpoints |
| Worker SG | TCP 8000 to Master SG | TCP 8001 to RSS SG; TCP 6379 to Redis SG; TCP 443 to VPC Endpoints |
| RSS SG | TCP 8001 from Worker SG | TCP 443 to S3 VPC Endpoint |
| Redis SG | TCP 6379 from Worker SG + Master SG | — |

### 7.3 EC2 Spot Fleet Configuration

```json
{
  "SpotFleetRequestConfig": {
    "IamFleetRole": "arn:aws:iam::ACCOUNT_ID:role/AmazonEC2SpotFleetRole",
    "AllocationStrategy": "priceCapacityOptimized",
    "TargetCapacity": 9,
    "OnDemandTargetCapacity": 2,
    "SpotTargetCapacity": 7,
    "InstanceInterruptionBehavior": "terminate",
    "LaunchTemplateConfigs": [{
      "LaunchTemplateSpecification": {
        "LaunchTemplateName": "spot-mapreduce-worker-lt",
        "Version": "$Latest"
      },
      "Overrides": [
        {"InstanceType": "c5.xlarge",  "SubnetId": "subnet-AZ1", "Priority": 1},
        {"InstanceType": "c6i.xlarge", "SubnetId": "subnet-AZ1", "Priority": 2},
        {"InstanceType": "c7g.xlarge", "SubnetId": "subnet-AZ1", "Priority": 3},
        {"InstanceType": "m5.xlarge",  "SubnetId": "subnet-AZ2", "Priority": 1},
        {"InstanceType": "m6i.xlarge", "SubnetId": "subnet-AZ2", "Priority": 2},
        {"InstanceType": "r5.xlarge",  "SubnetId": "subnet-AZ3", "Priority": 1},
        {"InstanceType": "r6i.xlarge", "SubnetId": "subnet-AZ3", "Priority": 2}
      ]
    }]
  }
}
```

> [!NOTE]
> `priceCapacityOptimized` implements the pool scoring function $S_{\text{pool}}$ from the architecture docs — AWS chooses pools with both low price AND available capacity, reducing interruption probability.

**Worker Launch Template UserData bootstrap script:**

```bash
#!/bin/bash
yum update -y
pip3 install requests xxhash crc32c redis boto3 prometheus-client

# Pull worker image from ECR
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
docker pull ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/spot-mapreduce-worker:latest

# Start worker container + sentinel sidecar
MASTER_IP=$(aws ssm get-parameter --name /spot-mapreduce/master-ip --query Parameter.Value --output text)
WORKER_PID=$(docker run -d \
  -e ORCHESTRATOR_URL=http://${MASTER_IP}:8000 \
  -e RSS_URL=http://$(aws ssm get-parameter --name /spot-mapreduce/rss-ip --query Parameter.Value --output text):8001 \
  -e REDIS_URL=$(aws ssm get-parameter --name /spot-mapreduce/redis-url --query Parameter.Value --output text) \
  ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/spot-mapreduce-worker:latest)

# Start IMDSv2 sentinel sidecar
python3 /opt/spot_sentinel.py --master-host ${MASTER_IP} --master-port 8000 --worker-pid ${WORKER_PID} &
```

### 7.4 IMDSv2 Preemption Sentinel

```python
# spot_sentinel.py — runs as sidecar on every Spot Worker instance

import os, sys, signal, time, threading, logging, requests
log = logging.getLogger("sentinel")

IMDS_TOKEN_URL  = "http://169.254.169.254/latest/api/token"
IMDS_ACTION_URL = "http://169.254.169.254/latest/meta-data/spot/instance-action"
POLL_INTERVAL   = 0.5       # 500ms — as specified in architecture docs
TOKEN_TTL       = 21600     # 6 hours

class IMDSv2Sentinel:
    def __init__(self, master_host, master_port, worker_pid):
        self.master_host = master_host
        self.master_port = master_port
        self.worker_pid  = worker_pid
        self._token      = None
        self._token_exp  = 0
        self._stop       = threading.Event()
        self._triggered  = False

    def _get_token(self):
        if time.monotonic() >= self._token_exp - 60:
            r = requests.put(IMDS_TOKEN_URL,
                             headers={"X-aws-ec2-metadata-token-ttl-seconds": str(TOKEN_TTL)},
                             timeout=(0.1, 0.2))
            r.raise_for_status()
            self._token    = r.text.strip()
            self._token_exp = time.monotonic() + TOKEN_TTL
        return self._token

    def _check_notice(self):
        try:
            token = self._get_token()
            r = requests.get(IMDS_ACTION_URL,
                             headers={"X-aws-ec2-metadata-token": token},
                             timeout=(0.1, 0.2))
            if r.status_code == 200:
                return r.json()          # {"action": "terminate", "time": "..."}
            return None                  # 404 = normal (no notice)
        except Exception:
            return None

    def start(self):
        log.info(f"IMDSv2 Sentinel started — polling every {POLL_INTERVAL*1000:.0f}ms")
        while not self._stop.is_set() and not self._triggered:
            t = time.monotonic()
            notice = self._check_notice()
            if notice:
                self._triggered = True
                self._respond(notice)
                break
            self._stop.wait(max(0, POLL_INTERVAL - (time.monotonic() - t)))

    def _respond(self, notice):
        log.critical(f"SPOT TERMINATION NOTICE: {notice}")

        # Step 1: Local fast-path SIGUSR1 (<10ms)
        os.kill(self.worker_pid, signal.SIGUSR1)
        log.info("SIGUSR1 sent to worker")

        # Step 2: Notify master orchestrator (<50ms)
        try:
            requests.post(f"http://{self.master_host}:{self.master_port}/preemption",
                          json={"instance_id": _get_instance_id(), "remaining_secs": 120},
                          timeout=2)
        except Exception as e:
            log.warning(f"Could not notify master: {e}")

        log.critical("Drain pipeline complete. Worker has ~120s before AWS SIGKILL.")

def _get_instance_id():
    try:
        token = requests.put(IMDS_TOKEN_URL,
                             headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
                             timeout=(0.1, 0.2)).text
        return requests.get("http://169.254.169.254/latest/meta-data/instance-id",
                            headers={"X-aws-ec2-metadata-token": token},
                            timeout=(0.1, 0.2)).text
    except Exception:
        return "unknown"

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--master-host", required=True)
    p.add_argument("--master-port", type=int, default=8000)
    p.add_argument("--worker-pid",  type=int, required=True)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    IMDSv2Sentinel(args.master_host, args.master_port, args.worker_pid).start()
```

### 7.5 S3 Integration (Replacing MinIO)

```python
# In AWS phase, set these env vars:
# AWS_DEFAULT_REGION=us-east-1
# S3_BUCKET=spot-mapreduce-data
# S3_EXPRESS_BUCKET=spot-mapreduce-shuffle--use1-az4--x-s3

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

s3 = boto3.client("s3", region_name="us-east-1",
                  config=Config(retries={"max_attempts": 5, "mode": "adaptive"},
                                max_pool_connections=50))

TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,   # 64 MB
    multipart_chunksize=16 * 1024 * 1024,   # 16 MB parts
    max_concurrency=10,
    use_threads=True,
)

# S3 Bucket Structure:
# spot-mapreduce-data/
#   input/{job_id}/split-XXXXXX.txt
#   output/{job_id}/_temporary/{task_id}_{attempt_id}/part-NNNNN.txt
#   output/{job_id}/part-NNNNN.txt          ← final (after atomic copy)
#   output/{job_id}/_SUCCESS
#   rss-overflow/{job_id}/{partition_id}/part-{id}.data
#   rss-overflow/{job_id}/{partition_id}/part-{id}.index

def commit_task_output_atomic(job_id, task_id, attempt_id, part_number):
    """Exactly-Once Semantics: copy staging → final, then delete staging."""
    bucket  = os.environ["S3_BUCKET"]
    staging = f"output/{job_id}/_temporary/{task_id}_{attempt_id}/part-{part_number:05d}.txt"
    final   = f"output/{job_id}/part-{part_number:05d}.txt"
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": staging}, Key=final)
    s3.delete_object(Bucket=bucket, Key=staging)

def spill_to_s3_express(job_id, partition_id, chunk_seq, data_bytes, index_bytes):
    """Triggered when RSS NVMe > 75% utilization."""
    bucket = os.environ["S3_EXPRESS_BUCKET"]
    prefix = f"{job_id}/{partition_id}"
    s3express = boto3.client("s3",
                             endpoint_url="https://s3express-use1-az4.us-east-1.amazonaws.com")
    s3express.put_object(Bucket=bucket, Key=f"{prefix}/chunk-{chunk_seq:08d}.data", Body=data_bytes)
    s3express.put_object(Bucket=bucket, Key=f"{prefix}/chunk-{chunk_seq:08d}.index", Body=index_bytes)
```

### 7.6 ElastiCache Redis (Replacing Local Redis)

```python
# AWS phase Redis connection (TLS + AUTH, replacing local redis://redis:6379)
import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

redis_client = redis.Redis(
    host=os.environ["ELASTICACHE_ENDPOINT"],   # e.g. mapreduce-redis.abc123.use1.cache.amazonaws.com
    port=6379,
    password=os.environ["REDIS_AUTH_TOKEN"],
    ssl=True,
    ssl_cert_reqs="required",
    decode_responses=True,
    socket_connect_timeout=2,
    socket_timeout=1,
    retry_on_timeout=True,
    retry=Retry(ExponentialBackoff(base=0.1, cap=2.0), retries=5),
    health_check_interval=30,
)
# The checkpoint commit/load code (Section 5.3) works unchanged with this client.
```

**Create ElastiCache replication group (CLI):**

```bash
aws elasticache create-replication-group \
  --replication-group-id mapreduce-redis \
  --replication-group-description "Spot MapReduce offset store" \
  --num-cache-clusters 2 \
  --cache-node-type cache.r7g.large \
  --engine redis \
  --engine-version 7.1 \
  --cache-subnet-group-name mapreduce-redis-subnet-group \
  --security-group-ids sg-0redis... \
  --automatic-failover-enabled \
  --multi-az-enabled \
  --at-rest-encryption-enabled \
  --transit-encryption-enabled \
  --auth-token "$REDIS_AUTH_TOKEN"
```

### 7.7 Step-by-Step Migration Checklist

```
PRE-MIGRATION (while still running locally)
  ☐ All local phase tests passing (FI-01 through FI-08)
  ☐ WordCount job completes correctly with 4 workers + 2 preemptions
  ☐ Docker images built and pushed to ECR:
      docker build -t spot-mapreduce-orchestrator ./orchestrator
      docker build -t spot-mapreduce-rss         ./rss
      docker build -t spot-mapreduce-worker      ./worker
      docker tag  spot-mapreduce-orchestrator ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/...
      docker push ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/...

INFRASTRUCTURE PROVISIONING (AWS Console or Terraform)
  ☐ Create VPC (10.10.0.0/16) with subnets per AZ
  ☐ Create Internet Gateway + NAT Gateway (public subnet)
  ☐ Create Security Groups (master, worker, rss, redis)
  ☐ Create S3 bucket: spot-mapreduce-data (block public access, versioning on)
  ☐ Create S3 Express bucket: spot-mapreduce-shuffle--use1-az4--x-s3
  ☐ Create ElastiCache subnet group (all private subnets)
  ☐ Create ElastiCache replication group (Redis 7.1, r7g.large, Multi-AZ)
  ☐ Create IAM roles: SpotWorkerRole, MasterOrchestratorRole, SpotFleetRole
  ☐ Create SSM Parameters: /spot-mapreduce/master-ip, /spot-mapreduce/redis-url, etc.

ORCHESTRATOR DEPLOYMENT
  ☐ Launch t3.medium On-Demand in master subnet (us-east-1a)
  ☐ Attach Elastic IP
  ☐ Run orchestrator container (same image as local, env vars pointing to ElastiCache + S3)
  ☐ Verify: curl http://ELASTIC_IP:8000/health returns {"status": "healthy"}
  ☐ Update SSM Parameter: /spot-mapreduce/master-ip = ELASTIC_IP

RSS DEPLOYMENT
  ☐ Launch r6i.2xlarge On-Demand in RSS subnets (us-east-1a + us-east-1b)
  ☐ Mount NVMe instance store at /data/rss
  ☐ Run rss container (same image, RSS_DATA_PATH=/data/rss)
  ☐ Update SSM Parameter: /spot-mapreduce/rss-ip = RSS_PRIVATE_IP

SPOT FLEET LAUNCH
  ☐ Create Launch Template with IMDSv2 enforced, ECR image, UserData bootstrap
  ☐ Request Spot Fleet (9 instances, 2 On-Demand baseline, 7 Spot)
  ☐ Verify workers register with orchestrator: GET /health shows active_workers > 0

FIRST AWS JOB RUN
  ☐ Upload input data: aws s3 cp sample_input.txt s3://spot-mapreduce-data/input/run-1/
  ☐ Submit job via curl/CLI pointing to ELASTIC_IP:8000
  ☐ Watch Grafana (AMP): confirm tasks running, heartbeats flowing
  ☐ Simulate preemption: aws ec2 terminate-instances --instance-ids i-XXXX
  ☐ Verify: orchestrator re-queues task, replacement worker resumes from checkpoint
  ☐ Verify: output correct — aws s3 cp s3://spot-mapreduce-data/output/run-1/ . --recursive

VALIDATION
  ☐ Run full TeraSort on 100GB with 20% hourly preemption (chaos agent)
  ☐ Confirm cost savings vs On-Demand baseline (CloudWatch billing metrics)
  ☐ Confirm cascading recomputation count = 0 (Grafana dashboard)
```

### 7.8 Cost Estimate

Small cluster running 8 hours/day, 5 days/week:

| Component | Instance Type | Qty | On-Demand | Spot Price | Monthly Cost |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Master Orchestrator | `t3.medium` On-Demand | 1 | $0.0416/hr | N/A | ~$7.20 |
| RSS Nodes | `r6i.2xlarge` On-Demand | 2 | $0.504/hr | N/A | ~$40.32 |
| Workers (Spot) | `c5.xlarge` Spot | 7 | $0.192/hr | ~$0.040/hr | ~$11.20 |
| Workers (On-Demand fallback) | `c5.xlarge` | 2 | $0.192/hr | N/A | ~$15.36 |
| ElastiCache Redis | `cache.r7g.large` | 1+1 replica | — | — | ~$20.00 |
| S3 Storage & Requests | — | — | — | — | ~$5.00 |
| **Total** | | | **~$99.08/month** | | |
| **On-Demand baseline (all workers on-demand)** | `c5.xlarge` ×9 | | | | ~$220/month |
| **Savings** | | | **~55%** cost reduction | | |

> [!TIP]
> Increasing the Spot ratio to 90% (8 Spot + 1 On-Demand) and using Graviton3 `c7g.xlarge` workers (30% cheaper than x86) can push savings to **70%+**.

---

## 8. Implementation Order for a Single Developer / Agent

Follow these steps **in order**. Each step is independently testable before moving on.

| Step | What to Build | Time Est. | Test: Done When... |
| :--- | :--- | :--- | :--- |
| 1 | SQLite schema + orchestrator stub (`POST /jobs`, `GET /tasks/poll`, `GET /health`) | 2 h | `curl http://localhost:8000/health` returns `{"status":"healthy"}` |
| 2 | Worker poll loop (no execution yet) + heartbeat thread | 1 h | Worker registers, polls, gets 204 No Content, keeps heartbeating |
| 3 | RSS service (`POST /partitions`, `GET /partitions/stream`, `GET /health`) | 2 h | `curl -X POST /partitions/job1/0 -d '{"key":"a","value":1}'` works; GET streams it back |
| 4 | Map executor (without checkpointing or preemption yet) | 2 h | Worker picks up a Map task, processes a small file, pushes chunks to RSS, reports complete |
| 5 | WordCount `map_fn` + `reduce_fn` | 30 min | `map_fn("hello world")` → `[("hello",1),("world",1)]` |
| 6 | Checkpoint commit/load (Redis CAS Lua script) | 2 h | Kill worker mid-map; restart it; it resumes from last checkpoint offset |
| 7 | SIGTERM handler + drain pipeline | 2 h | `docker stop worker-X` → worker flushes to RSS, calls `/drain`, exits within 30s |
| 8 | Lease Monitor background thread in orchestrator | 1 h | `docker kill worker-X` → task re-queued in ~5s (heartbeat timeout) |
| 9 | Barrier Monitor + Reduce phase + reduce executor | 3 h | Full WordCount job completes end-to-end with 4 workers |
| 10 | Docker Compose full stack (all 8 services) | 1 h | `docker compose up` → all services healthy; `docker compose ps` shows all green |
| 11 | Chaos scripts (simulate_preemption.sh, spot_storm.sh, crash_worker.sh) | 1 h | FI-01, FI-02, FI-03 all pass; output is always correct |
| 12 | Prometheus metrics + Grafana dashboard | 1 h | Grafana shows live task count, preemption events, checkpoint latency |
| 13 | AWS phase: VPC + Security Groups + S3 bucket + ElastiCache | 3 h | All AWS resources provisioned; Redis reachable from within VPC |
| 14 | AWS phase: Swap MinIO → S3, local Redis → ElastiCache (env var only) | 1 h | Same Docker image, different env vars; orchestrator healthy on EC2 |
| 15 | IMDSv2 Sentinel sidecar deployment | 2 h | `aws ec2 terminate-instances` triggers drain; task re-queued; replacement resumes |
| 16 | Spot Fleet request + Launch Template with UserData bootstrap | 2 h | 7 Spot + 2 On-Demand workers register with orchestrator |
| 17 | Full AWS validation: TeraSort 10 GB with 20% preemption | 2 h | Job completes; zero cascading recomputation; output verified |

**Total estimated implementation time: ~30 hours**

---

*Implementation Plan generated by: Local Architecture Agent, Core Engine Agent, and AWS Migration Agent — synthesized into this unified document.*

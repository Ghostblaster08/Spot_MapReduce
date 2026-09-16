# Transient Spot-Instance MapReduce Framework

A fault-tolerant, spot-optimized MapReduce implementation featuring decoupled Remote Shuffle Storage (RSS), atomic sub-task checkpointing, preemption sentinels, and stage barrier orchestration.

---

## Architecture Summary

```
                       ┌─────────────────────────────────────────┐
                       │       Orchestrator (Port 8000)          │
                       │  FastAPI + SQLite (WAL mode)            │
                       │  - Task Scheduler & Lease Monitor       │
                       │  - Stage Barrier Coordinator            │
                       └───────────▲─────────────────▲───────────┘
                                   │                 │
                Heartbeat & Lease  │                 │ Task Poll & Checkpoints
                                   │                 │
     ┌─────────────────────────────┴─────────────────┴─────────────────────────────┐
     │                                                                             │
┌────┴───────────────────────────┐                           ┌─────────────────────┴──────────┐
│      Spot Worker (Replicas)    │                           │      Spot Worker (Replicas)    │
│  - Line-by-line mapper         │                           │  - Stream fetcher from RSS     │
│  - Partition hashing (xxHash)  │                           │  - In-memory / external sort   │
│  - Checkpoint engine (Redis)   │                           │  - Reduce function execution   │
│  - Preemption Sentinel         │                           │  - Output partition writer     │
└──────────────┬─────────────────┘                           └────────────────▲───────────────┘
               │                                                              │
               │ Push Partition Chunks (NDJSON)                               │ Stream Chunks
               ▼                                                              │
┌─────────────────────────────────────────────────────────────────────────────┴───────────────┐
│                               Remote Shuffle Service (Port 8001)                            │
│  - Chunk receiver & partition indexer                                                       │
│  - Decoupled storage volume (/data/rss)                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                               ▲
                               │ Checkpoint Offsets (CAS)
┌──────────────────────────────┴──────────────────────────────────────────────────────────────┐
│                                Redis 7.2 (Port 6379)                                        │
│  - Checkpoint Offset & Monotonic Fencing Store                                              │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Quickstart (Local Execution)

### 1. Build and Start the Stack

```bash
docker compose up --build -d
```

Check service health:
```bash
docker compose ps
```
All services (`orchestrator`, `rss`, `redis`, `worker`, `prometheus`, `grafana`) should show as `healthy` or `running`.

### 2. Prepare Sample Input Data

```bash
docker compose exec orchestrator mkdir -p /data/input
docker compose cp job-client/jobs/sample_input.txt orchestrator:/data/input/sample_input.txt
```

### 3. Submit a MapReduce Job

```bash
docker compose run --rm job-client submit \
  --name wordcount-test \
  --map-fn worker.jobs.wordcount.map_fn \
  --reduce-fn worker.jobs.wordcount.reduce_fn \
  --input /data/input/sample_input.txt \
  --output /data/output \
  --mappers 4 \
  --reducers 4 \
  --wait
```

### 4. Verify Output Correctness

```bash
./scripts/verify_output.sh
```

### 5. Access Monitoring Dashboards

- **Grafana Dashboard:** `http://localhost:3000` (User: `admin` / Pass: `admin` or anonymous login)
- **Prometheus Metrics:** `http://localhost:9090`
- **Orchestrator Health & API:** `http://localhost:8000/docs`
- **RSS Health & API:** `http://localhost:8001/docs`

---

## Fault Tolerance & Preemption Testing

The test scripts simulate real-world transient cloud failures:

### 1. Clean Spot Preemption (Graceful Drain)
Simulates AWS EC2 120-second spot termination notice or GCP 30-second ACPI soft off:
```bash
./scripts/simulate_preemption.sh [worker_container_name] [grace_seconds]
```
The worker intercepts the SIGTERM, flushes in-memory partition slabs to RSS, commits its exact byte offset to Redis, notifies the orchestrator, and shuts down. The orchestrator re-queues the task with an incremented attempt ID. A replacement worker resumes from the saved checkpoint offset with zero data loss.

### 2. Sudden Worker Crash (SIGKILL)
Simulates instant node death or kernel panic:
```bash
./scripts/crash_worker.sh
```
The worker dies without a drain opportunity. The orchestrator's lease monitor detects missing heartbeats within 5 seconds, invalidates the lease, and re-queues the task. A new worker resumes from the last committed micro-batch checkpoint.

### 3. Correlated Spot Storm
Simulates 40% simultaneous eviction across the spot fleet:
```bash
./scripts/spot_storm.sh
```
Surviving workers continue processing; evicted tasks are dynamically re-routed and recovered without job abortion.

---

## Project Structure

```
Spot_MapReduce/
├── docker-compose.yml           # Full multi-container topology
├── .env                         # Local runtime settings
├── orchestrator/                # FastAPI Master (scheduler, barrier, lease monitor)
├── rss/                         # Remote Shuffle Service (decoupled partition store)
├── worker/                      # Python Map/Reduce worker with sentinel & Redis CAS
├── job-client/                  # CLI client for job submission & tracking
├── monitoring/                  # Prometheus & Grafana configurations
└── scripts/                     # Preemption, storm, crash, and verification scripts
```

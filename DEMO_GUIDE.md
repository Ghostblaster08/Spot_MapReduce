# Transient Spot-Instance MapReduce Framework
## Live Demonstration & Testing Guide

This guide provides step-by-step instructions, CLI commands, and a structured script for presenting and verifying the **Transient Spot-Instance MapReduce Framework**.

---

## 1. Quick Command Reference

All commands should be executed from the project root:
`/home/ghostblaster08/Projects/Sem7/ACC/Spot_MapReduce`

### Cluster Lifecycle

```bash
# 1. Start all 6 services in the background
docker compose up -d

# 2. Check health and container statuses
docker compose ps

# 3. Stream live worker logs
docker compose logs -f worker

# 4. Stop cluster and clean up
docker compose down
```

---

### Submitting Jobs

#### Method A: Built-in CLI Client (`job-client`)

```bash
# Submit WordCount and automatically stream progress to terminal:
docker compose run --rm job-client submit \
  --name demo-run \
  --map-fn worker.jobs.wordcount.map_fn \
  --reduce-fn worker.jobs.wordcount.reduce_fn \
  --input /data/input/large_input.txt \
  --output /data/output \
  --mappers 4 \
  --reducers 4 \
  --wait
```

#### Method B: Zero-Dependency `curl`

```bash
# 1. Submit job:
curl -s -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "manual-run",
    "map_fn": "worker.jobs.wordcount.map_fn",
    "reduce_fn": "worker.jobs.wordcount.reduce_fn",
    "input_path": "/data/input/large_input.txt",
    "output_path": "/data/output",
    "num_mappers": 4,
    "num_reducers": 4
  }' | jq .

# 2. Poll job status:
curl -s http://localhost:8000/jobs/<JOB_ID> | jq .

# 3. Watch status in real-time (updates every 1s):
watch -n 1 "curl -s http://localhost:8000/jobs/<JOB_ID> | jq ."
```

---

### Output Verification

```bash
# 1. Copy output from container volume to local host directory:
docker compose cp orchestrator:/data/output/<JOB_ID> data/output/

# 2. Run ground-truth comparison against single-node reference:
./scripts/verify_output.sh <JOB_ID> job-client/jobs/large_input.txt
```

---

## 2. Interactive Web Dashboards

| Service | Local URL | What It Demonstrates |
| :--- | :--- | :--- |
| **Grafana Dashboard** | [`http://localhost:3000`](http://localhost:3000) | Live visual metrics for **Running Tasks**, **Queued Tasks**, **Clean Preemptions**, and **Crashes**. |
| **Orchestrator Swagger UI** | [`http://localhost:8000/docs`](http://localhost:8000/docs) | Interactive API docs showing endpoints for job submission, task leases, heartbeat renews, and stage transitions. |
| **RSS Shuffle Service UI** | [`http://localhost:8001/docs`](http://localhost:8001/docs) | Decoupled chunk ingestion (`/partitions/{job_id}/{pid}`) and streaming endpoints. |
| **Prometheus Metrics** | [`http://localhost:9090`](http://localhost:9090) | Direct query interface for time-series metrics (`mapreduce_preemptions_total`, `mapreduce_crashes_total`). |

---

## 3. Step-by-Step 3-Minute Live Demo Flow

Use this structured script to present the project to evaluators or professors.

```
+-----------------------------------------------------------------------------+
| ACT 1: Architectural Rationale & Web Dashboard Overview   (~30s)           |
| ACT 2: Baseline MapReduce Execution & Correctness Pass    (~45s)           |
| ACT 3: Killer Feature: Live Spot Eviction & Zero Data Loss (~90s)           |
| ACT 4: Sudden Crash Recovery & State Inspection (Q&A)     (~30s)           |
+-----------------------------------------------------------------------------+
```

### Act 1: Problem & Architectural Overview (30 seconds)

> **Talking Point:**
> *"In standard MapReduce (Hadoop/Spark), spot instance preemption causes cascading recomputations because intermediate shuffle data is written to the evicted worker's local disk. We solve this with three decoupled mechanisms:*
> 1. *A **Remote Shuffle Service (RSS)** that decouples shuffle storage from transient compute.*
> 2. *Atomic sub-task **Redis CAS checkpointing** tracking exact input byte offsets.*
> 3. *A **Preemption Sentinel** that catches SIGTERM / IMDSv2 notices, flushing in-memory buffers to RSS before shutdown.*"

- Open **Grafana** at `http://localhost:3000` to show real-time cluster metrics.

---

### Act 2: Baseline Execution (45 seconds)

1. Open a split terminal:
   - **Top / Left:** `docker compose logs -f worker`
   - **Bottom / Right:** Command prompt
2. Submit a MapReduce job with the 4.5 MB dataset (77,500 lines):
   ```bash
   docker compose run --rm job-client submit \
     --name baseline-demo \
     --map-fn worker.jobs.wordcount.map_fn \
     --reduce-fn worker.jobs.wordcount.reduce_fn \
     --input /data/input/large_input.txt \
     --output /data/output \
     --mappers 4 \
     --reducers 4 \
     --wait
   ```
3. **Point out to evaluator:**
   - 4 Workers concurrently claim Map splits.
   - Stage Barrier: when all 4 Map tasks complete, Orchestrator transitions to `REDUCE_RUNNING`.
   - Reducers stream partition chunks from RSS, perform external merge sort, and write outputs.
4. **Verify correctness:**
   ```bash
   JOB_ID=$(ls -t data/output | grep -v '^_SUCCESS' | head -n 1)
   docker compose cp orchestrator:/data/output/$JOB_ID data/output/
   ./scripts/verify_output.sh $JOB_ID job-client/jobs/large_input.txt
   ```
   Show: `[PASS] WordCount output matches ground truth perfectly! (45 unique words verified)`.

---

### Act 3: The Killer Feature — Live Preemption & Recovery (90 seconds)

This demonstrates spot eviction without job failure or data loss.

1. Submit a fresh job:
   ```bash
   JOB_RESP=$(curl -s -X POST http://localhost:8000/jobs -H "Content-Type: application/json" -d '{"name":"live-spot-eviction","map_fn":"worker.jobs.wordcount.map_fn","reduce_fn":"worker.jobs.wordcount.reduce_fn","input_path":"/data/input/large_input.txt","output_path":"/data/output","num_mappers":4,"num_reducers":4}')
   JOB_ID=$(echo "$JOB_RESP" | jq -r .job_id)
   echo "Submitted Job: $JOB_ID"
   ```

2. **Evict a worker mid-execution:**
   ```bash
   sleep 1.0
   ./scripts/simulate_preemption.sh spot_mapreduce-worker-1 15
   ```

3. **Highlight what happens in real-time:**
   - **Worker Log:** Sentinel catches SIGTERM:
     ```
     [SIGTERM] Preemption warning signal received!
     [SENTINEL DRAIN] Preemption signal detected on task-map-...! Initiating emergency drain...
     Committed checkpoint to Redis: task=task-map-... offset=1680200
     [SENTINEL DRAIN] Clean drain reported to orchestrator
     ```
   - **Orchestrator Log:** Task is re-queued with incremented `attempt_id = 1`.
   - **Replacement Worker:** Claims the task and resumes:
     ```
     Resuming task from Redis checkpoint: offset=1680200
     ```
     *(Does NOT re-read from byte 0; avoids re-computing what was already flushed!)*
   - **Job Completes:** State transitions cleanly to `COMPLETED`.

4. **Run ground-truth check to prove zero data corruption:**
   ```bash
   docker compose cp orchestrator:/data/output/$JOB_ID data/output/
   ./scripts/verify_output.sh $JOB_ID job-client/jobs/large_input.txt
   ```
   Output:
   ```
   ==> [PASS] WordCount output matches ground truth perfectly! (45 unique words verified)
   ```

---

### Act 4: Sudden Worker Crash Recovery (SIGKILL)

If asked: *"What happens if the node dies instantly without a drain window?"*:

1. Run the instant crash script:
   ```bash
   ./scripts/crash_worker.sh
   ```
2. **Explain the mechanism:**
   - `SIGKILL` prevents the worker from running any drain logic.
   - The Orchestrator's background **Lease Monitor** detects missing heartbeats within 5 seconds.
   - Lease expires $\to$ task is re-queued $\to$ surviving worker claims it and resumes from the last micro-batch checkpoint committed to Redis.

---

## 5. Under-the-Hood Inspection (For Deep Technical Q&A)

### 1. Inspect Orchestrator SQLite Database

Shows monotonically increasing attempt IDs, byte ranges, and lease timestamps:
```bash
docker compose exec orchestrator python3 -c "
import asyncio, aiosqlite
async def dump():
    async with aiosqlite.connect('/data/orchestrator/state.db') as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute('SELECT task_id, state, attempt_id, split_start, split_end, last_checkpoint_offset FROM tasks LIMIT 8')
        for r in await cur.fetchall(): print(dict(r))
asyncio.run(dump())
"
```

### 2. Inspect Redis Atomic Checkpoints

Shows active CAS keys and committed byte offsets:
```bash
# List all checkpoint keys
docker compose exec redis redis-cli keys "smr:*"

# Inspect details of a specific checkpoint
docker compose exec redis redis-cli mget "smr:<JOB_ID>:<TASK_ID>:0:offset" "smr:<JOB_ID>:<TASK_ID>:0:record_id"
```

### 3. Inspect Physical RSS Chunk Files

Demonstrates how intermediate partitions are stored decoupled from the workers:
```bash
# List physical chunk files for partition 0:
docker compose exec rss ls -lh /data/rss/<JOB_ID>/0/chunks/

# View sample NDJSON records inside an RSS chunk:
docker compose exec rss head -n 5 /data/rss/<JOB_ID>/0/chunks/chunk-*-00000.ndjson
```

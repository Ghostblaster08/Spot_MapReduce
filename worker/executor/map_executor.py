import os
import json
import time
import importlib
import logging
import xxhash
import requests
from typing import Dict, Any, List

try:
    from worker.checkpoint.store import commit_checkpoint, load_checkpoint
    from worker.rss_client.client import RSSClient
    from worker.sentinel.preemption_handler import PreemptionHandler
except ModuleNotFoundError:
    from checkpoint.store import commit_checkpoint, load_checkpoint
    from rss_client.client import RSSClient
    from sentinel.preemption_handler import PreemptionHandler

log = logging.getLogger("worker.map_executor")

CHECKPOINT_INTERVAL_SECONDS = float(os.environ.get("CHECKPOINT_INTERVAL_SECONDS", "10.0"))
CHECKPOINT_N_RECORDS = int(os.environ.get("CHECKPOINT_N_RECORDS", "1000"))
SLAB_LIMIT_RECORDS = int(os.environ.get("SLAB_LIMIT_RECORDS", "500"))

def run_map_task(
    task: Dict[str, Any],
    sentinel: PreemptionHandler,
    rss_client: RSSClient,
    redis_client,
    orchestrator_url: str,
    state_ref: Dict[str, Any]
) -> str:
    job_id = task["job_id"]
    task_id = task["task_id"]
    attempt_id = task["attempt_id"]
    input_path = task["input_path"]
    split_start = task.get("split_start") or 0
    split_end = task.get("split_end") or float("inf")
    num_reducers = task["num_reducers"]
    map_fn_path = task["map_fn"]

    log.info(f"Starting MAP task {task_id} (attempt {attempt_id}) for job {job_id}")

    # ── 1. Dynamically load map function ─────────────────────────
    module_name, fn_name = map_fn_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        if module_name.startswith("worker."):
            module = importlib.import_module(module_name[7:])
        else:
            raise
    map_fn = getattr(module, fn_name)

    # ── 2. Check for existing checkpoint (Redis or Task info) ────
    chk = load_checkpoint(redis_client, job_id, task_id)
    if chk and chk.byte_offset > split_start:
        resume_offset = chk.byte_offset
        records_processed = chk.record_id
        ckpt_seq = chk.seq
        chunk_counters = {pid: chk.manifest.get(pid, 0) for pid in range(num_reducers)}
        log.info(f"Resuming task {task_id} from Redis checkpoint: offset={resume_offset}, records={records_processed}, seq={ckpt_seq}")
    else:
        resume_offset = task.get("resume_from_offset", split_start)
        records_processed = task.get("resume_from_seq", 0)
        ckpt_seq = 0
        chunk_counters = {pid: 0 for pid in range(num_reducers)}

    buffers: Dict[int, List[Dict[str, Any]]] = {pid: [] for pid in range(num_reducers)}

    def flush_partition(pid: int):
        if not buffers[pid]:
            return
        payload_lines = [json.dumps(r) for r in buffers[pid]]
        payload_bytes = ("\n".join(payload_lines) + "\n").encode("utf-8")
        seq = chunk_counters[pid]
        rss_client.push_chunk(
            job_id=job_id,
            partition_id=pid,
            task_id=task_id,
            attempt_id=attempt_id,
            chunk_seq=seq,
            payload=payload_bytes
        )
        chunk_counters[pid] = seq + 1
        buffers[pid].clear()

    def flush_all_buffers():
        for pid in range(num_reducers):
            flush_partition(pid)

    def record_checkpoint_sync(offset: int, recs: int, is_final_drain: bool = False):
        flush_all_buffers()
        # Redis CAS commit
        commit_checkpoint(
            r=redis_client,
            job_id=job_id,
            task_id=task_id,
            attempt_id=attempt_id,
            byte_offset=offset,
            record_id=recs,
            seq=ckpt_seq,
            manifest=dict(chunk_counters),
            is_final_drain=is_final_drain
        )
        # Orchestrator commit
        try:
            requests.post(
                f"{orchestrator_url}/tasks/{task_id}/checkpoint",
                json={
                    "task_id": task_id,
                    "job_id": job_id,
                    "attempt_id": attempt_id,
                    "byte_offset": offset,
                    "records_processed": recs,
                    "partition_manifest": {str(k): v for k, v in chunk_counters.items()},
                    "is_final_drain": is_final_drain
                },
                timeout=5
            )
        except Exception as e:
            log.warning(f"Could not notify orchestrator of checkpoint: {e}")

    last_ckpt_time = time.monotonic()
    last_ckpt_records = records_processed
    byte_pos = resume_offset

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with open(input_path, "rb") as f:
        f.seek(resume_offset)

        # If not starting at the very beginning of the file, skip the partial line to align
        if resume_offset > 0:
            skipped = f.readline()
            byte_pos += len(skipped)

        while byte_pos < split_end:
            # ── Preemption check ──────────────────────────────────
            if sentinel.is_preempting.is_set():
                log.warning(f"[SENTINEL DRAIN] Preemption signal detected on {task_id}! Initiating emergency drain...")
                ckpt_seq += 1
                record_checkpoint_sync(byte_pos, records_processed, is_final_drain=True)

                try:
                    requests.post(
                        f"{orchestrator_url}/tasks/{task_id}/drain",
                        json={
                            "task_id": task_id,
                            "job_id": job_id,
                            "attempt_id": attempt_id,
                            "drain_status": "CLEANLY_MIGRATED",
                            "final_byte_offset": byte_pos,
                            "final_records_processed": records_processed
                        },
                        timeout=5
                    )
                    log.info(f"[SENTINEL DRAIN] Clean drain reported to orchestrator for {task_id}")
                except Exception as e:
                    log.error(f"Failed to report drain to orchestrator: {e}")

                return "DRAINED"

            line_bytes = f.readline()
            if not line_bytes:
                break

            byte_pos += len(line_bytes)
            line_str = line_bytes.decode("utf-8", errors="replace").strip()
            if not line_str:
                continue

            # Execute user map function
            pairs = map_fn(line_str)
            for k, v in pairs:
                pid = xxhash.xxh32(k.encode("utf-8")).intdigest() % num_reducers
                buffers[pid].append({"key": k, "value": v})
                if len(buffers[pid]) >= SLAB_LIMIT_RECORDS:
                    flush_partition(pid)

            records_processed += 1
            state_ref["records_processed"] = records_processed
            state_ref["last_byte_offset"] = byte_pos

            # Checkpoint boundary
            now = time.monotonic()
            if (now - last_ckpt_time >= CHECKPOINT_INTERVAL_SECONDS) or (records_processed - last_ckpt_records >= CHECKPOINT_N_RECORDS):
                ckpt_seq += 1
                record_checkpoint_sync(byte_pos, records_processed, is_final_drain=False)
                last_ckpt_time = now
                last_ckpt_records = records_processed

    # Final flush
    flush_all_buffers()

    # Commit completion to orchestrator
    resp = requests.post(
        f"{orchestrator_url}/tasks/{task_id}/complete",
        json={
            "task_id": task_id,
            "job_id": job_id,
            "attempt_id": attempt_id,
            "final_byte_offset": byte_pos,
            "final_records_processed": records_processed,
            "partition_manifest": {str(k): v for k, v in chunk_counters.items()}
        },
        timeout=10
    )
    if resp.status_code != 200:
        log.error(f"Orchestrator rejected complete: {resp.text}")
        return "FAILED"

    log.info(f"MAP task {task_id} completed successfully (processed {records_processed} records).")
    return "COMPLETED"

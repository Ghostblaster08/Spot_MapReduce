import os
import itertools
import importlib
import logging
import requests
from typing import Dict, Any, List

try:
    from worker.rss_client.client import RSSClient
    from worker.sentinel.preemption_handler import PreemptionHandler
except ModuleNotFoundError:
    from rss_client.client import RSSClient
    from sentinel.preemption_handler import PreemptionHandler

log = logging.getLogger("worker.reduce_executor")

def run_reduce_task(
    task: Dict[str, Any],
    sentinel: PreemptionHandler,
    rss_client: RSSClient,
    orchestrator_url: str,
    state_ref: Dict[str, Any]
) -> str:
    job_id = task["job_id"]
    task_id = task["task_id"]
    attempt_id = task["attempt_id"]
    partition_id = task["partition_id"]
    reduce_fn_path = task["reduce_fn"]
    output_path = task.get("output_path", "/data/output")

    log.info(f"Starting REDUCE task {task_id} (partition {partition_id}) for job {job_id}")

    # ── 1. Dynamically import reduce function ─────────────────────
    module_name, fn_name = reduce_fn_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        if module_name.startswith("worker."):
            module = importlib.import_module(module_name[7:])
        else:
            raise
    reduce_fn = getattr(module, fn_name)

    # ── 2. Stream all records for this partition from RSS ─────────
    log.info(f"Fetching partition {partition_id} stream from RSS...")
    all_pairs: List[tuple] = []
    try:
        for k, v in rss_client.fetch_partition_stream(job_id, partition_id):
            all_pairs.append((k, v))
            state_ref["records_processed"] = len(all_pairs)
    except Exception as e:
        log.error(f"Failed to fetch partition stream from RSS: {e}", exc_info=True)
        return "FAILED"

    log.info(f"Fetched {len(all_pairs)} records from RSS for partition {partition_id}. Sorting by key...")

    # ── 3. Sort by key ────────────────────────────────────────────
    all_pairs.sort(key=lambda x: str(x[0]))

    # ── 4. Group by key and apply reduce function ─────────────────
    job_output_dir = os.path.join(output_path, job_id)
    os.makedirs(job_output_dir, exist_ok=True)
    out_file_path = os.path.join(job_output_dir, f"part-{partition_id:05d}.txt")
    temp_file_path = f"{out_file_path}.tmp"

    with open(temp_file_path, "w", encoding="utf-8") as out:
        for key, group in itertools.groupby(all_pairs, key=lambda x: x[0]):
            if sentinel.is_preempting.is_set():
                log.warning(f"[SENTINEL DRAIN] Preemption during reduce task {task_id}!")
                try:
                    requests.post(
                        f"{orchestrator_url}/tasks/{task_id}/drain",
                        json={
                            "task_id": task_id,
                            "job_id": job_id,
                            "attempt_id": attempt_id,
                            "drain_status": "CLEANLY_MIGRATED",
                            "final_byte_offset": 0,
                            "final_records_processed": 0
                        },
                        timeout=5
                    )
                except Exception as e:
                    log.error(f"Failed to report drain to orchestrator: {e}")
                return "DRAINED"

            values = [val for _, val in group]
            result = reduce_fn(key, values)
            out.write(f"{key}\t{result}\n")

    os.replace(temp_file_path, out_file_path)
    log.info(f"Wrote partition output to {out_file_path}")

    # ── 5. Complete task in orchestrator ──────────────────────────
    resp = requests.post(
        f"{orchestrator_url}/tasks/{task_id}/complete",
        json={
            "task_id": task_id,
            "job_id": job_id,
            "attempt_id": attempt_id,
            "final_byte_offset": 0,
            "final_records_processed": len(all_pairs)
        },
        timeout=10
    )
    if resp.status_code != 200:
        log.error(f"Orchestrator rejected complete for reduce task: {resp.text}")
        return "FAILED"

    log.info(f"REDUCE task {task_id} completed successfully.")
    return "COMPLETED"

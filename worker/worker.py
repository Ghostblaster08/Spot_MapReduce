import os
import sys
import time
import uuid
import threading
import logging
import requests
import redis

try:
    from worker.sentinel.preemption_handler import PreemptionHandler
    from worker.rss_client.client import RSSClient
    from worker.executor.map_executor import run_map_task
    from worker.executor.reduce_executor import run_reduce_task
except ModuleNotFoundError:
    from sentinel.preemption_handler import PreemptionHandler
    from rss_client.client import RSSClient
    from executor.map_executor import run_map_task
    from executor.reduce_executor import run_reduce_task

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("worker.main")

ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://orchestrator:8000").rstrip("/")
RSS_URL = os.environ.get("RSS_URL", "http://rss:8001").rstrip("/")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
HEARTBEAT_INTERVAL_SECONDS = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "2.0"))
WORKER_ID_PREFIX = os.environ.get("WORKER_ID_PREFIX", "worker")

def heartbeat_loop(worker_id: str, state_ref: dict, sentinel: PreemptionHandler):
    session = requests.Session()
    log.info(f"Heartbeat thread started for worker {worker_id} (interval={HEARTBEAT_INTERVAL_SECONDS}s)")

    while not sentinel.is_shutdown.is_set():
        task_id = state_ref.get("current_task_id")
        attempt_id = state_ref.get("current_attempt_id", 0)

        if task_id:
            try:
                resp = session.post(
                    f"{ORCHESTRATOR_URL}/heartbeat",
                    json={
                        "task_id": task_id,
                        "worker_id": worker_id,
                        "attempt_id": attempt_id,
                        "records_processed": state_ref.get("records_processed", 0)
                    },
                    timeout=3
                )
                if resp.status_code == 409:
                    log.warning(f"[FENCED] Received 409 Conflict from orchestrator for task {task_id}. Stopping execution.")
                    sentinel.is_preempting.set()
                elif resp.status_code == 200:
                    data = resp.json()
                    if not data.get("continue_task", True):
                        log.warning(f"Orchestrator commanded to stop task {task_id}")
                        sentinel.is_preempting.set()
            except Exception as e:
                log.warning(f"Heartbeat to {ORCHESTRATOR_URL} failed: {e}")

        time.sleep(HEARTBEAT_INTERVAL_SECONDS)

def main():
    worker_id = f"{WORKER_ID_PREFIX}-{uuid.uuid4().hex[:8]}"
    log.info(f"Starting Spot Worker ID: {worker_id}")

    sentinel = PreemptionHandler()
    rss_client = RSSClient(RSS_URL)

    try:
        redis_client = redis.Redis.from_url(REDIS_URL)
        redis_client.ping()
        log.info(f"Connected to Redis at {REDIS_URL}")
    except Exception as e:
        log.error(f"Could not connect to Redis: {e}")
        sys.exit(1)

    state_ref = {
        "current_task_id": None,
        "current_attempt_id": 0,
        "records_processed": 0,
        "last_byte_offset": 0
    }

    hb_thread = threading.Thread(
        target=heartbeat_loop,
        args=(worker_id, state_ref, sentinel),
        daemon=True,
        name="heartbeat"
    )
    hb_thread.start()

    session = requests.Session()
    log.info(f"Entering task polling loop (orchestrator={ORCHESTRATOR_URL})...")

    while not sentinel.is_shutdown.is_set():
        if sentinel.is_preempting.is_set():
            log.warning("Worker is preempted. Exiting poll loop.")
            break

        try:
            resp = session.get(
                f"{ORCHESTRATOR_URL}/tasks/poll",
                params={"worker_id": worker_id},
                timeout=10
            )
            if resp.status_code == 204:
                time.sleep(1.0)
                continue

            if resp.status_code != 200:
                log.warning(f"Orchestrator poll returned {resp.status_code}: {resp.text}")
                time.sleep(2.0)
                continue

            task = resp.json()
            state_ref["current_task_id"] = task["task_id"]
            state_ref["current_attempt_id"] = task["attempt_id"]
            state_ref["records_processed"] = 0

            t_type = task["task_type"]
            log.info(f"Claimed {t_type} task {task['task_id']} (job: {task['job_id']})")

            try:
                if t_type == "MAP":
                    result = run_map_task(
                        task=task,
                        sentinel=sentinel,
                        rss_client=rss_client,
                        redis_client=redis_client,
                        orchestrator_url=ORCHESTRATOR_URL,
                        state_ref=state_ref
                    )
                elif t_type == "REDUCE":
                    result = run_reduce_task(
                        task=task,
                        sentinel=sentinel,
                        rss_client=rss_client,
                        orchestrator_url=ORCHESTRATOR_URL,
                        state_ref=state_ref
                    )
                else:
                    log.error(f"Unknown task type: {t_type}")
                    result = "FAILED"
            except Exception as e:
                log.error(f"Task {task['task_id']} failed with error: {e}", exc_info=True)
                try:
                    session.post(
                        f"{ORCHESTRATOR_URL}/tasks/{task['task_id']}/fail",
                        json={
                            "task_id": task["task_id"],
                            "attempt_id": task["attempt_id"],
                            "error_message": str(e)
                        },
                        timeout=5
                    )
                except Exception as net_err:
                    log.warning(f"Could not report task failure: {net_err}")
                result = "FAILED"

            state_ref["current_task_id"] = None
            log.info(f"Finished execution of {task['task_id']} with result: {result}")

            if result == "DRAINED":
                log.warning("Task was drained due to preemption. Exiting worker cleanly.")
                sys.exit(0)

        except requests.exceptions.RequestException as e:
            log.warning(f"Orchestrator connection error: {e}. Retrying in 2s...")
            time.sleep(2.0)
        except Exception as e:
            log.error(f"Unexpected worker error: {e}", exc_info=True)
            time.sleep(2.0)

    log.info(f"Worker {worker_id} exiting cleanly.")
    sys.exit(0)

if __name__ == "__main__":
    main()

import time
import logging
import requests
from typing import Generator, Tuple, Any
import json

log = logging.getLogger("worker.rss_client")

class RSSClient:
    def __init__(self, rss_url: str):
        self.rss_url = rss_url.rstrip("/")
        self.session = requests.Session()

    def push_chunk(
        self,
        job_id: str,
        partition_id: int,
        task_id: str,
        attempt_id: int,
        chunk_seq: int,
        payload: bytes,
        max_retries: int = 5
    ) -> bool:
        url = f"{self.rss_url}/partitions/{job_id}/{partition_id}"
        params = {
            "task_id": task_id,
            "attempt_id": attempt_id,
            "chunk_seq": chunk_seq
        }

        for attempt in range(max_retries):
            try:
                resp = self.session.post(
                    url,
                    params=params,
                    data=payload,
                    headers={"Content-Type": "application/x-ndjson"},
                    timeout=10
                )
                if resp.status_code == 200:
                    return True
                log.warning(f"RSS returned {resp.status_code}: {resp.text}")
            except Exception as e:
                backoff = min(0.2 * (2 ** attempt), 3.0)
                log.warning(f"RSS push failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {backoff:.2f}s...")
                time.sleep(backoff)

        raise RuntimeError(f"Failed to push chunk {chunk_seq} to RSS at {url} after {max_retries} attempts")

    def fetch_partition_stream(self, job_id: str, partition_id: int) -> Generator[Tuple[str, Any], None, None]:
        url = f"{self.rss_url}/partitions/{job_id}/{partition_id}/stream"
        with self.session.get(url, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if line:
                    record = json.loads(line.decode("utf-8"))
                    yield (record["key"], record["value"])

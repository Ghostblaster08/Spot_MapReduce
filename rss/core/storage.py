import os
import json
import hashlib
import shutil
import logging
from typing import AsyncGenerator, Dict, Any, List

log = logging.getLogger("rss.storage")

RSS_DATA_PATH = os.environ.get("RSS_DATA_PATH", "/data/rss")

def get_partition_dir(job_id: str, partition_id: int) -> str:
    return os.path.join(RSS_DATA_PATH, job_id, str(partition_id))

def get_manifest_path(job_id: str, partition_id: int) -> str:
    return os.path.join(get_partition_dir(job_id, partition_id), "manifest.json")

def load_manifest(job_id: str, partition_id: int) -> Dict[str, Any]:
    path = get_manifest_path(job_id, partition_id)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Failed to read manifest at {path}: {e}")
    return {
        "job_id": job_id,
        "partition_id": partition_id,
        "chunks": [],
        "total_records": 0,
        "is_sealed": False
    }

def save_manifest(job_id: str, partition_id: int, manifest: Dict[str, Any]):
    path = get_manifest_path(job_id, partition_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    os.replace(temp_path, path)

async def write_chunk(
    job_id: str,
    partition_id: int,
    task_id: str,
    attempt_id: int,
    chunk_seq: int,
    data: bytes
) -> Dict[str, Any]:
    p_dir = get_partition_dir(job_id, partition_id)
    chunks_dir = os.path.join(p_dir, "chunks")
    os.makedirs(chunks_dir, exist_ok=True)

    chunk_filename = f"chunk-{task_id}-{attempt_id}-{chunk_seq:05d}.ndjson"
    chunk_path = os.path.join(chunks_dir, chunk_filename)
    temp_path = f"{chunk_path}.tmp"

    checksum = hashlib.md5(data).hexdigest()
    # Count records (newline-delimited)
    record_count = len([line for line in data.split(b"\n") if line.strip()])

    with open(temp_path, "wb") as f:
        f.write(data)
    os.replace(temp_path, chunk_path)

    manifest = load_manifest(job_id, partition_id)

    # Check for duplicate chunk_seq from same task and attempt (idempotency)
    existing = [c for c in manifest["chunks"] if c.get("filename") == chunk_filename]
    if not existing:
        chunk_meta = {
            "chunk_id": f"chunk-{task_id}-{chunk_seq}",
            "filename": chunk_filename,
            "source_task_id": task_id,
            "attempt_id": attempt_id,
            "chunk_seq": chunk_seq,
            "record_count": record_count,
            "byte_size": len(data),
            "checksum_md5": checksum
        }
        manifest["chunks"].append(chunk_meta)
        manifest["total_records"] = sum(c["record_count"] for c in manifest["chunks"])
        save_manifest(job_id, partition_id, manifest)

    log.info(f"Committed chunk {chunk_filename} to partition {partition_id} ({record_count} records, {len(data)} bytes)")

    return {
        "chunk_id": f"chunk-{task_id}-{chunk_seq}",
        "partition_id": partition_id,
        "records_accepted": record_count,
        "checksum_md5": checksum,
        "status": "COMMITTED"
    }

async def stream_partition(job_id: str, partition_id: int) -> AsyncGenerator[bytes, None]:
    """Yields all chunk lines for the given partition in sequential order."""
    p_dir = get_partition_dir(job_id, partition_id)
    chunks_dir = os.path.join(p_dir, "chunks")
    if not os.path.exists(chunks_dir):
        return

    # List all chunk files sorted by name/timestamp
    chunk_files = sorted(os.listdir(chunks_dir))
    for fname in chunk_files:
        if fname.endswith(".ndjson"):
            fpath = os.path.join(chunks_dir, fname)
            with open(fpath, "rb") as f:
                for line in f:
                    if line.strip():
                        yield line.rstrip(b"\r\n") + b"\n"

def get_job_summary(job_id: str) -> Dict[str, Any]:
    job_dir = os.path.join(RSS_DATA_PATH, job_id)
    if not os.path.exists(job_dir):
        return {"job_id": job_id, "partitions": {}}

    result = {}
    for p_name in os.listdir(job_dir):
        if p_name.isdigit():
            pid = int(p_name)
            manifest = load_manifest(job_id, pid)
            result[str(pid)] = {
                "total_records": manifest.get("total_records", 0),
                "chunk_count": len(manifest.get("chunks", [])),
                "is_sealed": manifest.get("is_sealed", False)
            }
    return {"job_id": job_id, "partitions": result}

def delete_job(job_id: str) -> Dict[str, Any]:
    job_dir = os.path.join(RSS_DATA_PATH, job_id)
    if os.path.exists(job_dir):
        shutil.rmtree(job_dir, ignore_errors=True)
        return {"deleted": True, "job_id": job_id}
    return {"deleted": False, "job_id": job_id}

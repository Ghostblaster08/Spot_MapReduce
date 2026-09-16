import redis
import logging
from dataclasses import dataclass
from typing import Optional, Dict

log = logging.getLogger("worker.checkpoint")

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
    redis.call('DEL', pfx .. ':manifest')
    for i = 7, #ARGV, 2 do
        redis.call('HSET', pfx .. ':manifest', ARGV[i], ARGV[i+1])
    end
end
redis.call('SET', KEYS[2], ARGV[1])
for _, k in ipairs({':offset',':record_id',':seq',':status',':fencing_token',':manifest'}) do
    redis.call('EXPIRE', pfx .. k, 604800)
end
return 'OK'
"""

@dataclass
class CheckpointState:
    byte_offset: int
    record_id: int
    seq: int
    status: str
    fencing_token: int
    manifest: Dict[int, int]

def commit_checkpoint(
    r: redis.Redis,
    job_id: str,
    task_id: str,
    attempt_id: int,
    byte_offset: int,
    record_id: int,
    seq: int,
    manifest: Dict[int, int],
    status: str = "RUNNING",
    is_final_drain: bool = False
) -> bool:
    if is_final_drain:
        status = "SEALED"

    pfx = f"smr:{job_id}:{task_id}:{attempt_id}"
    lkey = f"smr:{job_id}:{task_id}:latest_attempt"

    mf_args = []
    for k, v in manifest.items():
        mf_args.extend([str(k), str(v)])

    args = [
        str(attempt_id),
        str(byte_offset),
        str(record_id),
        str(seq),
        status,
        str(len(manifest))
    ] + mf_args

    try:
        res = r.eval(CHECKPOINT_LUA, 2, pfx, lkey, *args)
        if res not in (b"OK", "OK"):
            log.warning(f"Redis rejected checkpoint: {res}")
            return False
        log.info(f"Committed checkpoint to Redis: task={task_id} attempt={attempt_id} seq={seq} offset={byte_offset}")
        return True
    except Exception as e:
        log.error(f"Failed to commit checkpoint to Redis: {e}")
        return False

def load_checkpoint(
    r: redis.Redis,
    job_id: str,
    task_id: str
) -> Optional[CheckpointState]:
    try:
        latest = r.get(f"smr:{job_id}:{task_id}:latest_attempt")
        if latest is None:
            return None
        att = int(latest)
        pfx = f"smr:{job_id}:{task_id}:{att}"
        vals = r.mget(f"{pfx}:offset", f"{pfx}:record_id", f"{pfx}:seq", f"{pfx}:status", f"{pfx}:fencing_token")
        if vals[0] is None:
            return None

        manifest_raw = r.hgetall(f"{pfx}:manifest")
        manifest = {int(k): int(v) for k, v in manifest_raw.items()}

        return CheckpointState(
            byte_offset=int(vals[0]),
            record_id=int(vals[1]),
            seq=int(vals[2]),
            status=vals[3].decode() if isinstance(vals[3], bytes) else (vals[3] or "RUNNING"),
            fencing_token=int(vals[4]),
            manifest=manifest
        )
    except Exception as e:
        log.error(f"Error loading checkpoint from Redis: {e}")
        return None

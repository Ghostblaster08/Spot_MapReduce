from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict

class TaskResponse(BaseModel):
    task_id: str
    job_id: str
    task_type: Literal["MAP", "REDUCE"]
    attempt_id: int
    split_start: Optional[int] = None
    split_end: Optional[int] = None
    input_path: Optional[str] = None
    map_fn: Optional[str] = None
    partition_id: Optional[int] = None
    reduce_fn: Optional[str] = None
    output_path: Optional[str] = None
    num_reducers: int
    rss_url: str
    resume_from_offset: int = 0
    resume_from_seq: int = 0

class HeartbeatRequest(BaseModel):
    task_id: str
    worker_id: str
    attempt_id: int
    records_processed: int = 0
    timestamp: Optional[str] = None

class HeartbeatResponse(BaseModel):
    status: str
    continue_task: bool = True
    message: Optional[str] = None

class CheckpointRequest(BaseModel):
    task_id: str
    job_id: str
    attempt_id: int
    byte_offset: int
    records_processed: int
    partition_manifest: Optional[Dict[str, int]] = None
    is_final_drain: bool = False

class TaskCompleteRequest(BaseModel):
    task_id: str
    job_id: str
    attempt_id: int
    final_byte_offset: int = 0
    final_records_processed: int = 0
    partition_manifest: Optional[Dict[str, int]] = None

class TaskDrainRequest(BaseModel):
    task_id: str
    job_id: str
    attempt_id: int
    drain_status: str = "CLEANLY_MIGRATED"
    final_byte_offset: int = 0
    final_records_processed: int = 0

class TaskFailRequest(BaseModel):
    task_id: str
    attempt_id: int
    error_message: str

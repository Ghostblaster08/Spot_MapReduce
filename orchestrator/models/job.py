from pydantic import BaseModel, Field
from typing import Optional

class JobSubmitRequest(BaseModel):
    name: str = Field(..., description="Human-readable job name, e.g., 'wordcount-run-1'")
    map_fn: str = Field(..., description="Dotted module path for mapper, e.g., 'jobs.wordcount.map_fn'")
    reduce_fn: str = Field(..., description="Dotted module path for reducer, e.g., 'jobs.wordcount.reduce_fn'")
    input_path: str = Field(..., description="Path to input file")
    output_path: str = Field(..., description="Directory path for final outputs")
    num_mappers: int = Field(default=4, ge=1, description="Number of mapper splits")
    num_reducers: int = Field(default=4, ge=1, description="Number of reducer partitions")

class JobResponse(BaseModel):
    job_id: str
    name: str
    state: str
    num_mappers: int
    num_reducers: int
    submitted_at: str

class JobStatusResponse(BaseModel):
    job_id: str
    name: str
    state: str
    tasks_total: int
    tasks_completed: int
    tasks_running: int
    tasks_queued: int
    tasks_failed: int
    submitted_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None

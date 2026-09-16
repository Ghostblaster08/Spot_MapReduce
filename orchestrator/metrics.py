from prometheus_client import Counter, Gauge

MAPREDUCE_JOBS_TOTAL = Counter(
    "mapreduce_jobs_total",
    "Total MapReduce jobs processed by final state",
    ["state"]
)

MAPREDUCE_TASKS_RUNNING = Gauge(
    "mapreduce_tasks_running",
    "Currently executing tasks",
    ["task_type"]
)

MAPREDUCE_TASKS_QUEUED = Gauge(
    "mapreduce_tasks_queued",
    "Tasks waiting for available worker",
    ["task_type"]
)

MAPREDUCE_TASKS_COMPLETED_TOTAL = Counter(
    "mapreduce_tasks_completed_total",
    "Total tasks completed successfully",
    ["task_type"]
)

MAPREDUCE_PREEMPTIONS_TOTAL = Counter(
    "mapreduce_preemptions_total",
    "Total clean preemption drain events received"
)

MAPREDUCE_CRASHES_TOTAL = Counter(
    "mapreduce_crashes_total",
    "Total worker crash events (heartbeat lease timeouts)"
)

MAPREDUCE_CHECKPOINT_COMMITS_TOTAL = Counter(
    "mapreduce_checkpoint_commits_total",
    "Total task progress checkpoints committed",
    ["is_final_drain"]
)

MAPREDUCE_HEARTBEAT_TIMEOUTS_TOTAL = Counter(
    "mapreduce_heartbeat_timeouts_total",
    "Total task heartbeat lease expiries"
)

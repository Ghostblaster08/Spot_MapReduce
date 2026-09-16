import os
import aiosqlite
import logging

log = logging.getLogger("orchestrator.database")

DB_PATH = os.environ.get("DB_PATH", "/data/orchestrator/state.db")

async def get_db() -> aiosqlite.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode = WAL;")
    await db.execute("PRAGMA synchronous = NORMAL;")
    await db.execute("PRAGMA foreign_keys = ON;")
    return db

async def init_db():
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    log.info(f"Initializing SQLite database at {DB_PATH}")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode = WAL;")
        await db.execute("PRAGMA synchronous = NORMAL;")
        await db.execute("PRAGMA foreign_keys = ON;")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id          TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            state           TEXT NOT NULL DEFAULT 'PENDING',
            map_fn          TEXT NOT NULL,
            reduce_fn       TEXT NOT NULL,
            input_path      TEXT NOT NULL,
            output_path     TEXT NOT NULL,
            num_mappers     INTEGER NOT NULL,
            num_reducers    INTEGER NOT NULL,
            submitted_at    TEXT NOT NULL,
            started_at      TEXT,
            completed_at    TEXT,
            error_message   TEXT
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            task_id             TEXT PRIMARY KEY,
            job_id              TEXT NOT NULL REFERENCES jobs(job_id),
            task_type           TEXT NOT NULL,
            state               TEXT NOT NULL DEFAULT 'QUEUED',
            split_start         INTEGER,
            split_end           INTEGER,
            partition_id        INTEGER,
            assigned_worker     TEXT,
            attempt_id          INTEGER NOT NULL DEFAULT 0,
            lease_deadline      TEXT,
            last_checkpoint_offset   INTEGER DEFAULT 0,
            last_checkpoint_seq      INTEGER DEFAULT 0,
            last_checkpoint_at       TEXT,
            created_at          TEXT NOT NULL,
            started_at          TEXT,
            completed_at        TEXT,
            error_message       TEXT
        );
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS checkpoints (
            checkpoint_id       TEXT PRIMARY KEY,
            task_id             TEXT NOT NULL REFERENCES tasks(task_id),
            job_id              TEXT NOT NULL,
            attempt_id          INTEGER NOT NULL,
            byte_offset         INTEGER NOT NULL,
            records_processed   INTEGER NOT NULL,
            partition_manifest  TEXT,
            is_final_drain      BOOLEAN DEFAULT FALSE,
            created_at          TEXT NOT NULL
        );
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_state_type ON tasks(state, task_type);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_job ON tasks(job_id);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_checkpoints_task ON checkpoints(task_id);")

        await db.commit()
    log.info("Database initialized successfully with WAL mode.")

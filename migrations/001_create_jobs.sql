-- migrations/001_create_jobs.sql
-- Creates the jobs table and performance indexes for the task queue.

CREATE TABLE IF NOT EXISTS jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL UNIQUE,
    job_type        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    priority        INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'queued',
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    worker_id       TEXT,
    processed_by_worker_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    retry_after     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    result_payload  JSONB,
    error_message   TEXT
);

-- Used by the dequeue query: workers grab the highest-priority queued job
-- whose retry_after has passed (or is null). This partial index stays small
-- because completed/dead-lettered jobs are excluded.
CREATE INDEX IF NOT EXISTS idx_jobs_dequeue
    ON jobs (priority DESC, created_at ASC)
    WHERE status = 'queued';

-- Used by the lease reaper: finds jobs that a worker claimed but whose lease
-- has expired, meaning the worker crashed or stalled.
CREATE INDEX IF NOT EXISTS idx_jobs_lease
    ON jobs (lease_expires_at)
    WHERE status = 'running' AND lease_expires_at IS NOT NULL;

-- Used for administrative queries: filtering jobs by type and status
-- (e.g., "show me all failed email jobs").
CREATE INDEX IF NOT EXISTS idx_jobs_type_status
    ON jobs (job_type, status);

from __future__ import annotations

import random
import uuid
from datetime import timedelta
from enum import StrEnum
from typing import Any

import psycopg
from psycopg import errors as pg_errors
from psycopg.rows import dict_row

from taskqueue.models import Job, JobStatus

NOTIFY_NEW_CHANNEL = "jobs_new"
NOTIFY_DONE_CHANNEL = "jobs_done"

BACKOFF_BASE_SECONDS = 10
BACKOFF_CAP_SECONDS = 3600
BACKOFF_JITTER_RATIO = 0.2


class DuplicateJobError(Exception):
    """Raised when an idempotency_key already exists."""

    def __init__(self, idempotency_key: str):
        super().__init__(f"job with idempotency_key={idempotency_key!r} already exists")
        self.idempotency_key = idempotency_key


class JobNotRunningError(Exception):
    """Raised when ack/nack is called on a job not in the running state."""

    def __init__(self, job_id: uuid.UUID):
        super().__init__(f"job {job_id} is not in 'running' state")
        self.job_id = job_id


class NackOutcome(StrEnum):
    RETRYING = "retrying"
    DEAD_LETTERED = "dead_lettered"


def enqueue(
    conn: psycopg.Connection,
    *,
    idempotency_key: str,
    job_type: str,
    payload: dict[str, Any],
    priority: int = 0,
    max_attempts: int = 3,
) -> uuid.UUID:
    """Insert a new queued job and NOTIFY listeners. Commits on success."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO jobs (idempotency_key, job_type, payload, priority, max_attempts)
                VALUES (%s, %s, %s::jsonb, %s, %s)
                RETURNING id
                """,
                (idempotency_key, job_type, psycopg.types.json.Jsonb(payload), priority, max_attempts),
            )
            row = cur.fetchone()
            assert row is not None
            job_id: uuid.UUID = row[0]
            cur.execute("SELECT pg_notify(%s, %s)", (NOTIFY_NEW_CHANNEL, str(job_id)))
        conn.commit()
        return job_id
    except pg_errors.UniqueViolation as exc:
        conn.rollback()
        raise DuplicateJobError(idempotency_key) from exc


def dequeue(
    conn: psycopg.Connection,
    *,
    worker_id: str,
    job_types: list[str] | None = None,
    lease_seconds: int = 60,
) -> Job | None:
    """Atomically claim the highest-priority eligible job. Commits on success.

    If ``job_types`` is given, only jobs whose ``job_type`` is in the list are
    eligible. ``None`` means any type. An empty list raises ``ValueError`` —
    almost always an upstream bug rather than an intentional "match nothing".
    """
    if job_types is not None and len(job_types) == 0:
        raise ValueError("job_types must be None or a non-empty list")

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            WITH claimed AS (
                SELECT id FROM jobs
                WHERE status = %(queued)s
                  AND (retry_after IS NULL OR retry_after <= now())
                  AND (%(job_types)s::text[] IS NULL
                       OR job_type = ANY(%(job_types)s::text[]))
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE jobs j
            SET status = %(running)s,
                worker_id = %(worker_id)s,
                lease_expires_at = now() + make_interval(secs => %(lease_seconds)s),
                started_at = now(),
                attempt_count = attempt_count + 1
            FROM claimed
            WHERE j.id = claimed.id
            RETURNING j.*
            """,
            {
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
                "job_types": job_types,
                "queued": JobStatus.QUEUED,
                "running": JobStatus.RUNNING,
            },
        )
        row = cur.fetchone()
    conn.commit()
    if row is None:
        return None
    return Job(**row)


def ack(
    conn: psycopg.Connection,
    *,
    job_id: uuid.UUID,
    result_payload: dict[str, Any] | None = None,
) -> None:
    """Mark a running job succeeded. Commits on success.

    Raises JobNotRunningError if the row is not currently in 'running' (e.g.
    the lease expired and the reaper put it back, or ack was called twice).
    Fires NOTIFY on NOTIFY_DONE_CHANNEL with the job id as payload.
    """
    payload_param = (
        psycopg.types.json.Jsonb(result_payload) if result_payload is not None else None
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = %s,
                result_payload = %s::jsonb,
                completed_at = now(),
                lease_expires_at = NULL,
                worker_id = NULL
            WHERE id = %s AND status = %s
            RETURNING id
            """,
            (JobStatus.SUCCEEDED, payload_param, job_id, JobStatus.RUNNING),
        )
        if cur.fetchone() is None:
            conn.rollback()
            raise JobNotRunningError(job_id)
        cur.execute("SELECT pg_notify(%s, %s)", (NOTIFY_DONE_CHANNEL, str(job_id)))
    conn.commit()


def _compute_backoff(attempt: int) -> timedelta:
    """Exponential backoff with jitter: base*2^(attempt-1), capped, ±jitter.

    attempt is 1-indexed (first retry uses attempt=1).
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    raw = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
    capped = min(raw, BACKOFF_CAP_SECONDS)
    jitter = capped * BACKOFF_JITTER_RATIO * (2 * random.random() - 1)
    return timedelta(seconds=capped + jitter)


def nack(
    conn: psycopg.Connection,
    *,
    job_id: uuid.UUID,
    error_message: str | None = None,
) -> NackOutcome:
    """Record a job failure. Commits on success.

    If attempt_count < max_attempts, the job goes back to 'queued' with
    retry_after set to now + exponential backoff. Otherwise it is routed
    to dead_letter. Returns which outcome was taken.

    Raises JobNotRunningError if the row is not currently 'running'.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT attempt_count, max_attempts FROM jobs WHERE id = %s AND status = %s FOR UPDATE",
            (job_id, JobStatus.RUNNING),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            raise JobNotRunningError(job_id)
        attempt_count, max_attempts = row

        if attempt_count >= max_attempts:
            cur.execute(
                """
                UPDATE jobs
                SET status = %s,
                    completed_at = now(),
                    error_message = %s,
                    lease_expires_at = NULL,
                    worker_id = NULL
                WHERE id = %s
                """,
                (JobStatus.DEAD_LETTER, error_message, job_id),
            )
            outcome = NackOutcome.DEAD_LETTERED
        else:
            backoff = _compute_backoff(attempt_count)
            cur.execute(
                """
                UPDATE jobs
                SET status = %s,
                    retry_after = now() + %s,
                    error_message = %s,
                    lease_expires_at = NULL,
                    worker_id = NULL
                WHERE id = %s
                """,
                (JobStatus.QUEUED, backoff, error_message, job_id),
            )
            outcome = NackOutcome.RETRYING
    conn.commit()
    return outcome

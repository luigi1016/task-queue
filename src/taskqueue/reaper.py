"""Reclaim jobs whose worker leases have expired.

A worker that claims a job sets lease_expires_at = now() + lease_seconds.
If the worker crashes, hangs, or has its pod killed before it can ack/nack,
that lease eventually passes. This script finds such jobs and flips them
back to 'queued' so another worker can pick them up — unless the job has
already hit max_attempts, in which case it goes to 'dead_letter'. The
dead-letter branch catches poison-pill jobs that kill the worker process
(OOM, SIGKILL, segfault) without raising an exception that nack() could
catch and route normally.

Designed as a K8s CronJob entry point: run once, print counts, exit.
"""

import psycopg

from taskqueue.db import get_connection
from taskqueue.models import JobStatus


def reclaim_expired_leases(conn: psycopg.Connection) -> tuple[int, int]:
    """Reset abandoned 'running' jobs, or dead-letter ones out of attempts.

    Returns (reclaimed_count, dead_lettered_count). Idempotent. Safe to run
    concurrently with workers: the WHERE clause only matches rows whose
    lease has already passed, which a live worker would have either
    renewed (future) or moved out of 'running' via ack/nack.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = CASE
                    WHEN attempt_count >= max_attempts THEN %s
                    ELSE %s
                END,
                completed_at = CASE
                    WHEN attempt_count >= max_attempts THEN now()
                    ELSE NULL
                END,
                error_message = CASE
                    WHEN attempt_count >= max_attempts
                        THEN 'lease expired ' || attempt_count::text
                          || ' times (max ' || max_attempts::text || ')'
                    ELSE error_message
                END,
                worker_id = NULL,
                lease_expires_at = NULL
            WHERE status = %s
              AND lease_expires_at < now()
            RETURNING status
            """,
            (JobStatus.DEAD_LETTER, JobStatus.QUEUED, JobStatus.RUNNING),
        )
        rows = cur.fetchall()
    conn.commit()
    dead_lettered = sum(1 for (s,) in rows if s == JobStatus.DEAD_LETTER)
    reclaimed = len(rows) - dead_lettered
    return reclaimed, dead_lettered


def main() -> None:
    conn = get_connection()
    try:
        reclaimed, dead_lettered = reclaim_expired_leases(conn)
        print(
            f"reaper: reclaimed {reclaimed} expired leases, "
            f"dead-lettered {dead_lettered}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()

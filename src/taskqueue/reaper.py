"""Reclaim jobs whose worker leases have expired.

A worker that claims a job sets lease_expires_at = now() + lease_seconds.
If the worker crashes, hangs, or has its pod killed before it can ack/nack,
that lease eventually passes. This script finds such jobs and flips them
back to 'queued' so another worker can pick them up.

Designed as a K8s CronJob entry point: run once, print count, exit.
"""

import psycopg

from taskqueue.db import get_connection
from taskqueue.models import JobStatus


def reclaim_expired_leases(conn: psycopg.Connection) -> int:
    """Reset abandoned 'running' jobs back to 'queued'. Returns count reclaimed.

    Idempotent. Safe to run concurrently with workers: the WHERE clause only
    matches rows whose lease has already passed, which a live worker would
    have either renewed (future) or moved out of 'running' via ack/nack.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = %s,
                processed_by_worker_id = worker_id,
                worker_id = NULL,
                lease_expires_at = NULL
            WHERE status = %s
              AND lease_expires_at < now()
            RETURNING id
            """,
            (JobStatus.QUEUED, JobStatus.RUNNING),
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows)


def main() -> None:
    conn = get_connection()
    try:
        n = reclaim_expired_leases(conn)
        print(f"reaper: reclaimed {n} expired leases")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

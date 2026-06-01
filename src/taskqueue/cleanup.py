"""Delete terminal jobs whose retention TTL has passed.

Jobs in terminal states (succeeded, failed, dead_letter) stay in the `jobs`
table forever unless removed. Over time this bloats the table, grows indexes,
and slows admin queries. This script hard-deletes terminal rows older than a
configurable TTL.

Deletes in bounded batches with a commit per batch so a large backlog can't
hold locks or balloon the WAL in one giant transaction. The TTL is relative to
now(), so a missed run loses nothing — the next run catches everything up.

Designed as a K8s CronJob entry point: run once, print count, exit.
"""

import os

import psycopg

from taskqueue.db import get_connection
from taskqueue.models import JobStatus

DEFAULT_TTL_DAYS = 7
DEFAULT_BATCH_SIZE = 10_000

_TERMINAL_STATUSES = (
    JobStatus.SUCCEEDED,
    JobStatus.FAILED,
    JobStatus.DEAD_LETTER,
)


def delete_terminal_jobs(
    conn: psycopg.Connection,
    ttl_days: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Delete terminal jobs older than ttl_days. Returns total rows deleted.

    Only rows in a terminal status are ever matched — queued/running jobs are
    not in the status set (and their completed_at is NULL, which also fails the
    age check), so they are never touched at any age. Each batch commits before
    the next begins, bounding lock duration on a large table.
    """
    total = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM jobs
                WHERE id IN (
                    SELECT id FROM jobs
                    WHERE status IN (%s, %s, %s)
                      AND completed_at < now() - make_interval(days => %s)
                    LIMIT %s
                )
                RETURNING id
                """,
                (*_TERMINAL_STATUSES, ttl_days, batch_size),
            )
            deleted = len(cur.fetchall())
        conn.commit()
        total += deleted
        if deleted < batch_size:
            break
    return total


def main() -> None:
    ttl_days = int(os.environ.get("CLEANUP_TTL_DAYS", DEFAULT_TTL_DAYS))
    conn = get_connection()
    try:
        n = delete_terminal_jobs(conn, ttl_days)
        print(f"cleanup: deleted {n} terminal jobs older than {ttl_days}d")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

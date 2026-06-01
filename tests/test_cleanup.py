from __future__ import annotations

import uuid

from taskqueue import JobStatus
from taskqueue.cleanup import delete_terminal_jobs


def _insert_job(conn, status: JobStatus, completed_age_days: float | None) -> uuid.UUID:
    """Insert a job directly with a given status and completed_at age.

    completed_age_days is how far in the past to backdate completed_at; pass
    None to leave completed_at NULL (as non-terminal jobs have it).
    """
    job_id = uuid.uuid4()
    completed_at = (
        None if completed_age_days is None else f"now() - interval '{completed_age_days} days'"
    )
    completed_sql = "NULL" if completed_at is None else completed_at
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO jobs (id, idempotency_key, job_type, payload, status, completed_at)
            VALUES (%s, %s, 't', '{{}}', %s, {completed_sql})
            """,
            (job_id, str(job_id), status),
        )
    conn.commit()
    return job_id


def _exists(conn, job_id: uuid.UUID) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM jobs WHERE id = %s", (job_id,))
        return cur.fetchone() is not None


def test_deletes_succeeded_past_ttl(conn):
    job_id = _insert_job(conn, JobStatus.SUCCEEDED, completed_age_days=10)

    n = delete_terminal_jobs(conn, ttl_days=7)

    assert n == 1
    assert not _exists(conn, job_id)


def test_keeps_succeeded_within_ttl(conn):
    job_id = _insert_job(conn, JobStatus.SUCCEEDED, completed_age_days=3)

    n = delete_terminal_jobs(conn, ttl_days=7)

    assert n == 0
    assert _exists(conn, job_id)


def test_keeps_non_terminal_jobs_even_when_old(conn):
    # Backdate created_at via a very old completed_at would not apply here; these
    # rows have completed_at NULL, mirroring real queued/running jobs.
    queued = _insert_job(conn, JobStatus.QUEUED, completed_age_days=None)
    running = _insert_job(conn, JobStatus.RUNNING, completed_age_days=None)

    n = delete_terminal_jobs(conn, ttl_days=7)

    assert n == 0
    assert _exists(conn, queued)
    assert _exists(conn, running)


def test_deletes_failed_and_dead_letter(conn):
    failed = _insert_job(conn, JobStatus.FAILED, completed_age_days=30)
    dead = _insert_job(conn, JobStatus.DEAD_LETTER, completed_age_days=30)

    n = delete_terminal_jobs(conn, ttl_days=7)

    assert n == 2
    assert not _exists(conn, failed)
    assert not _exists(conn, dead)


def test_returns_total_across_mixed_rows(conn):
    old_ids = [_insert_job(conn, JobStatus.SUCCEEDED, completed_age_days=10) for _ in range(3)]
    fresh = _insert_job(conn, JobStatus.SUCCEEDED, completed_age_days=1)
    non_terminal = _insert_job(conn, JobStatus.QUEUED, completed_age_days=None)

    n = delete_terminal_jobs(conn, ttl_days=7)

    assert n == 3
    for jid in old_ids:
        assert not _exists(conn, jid)
    assert _exists(conn, fresh)
    assert _exists(conn, non_terminal)


def test_batching_deletes_all_rows(conn):
    ids = [_insert_job(conn, JobStatus.SUCCEEDED, completed_age_days=10) for _ in range(5)]

    # batch_size smaller than the row count forces multiple loop iterations
    # plus a final partial batch.
    n = delete_terminal_jobs(conn, ttl_days=7, batch_size=2)

    assert n == 5
    for jid in ids:
        assert not _exists(conn, jid)


def test_no_op_when_table_empty(conn):
    assert delete_terminal_jobs(conn, ttl_days=7) == 0

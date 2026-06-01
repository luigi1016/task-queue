from __future__ import annotations

from taskqueue import JobStatus, ack, dequeue, enqueue
from taskqueue.reaper import reclaim_expired_leases


def _expire_lease(conn, job_id) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
            (job_id,),
        )
    conn.commit()


def test_reaper_reclaims_expired_lease(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    job = dequeue(conn, worker_id="w1")
    assert job is not None
    _expire_lease(conn, job.id)

    n = reclaim_expired_leases(conn)
    assert n == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, worker_id, processed_by_worker_id, lease_expires_at, "
            "       attempt_count "
            "FROM jobs WHERE id = %s",
            (job.id,),
        )
        row = cur.fetchone()
    status, worker_id, processed_by_worker_id, lease_expires_at, attempt_count = row
    assert status == JobStatus.QUEUED
    assert worker_id is None
    # Poison-pill debugging: the worker that abandoned the lease is recorded
    # so we can answer "which worker died on this?" without grepping logs.
    assert processed_by_worker_id == "w1"
    assert lease_expires_at is None
    # attempt_count from the first dequeue is preserved; reaper doesn't touch it.
    assert attempt_count == 1


def test_reaper_ignores_fresh_lease(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    job = dequeue(conn, worker_id="w1")
    assert job is not None

    n = reclaim_expired_leases(conn)
    assert n == 0

    with conn.cursor() as cur:
        cur.execute("SELECT status FROM jobs WHERE id = %s", (job.id,))
        assert cur.fetchone()[0] == JobStatus.RUNNING


def test_reaper_ignores_completed_jobs(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    job = dequeue(conn, worker_id="w1")
    assert job is not None
    ack(conn, job_id=job.id)
    # ack already cleared lease_expires_at; backdate it to prove the status
    # filter (not the lease filter) is what protects completed rows.
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE jobs SET lease_expires_at = now() - interval '1 hour' WHERE id = %s",
            (job.id,),
        )
    conn.commit()

    n = reclaim_expired_leases(conn)
    assert n == 0

    with conn.cursor() as cur:
        cur.execute("SELECT status FROM jobs WHERE id = %s", (job.id,))
        assert cur.fetchone()[0] == JobStatus.SUCCEEDED


def test_reaper_returns_count_for_multiple_expired(conn):
    expired_ids = []
    for i in range(3):
        enqueue(conn, idempotency_key=f"exp-{i}", job_type="t", payload={})
        job = dequeue(conn, worker_id=f"w-{i}")
        assert job is not None
        _expire_lease(conn, job.id)
        expired_ids.append(job.id)

    # One fresh lease that should not be reaped.
    enqueue(conn, idempotency_key="fresh", job_type="t", payload={})
    fresh = dequeue(conn, worker_id="w-fresh")
    assert fresh is not None

    n = reclaim_expired_leases(conn)
    assert n == 3

    with conn.cursor() as cur:
        cur.execute("SELECT status FROM jobs WHERE id = %s", (fresh.id,))
        assert cur.fetchone()[0] == JobStatus.RUNNING


def test_reclaimed_job_can_be_redequeued(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    first = dequeue(conn, worker_id="w1")
    assert first is not None
    _expire_lease(conn, first.id)
    reclaim_expired_leases(conn)

    second = dequeue(conn, worker_id="w2")
    assert second is not None
    assert second.id == first.id
    assert second.worker_id == "w2"
    assert second.attempt_count == 2


def test_reaper_no_op_when_table_empty(conn):
    assert reclaim_expired_leases(conn) == 0

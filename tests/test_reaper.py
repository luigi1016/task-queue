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

    reclaimed, dead_lettered = reclaim_expired_leases(conn)
    assert reclaimed == 1
    assert dead_lettered == 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, worker_id, lease_expires_at, attempt_count "
            "FROM jobs WHERE id = %s",
            (job.id,),
        )
        row = cur.fetchone()
    status, worker_id, lease_expires_at, attempt_count = row
    assert status == JobStatus.QUEUED
    assert worker_id is None
    assert lease_expires_at is None
    # attempt_count from the first dequeue is preserved; reaper doesn't touch it.
    assert attempt_count == 1


def test_reaper_ignores_fresh_lease(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    job = dequeue(conn, worker_id="w1")
    assert job is not None

    assert reclaim_expired_leases(conn) == (0, 0)

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

    assert reclaim_expired_leases(conn) == (0, 0)

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

    reclaimed, dead_lettered = reclaim_expired_leases(conn)
    assert reclaimed == 3
    assert dead_lettered == 0

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
    assert reclaim_expired_leases(conn) == (0, 0)


def test_reaper_dead_letters_when_max_attempts_reached(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={}, max_attempts=2)
    job = dequeue(conn, worker_id="w1")
    assert job is not None
    # dequeue bumped attempt_count to 1; push it to max_attempts so the
    # reaper sees this as a poison-pill that's exhausted its retries.
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET attempt_count = 2 WHERE id = %s", (job.id,))
    conn.commit()
    _expire_lease(conn, job.id)

    reclaimed, dead_lettered = reclaim_expired_leases(conn)
    assert reclaimed == 0
    assert dead_lettered == 1

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, error_message, completed_at, worker_id, lease_expires_at "
            "FROM jobs WHERE id = %s",
            (job.id,),
        )
        row = cur.fetchone()
    status, error_message, completed_at, worker_id, lease_expires_at = row
    assert status == JobStatus.DEAD_LETTER
    assert error_message == "lease expired 2 times (max 2)"
    assert completed_at is not None
    assert worker_id is None
    assert lease_expires_at is None


def test_reaper_reclaims_when_under_max_attempts(conn):
    # attempt_count (1, set by dequeue) is below max_attempts (3) — boundary
    # check that the reaper does NOT dead-letter jobs still inside their budget.
    enqueue(conn, idempotency_key="k", job_type="t", payload={}, max_attempts=3)
    job = dequeue(conn, worker_id="w1")
    assert job is not None
    _expire_lease(conn, job.id)

    reclaimed, dead_lettered = reclaim_expired_leases(conn)
    assert reclaimed == 1
    assert dead_lettered == 0

    with conn.cursor() as cur:
        cur.execute("SELECT status FROM jobs WHERE id = %s", (job.id,))
        assert cur.fetchone()[0] == JobStatus.QUEUED

from __future__ import annotations

import uuid

import pytest

from taskqueue import (
    NOTIFY_DONE_CHANNEL,
    JobNotRunningError,
    JobStatus,
    ack,
    dequeue,
    enqueue,
)


def test_ack_marks_succeeded_and_stores_result(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    job = dequeue(conn, worker_id="w1")
    assert job is not None

    ack(conn, job_id=job.id, result_payload={"output": 42})

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, result_payload, completed_at, worker_id, lease_expires_at "
            "FROM jobs WHERE id = %s",
            (job.id,),
        )
        row = cur.fetchone()
    assert row is not None
    status, result_payload, completed_at, worker_id, lease_expires_at = row
    assert status == JobStatus.SUCCEEDED
    assert result_payload == {"output": 42}
    assert completed_at is not None
    assert worker_id is None
    assert lease_expires_at is None


def test_ack_with_no_result_payload_stores_null(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    job = dequeue(conn, worker_id="w1")
    assert job is not None

    ack(conn, job_id=job.id)

    with conn.cursor() as cur:
        cur.execute("SELECT status, result_payload FROM jobs WHERE id = %s", (job.id,))
        row = cur.fetchone()
    assert row == (JobStatus.SUCCEEDED, None)


def test_ack_fires_notify_on_jobs_done(conn, make_conn):
    listener = make_conn()
    listener.autocommit = True
    listener.execute(f"LISTEN {NOTIFY_DONE_CHANNEL}")

    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    job = dequeue(conn, worker_id="w1")
    assert job is not None

    ack(conn, job_id=job.id, result_payload={"ok": True})

    received = next(listener.notifies(timeout=2.0, stop_after=1), None)
    assert received is not None, "no NOTIFY received within timeout"
    assert received.channel == NOTIFY_DONE_CHANNEL
    assert received.payload == str(job.id)


def test_ack_on_queued_job_raises(conn):
    job_id = enqueue(conn, idempotency_key="k", job_type="t", payload={})
    with pytest.raises(JobNotRunningError) as exc_info:
        ack(conn, job_id=job_id)
    assert exc_info.value.job_id == job_id


def test_ack_twice_raises(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={})
    job = dequeue(conn, worker_id="w1")
    assert job is not None

    ack(conn, job_id=job.id)
    with pytest.raises(JobNotRunningError):
        ack(conn, job_id=job.id)


def test_ack_unknown_job_raises(conn):
    with pytest.raises(JobNotRunningError):
        ack(conn, job_id=uuid.uuid4())

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest

from taskqueue import (
    JobNotRunningError,
    JobStatus,
    NackOutcome,
    dequeue,
    enqueue,
    nack,
)
from taskqueue.queue import (
    BACKOFF_BASE_SECONDS,
    BACKOFF_JITTER_RATIO,
    _compute_backoff,
)


def test_nack_retries_and_sets_retry_after(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={}, max_attempts=3)
    job = dequeue(conn, worker_id="w1")
    assert job is not None

    outcome = nack(conn, job_id=job.id, error_message="boom")
    assert outcome is NackOutcome.RETRYING

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, retry_after, error_message, worker_id, "
            "       processed_by_worker_id, lease_expires_at, "
            "       attempt_count, retry_after > now() "
            "FROM jobs WHERE id = %s",
            (job.id,),
        )
        row = cur.fetchone()
    assert row is not None
    (
        status,
        retry_after,
        error_message,
        worker_id,
        processed_by_worker_id,
        lease,
        attempt_count,
        future,
    ) = row
    assert status == JobStatus.QUEUED
    assert retry_after is not None and future is True
    assert error_message == "boom"
    assert worker_id is None
    assert processed_by_worker_id == "w1"
    assert lease is None
    assert attempt_count == 1


def test_nack_routes_to_dead_letter_when_max_attempts_reached(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={}, max_attempts=2)

    # First failure: retries.
    first = dequeue(conn, worker_id="w1")
    assert first is not None
    assert nack(conn, job_id=first.id, error_message="fail-1") is NackOutcome.RETRYING

    # Bypass the retry_after delay so we can dequeue again immediately.
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET retry_after = NULL WHERE id = %s", (first.id,))
    conn.commit()

    # Second worker picks up the retried job. processed_by_worker_id should
    # track the most recent lease holder, so it ends up as "w2" not "w1".
    second = dequeue(conn, worker_id="w2")
    assert second is not None and second.id == first.id
    assert second.attempt_count == 2

    outcome = nack(conn, job_id=second.id, error_message="fail-2")
    assert outcome is NackOutcome.DEAD_LETTERED

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, error_message, completed_at, worker_id, "
            "       processed_by_worker_id, lease_expires_at "
            "FROM jobs WHERE id = %s",
            (second.id,),
        )
        row = cur.fetchone()
    status, error_message, completed_at, worker_id, processed_by_worker_id, lease = row
    assert status == JobStatus.DEAD_LETTER
    assert error_message == "fail-2"
    assert completed_at is not None
    assert worker_id is None
    assert processed_by_worker_id == "w2"
    assert lease is None


def test_nack_backoff_grows_across_retries(conn):
    enqueue(conn, idempotency_key="k", job_type="t", payload={}, max_attempts=10)

    deltas: list[float] = []
    for _ in range(3):
        job = dequeue(conn, worker_id="w1")
        assert job is not None
        assert nack(conn, job_id=job.id) is NackOutcome.RETRYING
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXTRACT(EPOCH FROM (retry_after - now())) FROM jobs WHERE id = %s",
                (job.id,),
            )
            deltas.append(float(cur.fetchone()[0]))
            cur.execute("UPDATE jobs SET retry_after = NULL WHERE id = %s", (job.id,))
        conn.commit()

    # Each delta should be larger than the previous by at least the un-jittered gap,
    # minus worst-case jitter on both sides.
    for i in range(1, len(deltas)):
        prev_max = BACKOFF_BASE_SECONDS * (2 ** (i - 1)) * (1 + BACKOFF_JITTER_RATIO)
        curr_min = BACKOFF_BASE_SECONDS * (2**i) * (1 - BACKOFF_JITTER_RATIO)
        assert curr_min > prev_max, (
            f"backoff windows overlap: attempt {i} max={prev_max}, attempt {i+1} min={curr_min}"
        )
        assert deltas[i] > deltas[i - 1]


def test_nack_on_queued_job_raises(conn):
    job_id = enqueue(conn, idempotency_key="k", job_type="t", payload={})
    with pytest.raises(JobNotRunningError) as exc_info:
        nack(conn, job_id=job_id)
    assert exc_info.value.job_id == job_id


def test_nack_unknown_job_raises(conn):
    with pytest.raises(JobNotRunningError):
        nack(conn, job_id=uuid.uuid4())


def test_compute_backoff_grows_and_caps():
    one = _compute_backoff(1)
    two = _compute_backoff(2)
    huge = _compute_backoff(100)

    # Attempt 1: 10s ± 20%.
    assert timedelta(seconds=8) <= one <= timedelta(seconds=12)
    # Attempt 2: 20s ± 20%.
    assert timedelta(seconds=16) <= two <= timedelta(seconds=24)
    # Capped at 1h ± 20%.
    assert timedelta(seconds=2880) <= huge <= timedelta(seconds=4320)


def test_compute_backoff_rejects_zero_attempt():
    with pytest.raises(ValueError):
        _compute_backoff(0)

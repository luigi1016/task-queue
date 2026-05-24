from __future__ import annotations

import threading
import time
import uuid

import pytest

from taskqueue import JobStatus, Worker, enqueue


def _wait_for(predicate, *, timeout: float = 5.0, interval: float = 0.05):
    """Spin until predicate() is truthy or timeout. Returns last value."""
    deadline = time.monotonic() + timeout
    value = predicate()
    while not value and time.monotonic() < deadline:
        time.sleep(interval)
        value = predicate()
    return value


def _row(conn, job_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, result_payload, error_message FROM jobs WHERE id = %s",
            (job_id,),
        )
        return cur.fetchone()


def _run_in_thread(worker: Worker) -> threading.Thread:
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    return t


def test_worker_processes_one_job_and_acks(conn):
    job_id = enqueue(
        conn,
        idempotency_key="w-1",
        job_type="echo",
        payload={"hello": "world"},
    )

    worker = Worker(
        handlers={"echo": lambda payload: {"echoed": payload}},
        worker_id="test-worker-serial",
        concurrency=1,
        poll_interval=0.2,
    )
    t = _run_in_thread(worker)
    try:
        got = _wait_for(lambda: _row(conn, job_id)[0] == JobStatus.SUCCEEDED)
        assert got, f"job never reached succeeded: row={_row(conn, job_id)}"
        status, result_payload, error_message = _row(conn, job_id)
        assert status == JobStatus.SUCCEEDED
        assert result_payload == {"echoed": {"hello": "world"}}
        assert error_message is None
    finally:
        worker.stop()
        t.join(timeout=2.0)
        assert not t.is_alive(), "worker thread did not stop within 2s"


def test_worker_nacks_on_handler_exception(conn):
    # max_attempts=1 so the first failure dead-letters immediately
    # (no waiting for the 10s exponential backoff floor).
    job_id = enqueue(
        conn,
        idempotency_key="w-2",
        job_type="boom",
        payload={},
        max_attempts=1,
    )

    def boom(_payload):
        raise RuntimeError("explosive failure")

    worker = Worker(
        handlers={"boom": boom},
        worker_id="test-worker-serial",
        concurrency=1,
        poll_interval=0.2,
    )
    t = _run_in_thread(worker)
    try:
        got = _wait_for(lambda: _row(conn, job_id)[0] == JobStatus.DEAD_LETTER)
        assert got, f"job never dead-lettered: row={_row(conn, job_id)}"
        status, _, error_message = _row(conn, job_id)
        assert status == JobStatus.DEAD_LETTER
        assert error_message == "explosive failure"
    finally:
        worker.stop()
        t.join(timeout=2.0)


def test_worker_nacks_unknown_job_type(conn):
    job_id = enqueue(
        conn,
        idempotency_key="w-3",
        job_type="unknown-type",
        payload={},
        max_attempts=1,
    )

    worker = Worker(
        handlers={},  # nothing registered
        worker_id="test-worker-serial",
        concurrency=1,
        poll_interval=0.2,
    )
    t = _run_in_thread(worker)
    try:
        got = _wait_for(lambda: _row(conn, job_id)[0] == JobStatus.DEAD_LETTER)
        assert got, f"job never dead-lettered: row={_row(conn, job_id)}"
        status, _, error_message = _row(conn, job_id)
        assert status == JobStatus.DEAD_LETTER
        assert "no handler" in (error_message or "")
        assert "unknown-type" in (error_message or "")
    finally:
        worker.stop()
        t.join(timeout=2.0)


def test_worker_pool_processes_many_jobs(conn):
    n_jobs = 12
    ids = []
    for i in range(n_jobs):
        ids.append(
            enqueue(
                conn,
                idempotency_key=f"pool-{uuid.uuid4()}",
                job_type="quick",
                payload={"i": i},
            )
        )

    worker = Worker(
        handlers={"quick": lambda payload: {"i": payload["i"]}},
        worker_id="test-worker-pool",
        concurrency=4,
        poll_interval=0.2,
    )
    t = _run_in_thread(worker)
    try:
        def all_done() -> bool:
            statuses = [_row(conn, jid)[0] for jid in ids]
            return all(s == JobStatus.SUCCEEDED for s in statuses)

        assert _wait_for(all_done, timeout=10.0), "not all pool jobs succeeded in time"
    finally:
        worker.stop()
        t.join(timeout=3.0)


def test_worker_rejects_invalid_concurrency():
    with pytest.raises(ValueError):
        Worker(handlers={}, worker_id="x", concurrency=0)

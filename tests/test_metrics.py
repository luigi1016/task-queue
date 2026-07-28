"""Prometheus instrumentation on the queue ops and worker.

Metrics live on the module-global default REGISTRY, which accumulates
across tests — every assertion here is a delta against a value captured
before acting, never an absolute. (For the same reason, never
importlib.reload(taskqueue.metrics): re-registering collectors raises.)
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest
from prometheus_client import REGISTRY

from taskqueue import (
    DuplicateJobError,
    JobStatus,
    NackOutcome,
    Worker,
    ack,
    dequeue,
    enqueue,
    nack,
)


def _sample(name: str, labels: dict[str, str] | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


def _key() -> str:
    return str(uuid.uuid4())


def _clear_retry_after(conn, job_id) -> None:
    """Make a backed-off retry immediately eligible for dequeue."""
    with conn.cursor() as cur:
        cur.execute("UPDATE jobs SET retry_after = NULL WHERE id = %s", (job_id,))
    conn.commit()


def test_enqueue_increments_counter(conn):
    labels = {"job_type": "metrics-echo"}
    before = _sample("taskqueue_jobs_enqueued_total", labels)

    enqueue(conn, idempotency_key=_key(), job_type="metrics-echo", payload={})

    assert _sample("taskqueue_jobs_enqueued_total", labels) == before + 1


def test_duplicate_enqueue_not_counted(conn):
    labels = {"job_type": "metrics-echo"}
    key = _key()
    enqueue(conn, idempotency_key=key, job_type="metrics-echo", payload={})
    before = _sample("taskqueue_jobs_enqueued_total", labels)

    with pytest.raises(DuplicateJobError):
        enqueue(conn, idempotency_key=key, job_type="metrics-echo", payload={})

    assert _sample("taskqueue_jobs_enqueued_total", labels) == before


def test_dequeue_increments_counter_and_observes_wait(conn):
    labels = {"job_type": "metrics-echo"}
    dequeued_before = _sample("taskqueue_jobs_dequeued_total", labels)
    wait_count_before = _sample("taskqueue_queue_wait_seconds_count", labels)

    enqueue(conn, idempotency_key=_key(), job_type="metrics-echo", payload={})
    job = dequeue(conn, worker_id="metrics-test")

    assert job is not None
    assert _sample("taskqueue_jobs_dequeued_total", labels) == dequeued_before + 1
    assert _sample("taskqueue_queue_wait_seconds_count", labels) == wait_count_before + 1
    # DB-clock difference for a just-enqueued job should be tiny but >= 0.
    assert _sample("taskqueue_queue_wait_seconds_sum", labels) >= 0


def test_empty_dequeue_counts_nothing(conn):
    labels = {"job_type": "metrics-echo"}
    before = _sample("taskqueue_jobs_dequeued_total", labels)

    assert dequeue(conn, worker_id="metrics-test") is None

    assert _sample("taskqueue_jobs_dequeued_total", labels) == before


def test_retry_dequeue_skips_wait_histogram(conn):
    labels = {"job_type": "metrics-echo"}
    job_id = enqueue(conn, idempotency_key=_key(), job_type="metrics-echo", payload={})
    job = dequeue(conn, worker_id="metrics-test")
    assert job is not None and job.id == job_id
    assert nack(conn, job_id=job_id) is NackOutcome.RETRYING
    _clear_retry_after(conn, job_id)

    wait_count_before = _sample("taskqueue_queue_wait_seconds_count", labels)
    dequeued_before = _sample("taskqueue_jobs_dequeued_total", labels)

    retry = dequeue(conn, worker_id="metrics-test")

    assert retry is not None and retry.attempt_count == 2
    # The retry still counts as a dequeue, but its wait includes deliberate
    # backoff, so the queue-wait histogram must not observe it.
    assert _sample("taskqueue_jobs_dequeued_total", labels) == dequeued_before + 1
    assert _sample("taskqueue_queue_wait_seconds_count", labels) == wait_count_before


def test_ack_increments_counter_and_observes_e2e(conn):
    labels = {"job_type": "metrics-echo"}
    job_id = enqueue(conn, idempotency_key=_key(), job_type="metrics-echo", payload={})
    dequeue(conn, worker_id="metrics-test")

    acked_before = _sample("taskqueue_jobs_acked_total", labels)
    e2e_count_before = _sample("taskqueue_job_e2e_seconds_count", labels)

    ack(conn, job_id=job_id)

    assert _sample("taskqueue_jobs_acked_total", labels) == acked_before + 1
    assert _sample("taskqueue_job_e2e_seconds_count", labels) == e2e_count_before + 1


def test_nack_counts_by_outcome(conn):
    retry_labels = {"job_type": "metrics-echo", "outcome": "retrying"}
    dead_labels = {"job_type": "metrics-echo", "outcome": "dead_lettered"}
    retry_before = _sample("taskqueue_jobs_nacked_total", retry_labels)
    dead_before = _sample("taskqueue_jobs_nacked_total", dead_labels)

    job_id = enqueue(
        conn, idempotency_key=_key(), job_type="metrics-echo", payload={}, max_attempts=2
    )
    dequeue(conn, worker_id="metrics-test")
    assert nack(conn, job_id=job_id) is NackOutcome.RETRYING
    _clear_retry_after(conn, job_id)
    dequeue(conn, worker_id="metrics-test")
    assert nack(conn, job_id=job_id) is NackOutcome.DEAD_LETTERED

    assert _sample("taskqueue_jobs_nacked_total", retry_labels) == retry_before + 1
    assert _sample("taskqueue_jobs_nacked_total", dead_labels) == dead_before + 1


def _wait_for(predicate, *, timeout: float = 5.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    value = predicate()
    while not value and time.monotonic() < deadline:
        time.sleep(interval)
        value = predicate()
    return value


def test_worker_in_flight_gauge_and_handler_duration(conn):
    labels = {"job_type": "metrics-block"}
    duration_before = _sample("taskqueue_handler_duration_seconds_count", labels)
    in_flight_before = _sample("taskqueue_jobs_in_flight")

    entered = threading.Event()
    release = threading.Event()

    def blocking_handler(_payload):
        entered.set()
        assert release.wait(timeout=5.0)
        return {}

    enqueue(conn, idempotency_key=_key(), job_type="metrics-block", payload={})
    worker = Worker(
        handlers={"metrics-block": blocking_handler},
        worker_id="metrics-test-worker",
        poll_interval=0.2,
    )
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    try:
        assert entered.wait(timeout=5.0), "handler never started"
        assert _sample("taskqueue_jobs_in_flight") == in_flight_before + 1
        release.set()
        assert _wait_for(
            lambda: _sample("taskqueue_jobs_in_flight") == in_flight_before
        ), "in-flight gauge never returned to baseline"
        assert (
            _sample("taskqueue_handler_duration_seconds_count", labels)
            == duration_before + 1
        )
    finally:
        release.set()
        worker.stop()
        t.join(timeout=2.0)
        assert not t.is_alive()


def test_handler_duration_observed_on_failure(conn):
    labels = {"job_type": "metrics-boom"}
    duration_before = _sample("taskqueue_handler_duration_seconds_count", labels)

    def boom(_payload):
        raise RuntimeError("kaboom")

    job_id = enqueue(
        conn, idempotency_key=_key(), job_type="metrics-boom", payload={}, max_attempts=1
    )
    worker = Worker(
        handlers={"metrics-boom": boom},
        worker_id="metrics-test-worker",
        poll_interval=0.2,
    )
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    try:
        def dead_lettered() -> bool:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
                row = cur.fetchone()
            return row is not None and row[0] == JobStatus.DEAD_LETTER

        assert _wait_for(dead_lettered), "job never dead-lettered"
        assert (
            _sample("taskqueue_handler_duration_seconds_count", labels)
            == duration_before + 1
        )
    finally:
        worker.stop()
        t.join(timeout=2.0)

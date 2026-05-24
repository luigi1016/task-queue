"""End-to-end test: run the actual demo service entrypoints against Postgres.

This exercises the same code paths that get deployed in k8s — the demo
producer enqueues random jobs, the demo worker dequeues + dispatches +
acks/nacks. The test asserts every job reaches a terminal state and none
get stuck in queued/running.

``max_attempts=1`` for produced jobs so flaky failures dead-letter on the
first try; otherwise the 10s exponential backoff floor would dominate the
test runtime.
"""

from __future__ import annotations

import random
import threading
import time
import uuid

import pytest

import taskqueue
from taskqueue import JobStatus, db
from taskqueue.registry import clear_registry

from demo_service.handlers import FLAKY, JOB_TYPES
from demo_service.worker_main import build_worker


@pytest.fixture(autouse=True)
def _clean_registry_around_e2e():
    """The demo's @task decorators populate the default registry on import.

    Other tests (e.g. test_registry.py) call clear_registry() between
    cases, which would wipe the demo's registrations. Re-importing the
    demo's handlers module here re-runs the decorators so the e2e worker
    always sees them, regardless of test ordering.
    """
    import importlib
    import demo_service.handlers

    clear_registry()
    importlib.reload(demo_service.handlers)
    yield


def _produce_bounded(n: int, max_attempts: int = 1) -> None:
    """Enqueue exactly ``n`` random jobs as fast as possible, then return.

    Mirrors ``demo_service.producer_main.run_loop`` minus the inter-job
    sleep and stop event — we want a finite stream for the test rather
    than the infinite production loop.
    """
    for _ in range(n):
        job_type = random.choice(JOB_TYPES)
        payload: dict[str, float] = {"duration_s": round(random.uniform(0.02, 0.1), 3)}
        if job_type == FLAKY:
            payload["fail_rate"] = round(random.uniform(0.2, 0.5), 3)
        with db.get_connection() as c:
            taskqueue.enqueue(
                c,
                idempotency_key=str(uuid.uuid4()),
                job_type=job_type,
                payload=payload,
                priority=random.randint(0, 9),
                max_attempts=max_attempts,
            )


def _all_terminal(conn) -> tuple[int, int, int]:
    """Return ``(non_terminal, succeeded, dead_lettered)``."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, count(*) FROM jobs GROUP BY status",
        )
        counts = {status: n for status, n in cur.fetchall()}
    non_terminal = counts.get(JobStatus.QUEUED, 0) + counts.get(JobStatus.RUNNING, 0)
    succeeded = counts.get(JobStatus.SUCCEEDED, 0)
    dead = counts.get(JobStatus.DEAD_LETTER, 0)
    return non_terminal, succeeded, dead


def _run_demo_e2e(conn, monkeypatch, *, concurrency: int, n_jobs: int) -> None:
    monkeypatch.setenv("WORKER_CONCURRENCY", str(concurrency))
    monkeypatch.setenv("WORKER_ID", f"e2e-worker-{concurrency}")
    monkeypatch.setenv("POLL_INTERVAL_S", "0.2")
    monkeypatch.setenv("LEASE_SECONDS", "60")

    worker = build_worker()
    worker_thread = threading.Thread(target=worker.run, daemon=True)
    worker_thread.start()

    try:
        _produce_bounded(n_jobs, max_attempts=1)

        deadline = time.monotonic() + 30.0
        non_terminal, succeeded, dead = _all_terminal(conn)
        while non_terminal > 0 and time.monotonic() < deadline:
            time.sleep(0.2)
            non_terminal, succeeded, dead = _all_terminal(conn)

        assert non_terminal == 0, (
            f"jobs stuck in queued/running after timeout: "
            f"non_terminal={non_terminal} succeeded={succeeded} dead={dead}"
        )
        assert succeeded + dead == n_jobs, (
            f"total terminal jobs ({succeeded + dead}) != enqueued ({n_jobs})"
        )
    finally:
        worker.stop()
        worker_thread.join(timeout=5.0)
        assert not worker_thread.is_alive(), "worker thread did not stop"


@pytest.mark.parametrize("concurrency,n_jobs", [(1, 15), (4, 30)])
def test_demo_e2e(conn, monkeypatch, concurrency, n_jobs):
    _run_demo_e2e(conn, monkeypatch, concurrency=concurrency, n_jobs=n_jobs)

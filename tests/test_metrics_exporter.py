"""Queue-state exporter: gauges computed from Postgres on each collect().

The collector is exercised directly through its registry — no HTTP server.
It connects via DATABASE_URL, which conftest mirrors from TEST_DATABASE_URL,
so it reads the same database the ``conn`` fixture seeds.
"""

from __future__ import annotations

import uuid

from prometheus_client import generate_latest

from taskqueue import enqueue
from taskqueue.metrics_exporter import build_registry
from taskqueue.models import JobStatus


def _key() -> str:
    return str(uuid.uuid4())


def _depth(registry, status: str) -> float | None:
    return registry.get_sample_value("taskqueue_queue_depth", {"status": status})


def test_empty_table_zero_fills_all_statuses(conn):
    registry = build_registry()
    for status in JobStatus:
        assert _depth(registry, status.value) == 0
    assert registry.get_sample_value("taskqueue_dead_letter_jobs") == 0
    # No queued jobs → no backlog series at all (not a zero series).
    assert (
        registry.get_sample_value(
            "taskqueue_backlog_age_seconds", {"job_type": "anything"}
        )
        is None
    )


def test_queue_depth_counts_by_status(conn):
    for _ in range(3):
        enqueue(conn, idempotency_key=_key(), job_type="depth-test", payload={})
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (idempotency_key, job_type, payload, status, completed_at)
            VALUES (%s, 'depth-test', '{}', 'dead_letter', now()),
                   (%s, 'depth-test', '{}', 'succeeded', now())
            """,
            (_key(), _key()),
        )
    conn.commit()

    registry = build_registry()
    assert _depth(registry, "queued") == 3
    assert _depth(registry, "dead_letter") == 1
    assert _depth(registry, "succeeded") == 1
    assert _depth(registry, "running") == 0
    assert registry.get_sample_value("taskqueue_dead_letter_jobs") == 1


def test_backlog_age_reflects_oldest_queued_job(conn):
    enqueue(conn, idempotency_key=_key(), job_type="backlog-test", payload={})
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs SET created_at = now() - interval '120 seconds'
            WHERE job_type = 'backlog-test'
            """
        )
    conn.commit()
    # A younger queued job of the same type must not shrink the age.
    enqueue(conn, idempotency_key=_key(), job_type="backlog-test", payload={})

    registry = build_registry()
    age = registry.get_sample_value(
        "taskqueue_backlog_age_seconds", {"job_type": "backlog-test"}
    )
    assert age is not None
    assert 115 <= age <= 180


def test_reclaimable_counts_running_jobs_with_expired_lease(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO jobs (idempotency_key, job_type, payload, status, lease_expires_at)
            VALUES (%s, 'reap-test', '{}', 'running', now() - interval '30 seconds'),
                   (%s, 'reap-test', '{}', 'running', now() + interval '30 seconds')
            """,
            (_key(), _key()),
        )
    conn.commit()

    registry = build_registry()
    # Only the expired-lease running job is reclaimable; the live-lease one isn't.
    assert registry.get_sample_value("taskqueue_reclaimable_jobs") == 1


def test_gauges_are_fresh_per_collect(conn):
    registry = build_registry()
    assert _depth(registry, "queued") == 0

    enqueue(conn, idempotency_key=_key(), job_type="fresh-test", payload={})

    # Same registry, no re-registration: the SQL runs on every collect.
    assert _depth(registry, "queued") == 1


def test_generate_latest_smoke(conn):
    enqueue(conn, idempotency_key=_key(), job_type="smoke-test", payload={})

    output = generate_latest(build_registry()).decode()

    assert 'taskqueue_queue_depth{status="queued"} 1.0' in output
    assert "taskqueue_dead_letter_jobs 0.0" in output
    assert 'taskqueue_backlog_age_seconds{job_type="smoke-test"}' in output

from __future__ import annotations

import threading
import time

import pytest

from taskqueue import JobStatus, Worker, enqueue, task
from taskqueue.registry import clear_registry, registered_handlers


@pytest.fixture(autouse=True)
def _clean_registry():
    """The registry is module-level state; isolate every test."""
    clear_registry()
    yield
    clear_registry()


def test_decorator_registers_handler():
    @task("foo")
    def foo_handler(payload):
        return {"got": payload}

    assert registered_handlers() == {"foo": foo_handler}


def test_decorator_returns_original_function():
    @task("echo")
    def echo(payload):
        return {"echoed": payload}

    # The decorated name is still directly callable as a plain function.
    assert echo({"hello": "world"}) == {"echoed": {"hello": "world"}}


def test_duplicate_registration_raises():
    @task("dup")
    def first(payload):
        return None

    with pytest.raises(ValueError, match="already registered"):

        @task("dup")
        def second(payload):
            return None


def test_registered_handlers_returns_copy():
    @task("a")
    def a_handler(payload):
        return None

    snapshot = registered_handlers()
    snapshot["b"] = lambda p: None  # mutating the copy must not affect the registry

    assert "b" not in registered_handlers()


def test_clear_registry_empties_it():
    @task("a")
    def a_handler(payload):
        return None

    assert registered_handlers() != {}
    clear_registry()
    assert registered_handlers() == {}


def _wait_for(predicate, *, timeout: float = 5.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    value = predicate()
    while not value and time.monotonic() < deadline:
        time.sleep(interval)
        value = predicate()
    return value


def _row(conn, job_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, result_payload FROM jobs WHERE id = %s",
            (job_id,),
        )
        return cur.fetchone()


def test_worker_picks_up_registry_when_handlers_none(conn):
    # End-to-end integration: register via decorator, construct Worker with
    # no explicit handlers, prove the registered handler actually runs.
    @task("registry-echo")
    def echo(payload):
        return {"echoed": payload}

    job_id = enqueue(
        conn,
        idempotency_key="registry-1",
        job_type="registry-echo",
        payload={"hello": "world"},
    )

    worker = Worker(worker_id="test-registry", concurrency=1, poll_interval=0.2)
    t = threading.Thread(target=worker.run, daemon=True)
    t.start()
    try:
        assert _wait_for(lambda: _row(conn, job_id)[0] == JobStatus.SUCCEEDED), (
            f"job never succeeded: row={_row(conn, job_id)}"
        )
        status, result = _row(conn, job_id)
        assert status == JobStatus.SUCCEEDED
        assert result == {"echoed": {"hello": "world"}}
    finally:
        worker.stop()
        t.join(timeout=2.0)

from __future__ import annotations

import uuid

import pytest

from taskqueue import NOTIFY_NEW_CHANNEL, DuplicateJobError, JobStatus, enqueue


def test_enqueue_returns_uuid_and_inserts_row(conn):
    job_id = enqueue(
        conn,
        idempotency_key="key-1",
        job_type="email",
        payload={"to": "a@b.com"},
        priority=5,
    )
    assert isinstance(job_id, uuid.UUID)
    with conn.cursor() as cur:
        cur.execute("SELECT id, status, priority, payload FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    assert row is not None
    assert row[1] == JobStatus.QUEUED
    assert row[2] == 5
    assert row[3] == {"to": "a@b.com"}


def test_duplicate_idempotency_key_raises(conn):
    enqueue(conn, idempotency_key="dup", job_type="t", payload={})
    with pytest.raises(DuplicateJobError) as exc_info:
        enqueue(conn, idempotency_key="dup", job_type="t", payload={})
    assert exc_info.value.idempotency_key == "dup"
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM jobs WHERE idempotency_key = 'dup'")
        assert cur.fetchone()[0] == 1


def test_enqueue_emits_notify(conn, make_conn):
    listener = make_conn()
    listener.autocommit = True
    listener.execute(f"LISTEN {NOTIFY_NEW_CHANNEL}")

    job_id = enqueue(conn, idempotency_key="notify-1", job_type="t", payload={})

    received = next(listener.notifies(timeout=2.0, stop_after=1), None)
    assert received is not None, "no NOTIFY received within timeout"
    assert received.channel == NOTIFY_NEW_CHANNEL
    assert received.payload == str(job_id)

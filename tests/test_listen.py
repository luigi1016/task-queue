from __future__ import annotations

import threading
import time

from taskqueue import NOTIFY_NEW_CHANNEL, enqueue, listen


def test_listen_times_out_when_no_notify(make_conn):
    listener = make_conn()
    listener.autocommit = True
    listener.execute(f"LISTEN {NOTIFY_NEW_CHANNEL}")

    start = time.monotonic()
    received = listen(listener, poll_interval=0.3)
    elapsed = time.monotonic() - start

    assert received is False
    # Allow a small slack on the floor; the upper bound catches a regression
    # to a much longer (or infinite) wait.
    assert 0.25 <= elapsed < 2.0, f"unexpected elapsed time {elapsed:.3f}s"


def test_listen_returns_true_on_notify(conn, make_conn):
    listener = make_conn()
    listener.autocommit = True
    listener.execute(f"LISTEN {NOTIFY_NEW_CHANNEL}")

    def enqueue_after_delay() -> None:
        time.sleep(0.2)
        enqueue(conn, idempotency_key="listen-1", job_type="t", payload={})

    t = threading.Thread(target=enqueue_after_delay, daemon=True)
    t.start()
    try:
        start = time.monotonic()
        received = listen(listener, poll_interval=2.0)
        elapsed = time.monotonic() - start
    finally:
        t.join(timeout=2.0)

    assert received is True
    # Should wake well before the 2s timeout.
    assert elapsed < 1.5, f"listen() didn't wake on NOTIFY (waited {elapsed:.3f}s)"

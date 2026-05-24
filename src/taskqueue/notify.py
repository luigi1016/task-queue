"""LISTEN/NOTIFY helpers for workers waiting on the queue.

The queue layer (``queue.py``) fires ``pg_notify`` from ``enqueue`` and
``ack`` so consumers can wake up immediately when there is work to do.
This module gives consumers the matching block-until-notified primitive.

The connection passed in must be in autocommit mode and must have already
issued ``LISTEN <channel>`` — both are connection-lifecycle concerns owned
by the caller (a worker typically). Postgres only delivers notifications
at transaction boundaries; without autocommit, the listener would have to
commit between every wait, which is awkward.
"""

from __future__ import annotations

import psycopg


def listen(conn: psycopg.Connection, *, poll_interval: float = 5.0) -> bool:
    """Block until a NOTIFY arrives or ``poll_interval`` seconds elapse.

    Returns ``True`` if a notification was received, ``False`` on timeout.
    The fallback timeout is what lets the worker re-check the queue even
    if a NOTIFY was missed (e.g. brief network blip, or a job whose
    ``retry_after`` just elapsed without re-firing a NOTIFY).
    """
    notify = next(conn.notifies(timeout=poll_interval, stop_after=1), None)
    return notify is not None

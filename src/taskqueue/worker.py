"""Reusable worker loop.

Consumers register their handlers (a ``dict[str, Callable]``) and call
``Worker(...).run()``. The worker dequeues jobs, dispatches by ``job_type``,
acks on success, nacks on exception, and uses ``listen()`` for idle waiting
between polls.

Two execution modes:

- ``concurrency=1`` (default): single-threaded dequeue → handler → ack/nack
  loop. New jobs arriving while idle wake the worker via NOTIFY within
  milliseconds.

- ``concurrency>1``: a ``ThreadPoolExecutor`` of N handler threads. The main
  thread keeps the pool full. Each handler thread uses its own DB
  connections (psycopg connections are not thread-safe across operations).
  Trade-off: in pool mode, NOTIFY only wakes us when the pool is fully idle.
  When the pool has any in-flight job, the main thread blocks on
  ``concurrent.futures.wait`` which only wakes on completion, so the
  new-job latency floor while busy is ``poll_interval``.

The library does not install signal handlers — that's the consumer's job in
its ``main()``. ``signal.signal`` raises in non-main threads, and tests run
``Worker.run()`` from a worker thread. Use ``worker.stop()`` instead.
"""

from __future__ import annotations

import concurrent.futures
import logging
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Callable

import psycopg

from taskqueue import db
from taskqueue.models import Job
from taskqueue.notify import listen
from taskqueue.queue import (
    NOTIFY_NEW_CHANNEL,
    JobNotRunningError,
    ack,
    dequeue,
    nack,
)

logger = logging.getLogger(__name__)

HandlerFn = Callable[[dict[str, Any]], dict[str, Any] | None]


class Worker:
    """A queue worker that dispatches jobs to handler functions.

    Parameters
    ----------
    handlers : dict[str, HandlerFn] or None, default None
        Maps ``job_type`` → handler. The handler receives the job's payload
        and returns a result dict (stored as ``result_payload`` on the row)
        or ``None``. Raising any exception is treated as failure and the
        job is nacked (retry-with-backoff or dead-letter, depending on the
        job's remaining attempts).

        When ``None`` (the default), the worker falls back to
        ``taskqueue.registry.registered_handlers()`` — i.e. everything
        registered via the ``@taskqueue.task`` decorator at import time.
        Pass an explicit dict to bypass the registry (useful for tests or
        for running multiple workers with different handler sets in one
        process).
    worker_id : str
        Recorded on each claimed row. Use a stable, identifiable string
        (typically the pod's hostname) so leases are traceable.
    concurrency : int, default 1
        Number of jobs to process in parallel. 1 = no thread pool.
    lease_seconds : int, default 60
        Passed through to ``dequeue``. If a handler runs longer than this,
        the reaper may reclaim the job and a second worker may pick it up
        (at-least-once delivery).
    poll_interval : float, default 5.0
        Maximum time ``listen()`` blocks before re-checking the queue. Also
        the wake granularity for the pool-mode main loop.
    """

    def __init__(
        self,
        *,
        handlers: dict[str, HandlerFn] | None = None,
        worker_id: str,
        concurrency: int = 1,
        lease_seconds: int = 60,
        poll_interval: float = 5.0,
    ):
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        if handlers is None:
            # Late import so worker.py doesn't pull in registry at import
            # time (keeps the dependency direction one-way).
            from taskqueue.registry import registered_handlers
            handlers = registered_handlers()
        self.handlers = handlers
        self.worker_id = worker_id
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self._stop = threading.Event()

    def stop(self) -> None:
        """Signal the worker loop to exit at its next wake-up.

        Any in-flight handlers continue running until they finish — the pool's
        ``__exit__`` blocks on them. ``poll_interval`` is the worst-case wait
        before the loop notices the stop signal.
        """
        self._stop.set()

    def run(self) -> None:
        """Run the worker loop until ``stop()`` is called.

        Holds one dedicated autocommit connection for LISTEN/NOTIFY and opens
        fresh per-operation connections for dequeue/ack/nack. (No connection
        pool yet — a future psycopg_pool migration could amortize the connect
        cost under load.)
        """
        listen_conn = self._make_listen_conn()
        try:
            if self.concurrency == 1:
                self._run_serial(listen_conn)
            else:
                self._run_pool(listen_conn)
        finally:
            listen_conn.close()

    def _make_listen_conn(self) -> psycopg.Connection:
        conn = db.get_connection()
        conn.autocommit = True
        conn.execute(f"LISTEN {NOTIFY_NEW_CHANNEL}")
        return conn

    def _run_serial(self, listen_conn: psycopg.Connection) -> None:
        while not self._stop.is_set():
            with db.get_connection() as conn:
                job = dequeue(
                    conn,
                    worker_id=self.worker_id,
                    lease_seconds=self.lease_seconds,
                )
            if job is None:
                listen(listen_conn, poll_interval=self.poll_interval)
            else:
                self._process(job)

    def _run_pool(self, listen_conn: psycopg.Connection) -> None:
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            in_flight: set[Future] = set()
            while not self._stop.is_set():
                done = {f for f in in_flight if f.done()}
                for f in done:
                    exc = f.exception()
                    if exc is not None:
                        logger.exception("worker task crashed", exc_info=exc)
                in_flight -= done

                while len(in_flight) < self.concurrency and not self._stop.is_set():
                    with db.get_connection() as conn:
                        job = dequeue(
                            conn,
                            worker_id=self.worker_id,
                            lease_seconds=self.lease_seconds,
                        )
                    if job is None:
                        break
                    in_flight.add(pool.submit(self._process, job))

                if not in_flight:
                    listen(listen_conn, poll_interval=self.poll_interval)
                else:
                    wait(
                        in_flight,
                        timeout=self.poll_interval,
                        return_when=FIRST_COMPLETED,
                    )
            # Pool's __exit__ waits for in-flight handlers to drain.

    def _process(self, job: Job) -> None:
        handler = self.handlers.get(job.job_type)
        if handler is None:
            self._fail(job, f"no handler registered for job_type={job.job_type!r}")
            return
        try:
            result = handler(job.payload)
        except Exception as exc:
            logger.exception("handler raised for job %s (type=%s)", job.id, job.job_type)
            self._fail(job, str(exc) or exc.__class__.__name__)
            return
        self._succeed(job, result)

    def _succeed(self, job: Job, result: dict[str, Any] | None) -> None:
        try:
            with db.get_connection() as conn:
                ack(conn, job_id=job.id, result_payload=result or {})
        except JobNotRunningError:
            # Lease expired and reaper put the job back, or someone else
            # already finalized it. Log and move on — we no longer own it.
            logger.warning("ack failed: job %s no longer in running state", job.id)

    def _fail(self, job: Job, error_message: str) -> None:
        try:
            with db.get_connection() as conn:
                nack(conn, job_id=job.id, error_message=error_message)
        except JobNotRunningError:
            logger.warning("nack failed: job %s no longer in running state", job.id)

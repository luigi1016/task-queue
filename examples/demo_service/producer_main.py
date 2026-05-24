"""Demo producer entry point — enqueues random jobs in a loop.

A real producer would enqueue jobs in response to user actions, scheduled
triggers, webhook events, etc. The pattern is the same: open a connection,
call ``taskqueue.enqueue(...)``, close.
"""

from __future__ import annotations

import logging
import os
import random
import signal
import threading
import uuid

import taskqueue
from taskqueue import db

from demo_service.handlers import JOB_TYPES

logger = logging.getLogger(__name__)


def _build_random_job() -> tuple[str, dict[str, float], int]:
    job_type = random.choice(JOB_TYPES)
    payload: dict[str, float] = {"duration_s": round(random.uniform(0.05, 0.5), 3)}
    if job_type == "flaky":
        payload["fail_rate"] = round(random.uniform(0.1, 0.4), 3)
    priority = random.randint(0, int(os.environ.get("PRODUCER_MAX_PRIORITY", "9")))
    return job_type, payload, priority


def run_loop(stop: threading.Event) -> int:
    """Enqueue random jobs until ``stop`` is set. Returns the number enqueued.

    Pulled out of ``main`` so it can be reused (or wrapped with a job-count
    cap) from tests without bringing in signal handlers.
    """
    interval = float(os.environ.get("PRODUCER_INTERVAL_S", "1.0"))
    max_attempts = int(os.environ.get("PRODUCER_MAX_ATTEMPTS", "3"))
    enqueued = 0
    while not stop.is_set():
        job_type, payload, priority = _build_random_job()
        with db.get_connection() as conn:
            taskqueue.enqueue(
                conn,
                idempotency_key=str(uuid.uuid4()),
                job_type=job_type,
                payload=payload,
                priority=priority,
                max_attempts=max_attempts,
            )
        enqueued += 1
        logger.info("enqueued job_type=%s priority=%d payload=%s", job_type, priority, payload)
        stop.wait(interval)
    return enqueued


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    logger.info("demo producer starting")
    n = run_loop(stop)
    logger.info("demo producer stopped after enqueueing %d jobs", n)


if __name__ == "__main__":
    main()

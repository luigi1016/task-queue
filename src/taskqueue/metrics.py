"""Prometheus metric definitions for the queue's RED signals.

All metrics live on the default ``prometheus_client`` registry and are
inert until a process calls ``maybe_start_metrics_server()`` (or serves the
registry some other way) — library users who never expose ``/metrics`` pay
only an in-memory counter increment.

Two things to keep in mind when reading dashboards:

- Registries are per-process. ``jobs_enqueued_total`` counts enqueues made
  *by that process* (typically a producer), while dequeue/ack/nack live in
  worker processes. Cross-check totals against the queue-state exporter's
  gauges, which read the database directly.
- ``job_type`` is a label on most metrics, so it must remain a small,
  fixed set. Never derive it from user input.
"""

from __future__ import annotations

import logging
import os

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

# Wait/e2e latencies are dominated by queue time: sub-second when workers
# are idle, minutes when there's a backlog worth alerting on.
WAIT_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)

# Handler runtime. The ceiling tracks the default lease (60 s) — anything
# slower is at risk of being reaped and should be visible in the +Inf bucket.
HANDLER_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)

JOBS_ENQUEUED = Counter(
    "taskqueue_jobs_enqueued_total",
    "Jobs successfully enqueued (committed inserts only).",
    ["job_type"],
)

JOBS_DEQUEUED = Counter(
    "taskqueue_jobs_dequeued_total",
    "Jobs claimed by a worker (includes retry attempts).",
    ["job_type"],
)

JOBS_ACKED = Counter(
    "taskqueue_jobs_acked_total",
    "Jobs acknowledged as succeeded.",
    ["job_type"],
)

JOBS_NACKED = Counter(
    "taskqueue_jobs_nacked_total",
    "Jobs negatively acknowledged, by outcome (retrying | dead_lettered).",
    ["job_type", "outcome"],
)

QUEUE_WAIT_SECONDS = Histogram(
    "taskqueue_queue_wait_seconds",
    "Time from enqueue to first dequeue (DB clock). First attempt only — "
    "retries wait out a deliberate backoff and would skew the SLI.",
    ["job_type"],
    buckets=WAIT_BUCKETS,
)

HANDLER_DURATION_SECONDS = Histogram(
    "taskqueue_handler_duration_seconds",
    "Wall-clock handler runtime, observed on success and failure.",
    ["job_type"],
    buckets=HANDLER_BUCKETS,
)

JOB_E2E_SECONDS = Histogram(
    "taskqueue_job_e2e_seconds",
    "Time from enqueue to successful completion (DB clock), including any "
    "retries and backoff.",
    ["job_type"],
    buckets=WAIT_BUCKETS,
)

JOBS_IN_FLIGHT = Gauge(
    "taskqueue_jobs_in_flight",
    "Jobs currently being processed by this worker process.",
)


def maybe_start_metrics_server() -> int | None:
    """Expose the default registry on METRICS_PORT (default 9090).

    Returns the port, or None when disabled via ``METRICS_PORT=0``. Meant
    to be called once from a consumer's ``main()`` — the library itself
    never starts a server.
    """
    port = int(os.environ.get("METRICS_PORT", "9090"))
    if port == 0:
        logger.info("metrics server disabled (METRICS_PORT=0)")
        return None
    start_http_server(port)
    logger.info("metrics server listening on :%d/metrics", port)
    return port

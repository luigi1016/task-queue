"""Queue-state Prometheus exporter — gauges computed from Postgres.

Complements the per-process RED metrics in ``taskqueue.metrics``: those
count what each producer/worker process *did*, while this exporter reports
what the database *is* — queue depth, backlog age, dead-letter population —
regardless of which process (or no process) touched a row.

Runs as its own long-lived service (``ROLE=exporter`` in entrypoint.sh).
The SQL (see ``taskqueue.queries.metrics``) executes on every scrape via a
custom Collector, so values are always current — no refresh loop, no
staleness window. A scrape costs one connection and three cheap queries;
at Prometheus' 15 s interval that's negligible.

If the database is unreachable the scrape fails and the target shows
``up == 0`` — exactly the signal you want, rather than stale gauges.
"""

from __future__ import annotations

import logging
import os
import signal
import threading

from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector

from taskqueue import db
from taskqueue.models import JobStatus
from taskqueue.queries import metrics as queries

logger = logging.getLogger(__name__)


class QueueStateCollector(Collector):
    """Collector that reads queue-state gauges from Postgres per scrape."""

    def collect(self):
        with db.get_connection() as conn, conn.cursor() as cur:
            depth = GaugeMetricFamily(
                "taskqueue_queue_depth",
                "Jobs currently in each status.",
                labels=["status"],
            )
            cur.execute(queries.STATUS_COUNTS)
            counts = dict(cur.fetchall())
            # Zero-fill so every status is always present — a queue_depth
            # series that disappears reads as "no data", not "zero".
            for status in JobStatus:
                depth.add_metric([status.value], counts.get(status.value, 0))
            yield depth

            backlog = GaugeMetricFamily(
                "taskqueue_backlog_age_seconds",
                "Age of the oldest queued job, per job_type.",
                labels=["job_type"],
            )
            cur.execute(queries.BACKLOG_AGE_BY_TYPE)
            for job_type, age_seconds in cur.fetchall():
                backlog.add_metric([job_type], float(age_seconds))
            yield backlog

            cur.execute(queries.DEAD_LETTER_COUNT)
            row = cur.fetchone()
            assert row is not None
            yield GaugeMetricFamily(
                "taskqueue_dead_letter_jobs",
                "Jobs currently in the dead_letter status.",
                value=row[0],
            )


def build_registry() -> CollectorRegistry:
    """A fresh registry holding only the queue-state collector.

    Deliberately not the default registry: the exporter shouldn't re-export
    this process's own python/process metrics alongside queue state.
    """
    registry = CollectorRegistry()
    registry.register(QueueStateCollector())
    return registry


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    port = int(os.environ.get("METRICS_PORT", "9090"))
    start_http_server(port, registry=build_registry())
    logger.info("queue-state exporter listening on :%d/metrics", port)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    logger.info("queue-state exporter stopped")


if __name__ == "__main__":
    main()

"""Queue-state SQL scraped by the metrics exporter.

Each query runs once per Prometheus scrape (see
``taskqueue.metrics_exporter``), so keep them cheap: they lean on the
partial indexes from migrations/001 and touch no payload columns.
"""

# Jobs per status. Statuses with zero rows are absent from the result —
# the exporter zero-fills across JobStatus so gauges never disappear.
STATUS_COUNTS = """
SELECT status, count(*) AS depth
FROM jobs
GROUP BY status
"""

# Age of the oldest queued job per job_type, in seconds. 'queued' includes
# jobs waiting out a retry backoff, so backlog age can exceed queue-wait.
BACKLOG_AGE_BY_TYPE = """
SELECT job_type, extract(epoch FROM now() - min(created_at)) AS age_seconds
FROM jobs
WHERE status = 'queued'
GROUP BY job_type
"""

# Current dead-letter population. A gauge, not a counter — cleanup deletes
# terminal rows, so this can decrease.
DEAD_LETTER_COUNT = """
SELECT count(*) AS dead_letter_jobs
FROM jobs
WHERE status = 'dead_letter'
"""

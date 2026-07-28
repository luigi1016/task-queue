"""Operational/debugging queries — the canonical copies behind the
cheatsheet in docs/operations.md.

These are meant to be pasted into psql (or executed from ad-hoc tooling);
nothing in the library runs them on a schedule.
"""

# Currently running jobs, by lease owner.
RUNNING_BY_WORKER = """
SELECT worker_id, count(*), max(lease_expires_at) AS latest_lease
FROM jobs
WHERE status = 'running'
GROUP BY worker_id
ORDER BY worker_id
"""

# Throughput per worker over the last hour (completed + dead-lettered).
# worker_id is NULL on terminal rows because it's a lease field, so the
# attribution lives in processed_by_worker_id instead.
THROUGHPUT_BY_WORKER_LAST_HOUR = """
SELECT processed_by_worker_id, count(*)
FROM jobs
WHERE status IN ('succeeded', 'dead_letter')
  AND completed_at > now() - interval '1 hour'
GROUP BY processed_by_worker_id
ORDER BY count DESC
"""

# Backlog age (oldest queued job) with counts, per job_type.
BACKLOG_BY_TYPE = """
SELECT job_type, count(*), min(created_at) AS oldest
FROM jobs
WHERE status = 'queued'
GROUP BY job_type
ORDER BY oldest
"""

# Most recent dead-lettered jobs with the failure reason.
RECENT_DEAD_LETTERS = """
SELECT id, job_type, attempt_count, completed_at, error_message
FROM jobs
WHERE status = 'dead_letter'
ORDER BY completed_at DESC
LIMIT 20
"""

# Expired leases the reaper should pick up next tick.
EXPIRED_LEASES = """
SELECT id, worker_id, lease_expires_at
FROM jobs
WHERE status = 'running' AND lease_expires_at < now()
"""

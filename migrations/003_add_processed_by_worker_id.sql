-- migrations/003_add_processed_by_worker_id.sql
-- Adds a separate column that persists the last worker to handle a job.
--
-- worker_id is a lease field. ack(), nack(), and the reaper all clear it
-- when the job leaves 'running', so completed rows have worker_id IS NULL
-- and there's no way to answer "which worker handled this job?" from the
-- table. processed_by_worker_id copies the lease owner at resolution time
-- and stays put through 'succeeded', 'dead_letter', and 'queued' (retry).

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS processed_by_worker_id TEXT;

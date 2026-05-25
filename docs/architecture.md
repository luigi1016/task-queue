# Architecture

## Local development

Everything runs locally inside a minikube cluster on Docker Desktop. You interact via `kubectl` from your terminal.

![Local development architecture](./architecture.svg)

- **Postgres** — single pod, stores all job state
- **Producer** — enqueues fake jobs in a loop
- **Workers (x3)** — compete for jobs via `SELECT ... FOR UPDATE SKIP LOCKED`
- **Lease reaper** — CronJob that reclaims stuck jobs every minute
- **TTL cleanup** — CronJob that deletes old completed/dead-lettered jobs daily

## Production

In production, the same architecture runs on a real Kubernetes cluster in the cloud. Your Mac pushes images to a container registry and applies manifests via `kubectl`.

![Cloud architecture](./cloud-architecture.svg)

## The model

### The `jobs` table

A single Postgres table is the source of truth for everything: pending work, in-flight work, retries, dead letters, results. Key columns:

| Column | Purpose |
| --- | --- |
| `id` | UUID primary key |
| `idempotency_key` | `UNIQUE` — enqueue with the same key twice → second insert errors instead of duplicating work |
| `job_type` | Routes the row to a registered handler |
| `payload` | `JSONB` — handler input |
| `priority` | Higher wins on dequeue |
| `status` | `queued` / `running` / `succeeded` / `failed` / `dead_letter` (enforced by a `CHECK` constraint) |
| `attempt_count`, `max_attempts` | Drive retry vs. dead-letter on `nack` |
| `worker_id`, `lease_expires_at` | Set on claim; cleared on ack/nack/reap |
| `retry_after` | `nack` sets this so the row is invisible to dequeue until backoff elapses |
| `result_payload`, `error_message` | Whatever the handler returned or the exception text |

A partial index on `(priority DESC, created_at ASC) WHERE status = 'queued'` keeps the dequeue query fast even when the table accumulates terminal rows. A second partial index on `lease_expires_at WHERE status = 'running'` makes the reaper scan cheap.

### `SELECT ... FOR UPDATE SKIP LOCKED`

Workers dequeue with a single round-trip:

```sql
WITH claimed AS (
    SELECT id FROM jobs
    WHERE status = 'queued'
      AND (retry_after IS NULL OR retry_after <= now())
    ORDER BY priority DESC, created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs j
SET status = 'running', worker_id = ..., lease_expires_at = now() + ..., ...
FROM claimed
WHERE j.id = claimed.id
RETURNING j.*;
```

`SKIP LOCKED` lets N workers each grab a different row concurrently with no application-level coordination. The `UPDATE ... RETURNING` flips the status and stamps the lease in the same transaction as the lock, so a claim either commits atomically or doesn't happen.

### NOTIFY / LISTEN

Polling the queue every few seconds works but wastes round-trips when the queue is empty and adds latency when jobs do arrive. Postgres' `LISTEN`/`NOTIFY` solves both:

- `enqueue` fires `pg_notify('jobs_new', <job_id>)` after `INSERT`
- `ack` fires `pg_notify('jobs_done', <job_id>)` so callers can wait for completion
- Workers `LISTEN jobs_new` and block on `conn.notifies(timeout=POLL_INTERVAL_S)`

The poll interval is the safety net — if a NOTIFY is missed (brief network blip, a `retry_after` quietly elapsing without re-firing), the worker re-checks at most `POLL_INTERVAL_S` seconds later.

### Leases and the reaper

When a worker claims a job it stamps `lease_expires_at = now() + LEASE_SECONDS`. If the worker crashes mid-handler, that row is stuck in `running` with no live owner.

The reaper is a tiny CronJob that runs every minute:

```sql
UPDATE jobs
SET status = 'queued', worker_id = NULL, lease_expires_at = NULL
WHERE status = 'running' AND lease_expires_at < now();
```

Idempotent and safe to overlap with live workers — a live worker either still holds an unexpired lease (not matched) or has already ack/nacked (moved out of `running`).

### Retries and dead-lettering

`nack` checks `attempt_count` against `max_attempts`:

- Under the limit → status back to `queued`, `retry_after = now() + backoff`
- At the limit → status to `dead_letter`, `error_message` recorded

Backoff is exponential with jitter: `10s * 2^(attempt-1)`, capped at 1 hour, with ±20% randomization to avoid synchronized retry storms.

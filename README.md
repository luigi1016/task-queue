# taskqueue

Async task queue library backed by Postgres. Provides durable job queuing with at-least-once delivery, automatic retries with exponential backoff, dead-letter handling, and lease-based failure detection. Designed to run in Kubernetes with zero infrastructure beyond the database.

## Architecture

Everything runs locally inside a minikube cluster on Docker Desktop. You interact via `kubectl` from your terminal.

![Local development architecture](docs/architecture.svg)

- **Postgres** — single pod, stores all job state
- **Producer** — enqueues fake jobs in a loop
- **Workers (x3)** — compete for jobs via `SELECT ... FOR UPDATE SKIP LOCKED`
- **Lease reaper** — CronJob that reclaims stuck jobs every 30s
- **TTL cleanup** — CronJob that deletes old completed/dead-lettered jobs daily

In production, the same architecture runs on a real Kubernetes cluster in the cloud. Your Mac pushes images to a container registry and applies manifests via `kubectl`.

![Cloud architecture](docs/cloud-architecture.svg)

## Prerequisites

- Python 3.11+
- Postgres 16+
- Docker Desktop
- [minikube](https://minikube.sigs.k8s.io/docs/start/) (`brew install minikube`)
- kubectl (`brew install kubectl`)

## Setup

```bash
# Clone and enter the project
git clone <repo-url>
cd task_queue

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install the package and dev dependencies
pip install -e ".[dev]"
```

## Local Kubernetes (minikube)

### Start the cluster

```bash
minikube start --driver=docker --memory=4096 --cpus=2
```

### Deploy Postgres

```bash
kubectl apply -f k8s/
```

This creates:
- **Secret** — database credentials and `DATABASE_URL`
- **PersistentVolumeClaim** — 1GB disk so Postgres data survives pod restarts
- **Deployment** — runs `postgres:16` with the credentials injected
- **Service** — stable DNS name (`postgres:5432`) so other pods can connect

### Verify Postgres is running

```bash
# Watch until you see 1/1 Running, then Ctrl+C
kubectl get pods -l app=postgres -w

# Test the connection from inside the cluster
kubectl run pg-test --rm -it --restart=Never \
  --image=postgres:16 \
  --env="PGPASSWORD=taskqueue-dev-password" \
  -- psql -h postgres -U taskqueue -d taskqueue --pset pager=off -c "SELECT 1;"
```

You should see a result of `1`, then the test pod cleans itself up.

### Build and deploy the task queue

```bash
# Point docker CLI at minikube's daemon
eval $(minikube docker-env)

# Build the image
docker build -t taskqueue:latest .

# Run the database migration
kubectl delete job taskqueue-migrate --ignore-not-found
kubectl apply -f k8s/migrate-job.yaml
kubectl wait --for=condition=complete job/taskqueue-migrate --timeout=60s
kubectl logs job/taskqueue-migrate
```

### Deploy the worker and producer

The same container image runs as both, dispatched by the `ROLE` env var to
two different entry points in `examples/demo_service/` — the same code you
could run locally with `python -m`. In a real consumer's deployment, you'd
replace `demo_service` with your own application package.

```bash
kubectl apply -f k8s/worker-deployment.yaml
kubectl apply -f k8s/producer-deployment.yaml
```

Watch jobs flow:

```bash
kubectl logs -l app=taskqueue-producer -f      # see enqueues
kubectl logs -l app=taskqueue-worker -f        # see handlers fire
kubectl exec deploy/postgres -- psql -U taskqueue -d taskqueue \
  -c "SELECT status, count(*) FROM jobs GROUP BY status;"
```

Scale workers horizontally (each pod uses `SELECT ... FOR UPDATE SKIP LOCKED`
on dequeue, so no two pods ever claim the same job — no coordination needed):

```bash
kubectl scale deployment/taskqueue-worker --replicas=3
```

`terminationGracePeriodSeconds: 90` on the worker Deployment must exceed
`LEASE_SECONDS` plus the longest expected handler runtime — otherwise k8s
will `SIGKILL` mid-handler and the reaper has to clean up the orphaned
lease.

### Deploy the lease reaper

The reaper runs as a Kubernetes CronJob — once a minute it scans for jobs
whose worker lease has expired (i.e., the worker crashed mid-job) and resets
them back to `queued` so another worker can pick them up.

```bash
kubectl apply -f k8s/reaper-cronjob.yaml
```

Verify it works without waiting for the next scheduled tick:

```bash
# Trigger an immediate run from the CronJob template
kubectl create job --from=cronjob/taskqueue-reaper reaper-test
kubectl wait --for=condition=complete job/reaper-test --timeout=60s
kubectl logs job/reaper-test
# expected: "reaper: reclaimed N expired leases"

# Clean up the one-off job
kubectl delete job reaper-test
```

Kubernetes CronJob's minimum granularity is one minute, so a stuck job waits
at most ~1 minute past lease expiry before reclamation. `concurrencyPolicy:
Forbid` prevents overlapping runs if a tick is slow.

### Stopping and restarting

- `minikube stop` — pauses everything, data is preserved
- `minikube start` — brings it back, Kubernetes restarts your pods automatically (no need to re-apply manifests)
- `minikube delete` — destroys the cluster and all data

## Run the demo service locally

You can exercise the producer/worker loop without minikube by pointing them
at any reachable Postgres. The easiest source is the minikube Postgres via
`kubectl port-forward`.

```bash
# Terminal 1 — port-forward Postgres
kubectl port-forward svc/postgres 5432:5432

# Terminal 2 — worker
export DATABASE_URL=postgres://taskqueue:taskqueue-dev-password@localhost:5432/taskqueue
export PYTHONPATH=$PWD/src:$PWD/examples
python -m demo_service.worker_main

# Terminal 3 — producer
export DATABASE_URL=postgres://taskqueue:taskqueue-dev-password@localhost:5432/taskqueue
export PYTHONPATH=$PWD/src:$PWD/examples
python -m demo_service.producer_main

# Terminal 4 — observe
psql "$DATABASE_URL" -c "SELECT status, count(*) FROM jobs GROUP BY status;"
```

You should see `succeeded` and `dead_letter` counts grow steadily while
`queued`/`running` stay small and transient. `Ctrl+C` on either service
triggers a clean shutdown (in-flight handlers finish; no new dequeues).

### Configuration

Worker env vars:

| Variable | Default | Purpose |
| --- | --- | --- |
| `WORKER_ID` | `$HOSTNAME` | Recorded on each claimed row for lease traceability |
| `WORKER_CONCURRENCY` | `1` | Handler threads per pod (1 = serial, no pool) |
| `POLL_INTERVAL_S` | `5.0` | Max time `listen()` blocks before re-checking the queue |
| `LEASE_SECONDS` | `60` | Lease duration on each claimed job |

Producer env vars:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PRODUCER_INTERVAL_S` | `1.0` | Sleep between enqueues |
| `PRODUCER_MAX_PRIORITY` | `9` | Top of the random priority range |
| `PRODUCER_MAX_ATTEMPTS` | `3` | `max_attempts` set on each enqueued job |

## Using `taskqueue` as a library

The producer and worker in `examples/demo_service/` are reference code for
what a real consumer would write. The integration surface is small.

### Enqueuing (same in both styles)

```python
import taskqueue

with taskqueue.db.get_connection() as conn:
    taskqueue.enqueue(
        conn,
        idempotency_key="unique-key",
        job_type="send_email",
        payload={"to": "a@b.com", "subject": "..."},
        priority=5,
    )
```

### Worker — decorator style (recommended)

```python
# myapp/handlers.py
import taskqueue

@taskqueue.task("send_email")
def send_email(payload):
    smtp.send(...)
    return {"sent_at": time.time()}   # becomes the row's result_payload

# myapp/worker_main.py
import taskqueue
import myapp.handlers      # noqa: F401 — side effect: runs @task decorators

worker = taskqueue.Worker(worker_id=socket.gethostname(), concurrency=4)
worker.run()
```

The `@taskqueue.task("send_email")` line is two function calls at import
time: it builds a closure capturing `"send_email"`, then runs it on the
decorated function and stores the pair in a module-level dict. The
decorated function is returned unchanged and is still directly callable
in your own code.

**The handler module import is load-bearing.** Without
`import myapp.handlers`, the decorators never run and the registry stays
empty — every job would nack with "no handler registered." Lint configs
should treat `taskqueue.task`-decorated modules as not-actually-unused.

### Worker — dependency-injection style (still supported)

```python
import taskqueue

def send_email(payload):
    smtp.send(...)
    return {"sent_at": time.time()}

worker = taskqueue.Worker(
    handlers={"send_email": send_email},   # explicit dict
    worker_id=socket.gethostname(),
    concurrency=4,
)
worker.run()
```

Prefer this when you need multiple workers with different handler sets in
one process, or in tests where global registry state would leak between
cases. `Worker(handlers={...})` overrides the default-from-registry
behavior — handlers registered via `@task` are ignored.

## Run tests

There are two test entry points, separated by speed and what they cover:

| Script | Runtime | Covers |
| --- | --- | --- |
| `./bin/test` | ~1s | Python + SQL logic — enqueue, dequeue, ack, nack, reaper query, backoff helper. The fast loop you run constantly. |
| `./bin/smoke-cronjob` | ~30s | The reaper CronJob end-to-end — image build, K8s deploy, scheduling template, container startup, log retrieval. Run before pushing manifest changes. |

### `./bin/test` — fast pytest suite

Tests hit a real Postgres — they cover the `UNIQUE` constraint and
`SELECT ... FOR UPDATE SKIP LOCKED` semantics that mocks can't simulate.

One-time setup (creates a separate test DB on the minikube Postgres):

```bash
kubectl exec deploy/postgres -- psql -U taskqueue -d postgres -c "CREATE DATABASE taskqueue_test;"
```

Then run the suite via the wrapper script:

```bash
./bin/test           # all tests
./bin/test -v        # verbose
./bin/test tests/test_dequeue.py    # one file
```

The script handles the `kubectl port-forward`, `TEST_DATABASE_URL`
export, and auto-discovers `.venv/bin/pytest` so you don't need to
activate the venv first. Cleans up on exit.

If you'd rather run pytest directly: forward the service in a separate
terminal (`kubectl port-forward svc/postgres 5432:5432`), export
`TEST_DATABASE_URL=postgres://taskqueue:taskqueue-dev-password@localhost:5432/taskqueue_test`,
then `pytest`. Tests are skipped if `TEST_DATABASE_URL` is unset.

### `./bin/smoke-cronjob` — reaper CronJob end-to-end

Verifies the full deployment path that `./bin/test` can't reach: builds
the image into minikube, seeds a fake stuck job in the live Postgres,
triggers the CronJob template, asserts the reaper logged "reclaimed 1"
and that the row was flipped back to `queued`. Cleans up on success or
failure.

```bash
./bin/smoke-cronjob
```

Requires the CronJob to already be applied (`kubectl apply -f
k8s/reaper-cronjob.yaml`) and the Postgres pod to be ready. Takes
~30 seconds — most of it pulling/starting the reaper container.

## Project structure

```
src/taskqueue/       # Library source code
  __init__.py
  models.py          # Job dataclass
  db.py              # Database connection
  queue.py           # enqueue / dequeue / ack / nack
  notify.py          # listen() — block until NOTIFY or timeout
  registry.py        # @task decorator + default handler registry
  worker.py          # Worker class (serial + thread-pool modes)
  reaper.py          # Reclaim expired leases
  migrate.py         # Runs SQL migrations
examples/demo_service/   # Example consumer of the library (not part of the wheel)
  handlers.py        # sleep_handler, flaky_handler
  worker_main.py     # python -m demo_service.worker_main
  producer_main.py   # python -m demo_service.producer_main
tests/               # Test suite
migrations/          # SQL migration files
k8s/                 # Kubernetes manifests
  postgres-*.yaml
  migrate-job.yaml
  reaper-cronjob.yaml
  worker-deployment.yaml
  producer-deployment.yaml
Dockerfile           # Single image; ROLE env var selects entry point
entrypoint.sh        # Dispatches ROLE to demo_service or taskqueue entry points
pyproject.toml       # Package metadata and dependencies
```

## Docker

```bash
docker build -t taskqueue .

# Run as different roles
docker run -e ROLE=worker -e DATABASE_URL=postgres://... taskqueue
docker run -e ROLE=producer -e DATABASE_URL=postgres://... taskqueue
```

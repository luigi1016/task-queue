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
  migrate.py         # Runs SQL migrations
tests/               # Test suite
migrations/          # SQL migration files
  001_create_jobs.sql
k8s/                 # Kubernetes manifests
  postgres-secret.yaml
  postgres-pvc.yaml
  postgres-deployment.yaml
  postgres-service.yaml
  migrate-job.yaml   # One-shot Job to run migrations
Dockerfile           # Single image, multiple roles via ROLE env var
entrypoint.sh        # Dispatches to producer/worker/reaper/cleanup/migrate
pyproject.toml       # Package metadata and dependencies
```

## Docker

```bash
docker build -t taskqueue .

# Run as different roles
docker run -e ROLE=worker -e DATABASE_URL=postgres://... taskqueue
docker run -e ROLE=producer -e DATABASE_URL=postgres://... taskqueue
```

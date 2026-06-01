# Setup

First-time setup for the local minikube environment. After this, day-to-day commands live in [operations.md](./operations.md).

## Prerequisites

- Python 3.11+
- Postgres 16+
- Docker Desktop
- [minikube](https://minikube.sigs.k8s.io/docs/start/) (`brew install minikube`)
- kubectl (`brew install kubectl`)

## Install the package

```bash
git clone <repo-url>
cd task_queue

python3 -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
```

## Start minikube

```bash
minikube start --driver=docker --memory=4096 --cpus=2
```

## Deploy Postgres

```bash
kubectl apply -f k8s/
```

This creates:

- **Secret** — database credentials and `DATABASE_URL`
- **PersistentVolumeClaim** — 1GB disk so Postgres data survives pod restarts
- **Deployment** — runs `postgres:16` with the credentials injected
- **Service** — stable DNS name (`postgres:5432`) so other pods can connect

Verify it came up:

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

## Build the image and run migrations

```bash
# Point docker CLI at minikube's daemon so the image lands inside the cluster
eval $(minikube docker-env)

docker build -t taskqueue:latest .

kubectl delete job taskqueue-migrate --ignore-not-found
kubectl apply -f k8s/migrate-job.yaml
kubectl wait --for=condition=complete job/taskqueue-migrate --timeout=60s
kubectl logs job/taskqueue-migrate
```

## Deploy the worker, producer, and reaper

The same container image runs as worker, producer, or reaper — `ROLE` env var picks the entry point in `entrypoint.sh`. In a real consumer's deployment, you'd replace the `demo_service` entry points with your own application package.

```bash
kubectl apply -f k8s/worker-deployment.yaml
kubectl apply -f k8s/producer-deployment.yaml
kubectl apply -f k8s/reaper-cronjob.yaml
```

Watch jobs flow:

```bash
kubectl logs -l app=taskqueue-producer -f      # see enqueues
kubectl logs -l app=taskqueue-worker -f        # see handlers fire
kubectl exec deploy/postgres -- psql -U taskqueue -d taskqueue \
  -c "SELECT status, count(*) FROM jobs GROUP BY status;"
```

You should see `succeeded` and `dead_letter` counts grow steadily while `queued`/`running` stay small and transient.

## What's next

- Day-to-day cluster operations → [operations.md](./operations.md)
- How the queue works under the hood → [architecture.md](./architecture.md)
- Wiring the library into your own service → [using-the-library.md](./using-the-library.md)
- Running the test suite → [testing.md](./testing.md)

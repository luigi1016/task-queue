# taskqueue

Async task queue library backed by Postgres. Durable job queuing with at-least-once delivery, automatic retries with exponential backoff, dead-letter handling, and lease-based failure detection. Designed to run in Kubernetes with zero infrastructure beyond the database.

![Local development architecture](docs/architecture.svg)

## Documentation

| Guide | When to read |
| --- | --- |
| [Architecture](docs/architecture.md) | How the queue works — `jobs` table, `SKIP LOCKED`, NOTIFY/LISTEN, leases, retries |
| [Setup](docs/setup.md) | First-time install — prereqs, minikube, Postgres, migrations, first deploy |
| [Operations](docs/operations.md) | kubectl cheatsheet — deploy, scale, inspect, tear down, common SQL, troubleshooting |
| [Using the library](docs/using-the-library.md) | Decorator + DI patterns, env var reference, running outside k8s |
| [Testing](docs/testing.md) | `./bin/test`, `./bin/smoke-cronjob` |

## Quick start

```bash
git clone <repo-url>
cd task_queue
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Then follow [Setup](docs/setup.md) to bring up the cluster, or jump to [Using the library](docs/using-the-library.md) if you're wiring `taskqueue` into your own service.

## Project layout

```
src/taskqueue/         # Library source (queue, worker, reaper, registry, ...)
examples/demo_service/ # Reference consumer — producer + worker entry points
tests/                 # pytest suite (hits a real Postgres)
migrations/            # SQL migrations
k8s/                   # Kubernetes manifests
bin/                   # ./bin/test, ./bin/smoke-cronjob
docs/                  # The docs linked above
Dockerfile             # Single image; ROLE env var selects entry point
```

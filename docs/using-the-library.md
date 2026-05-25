# Using `taskqueue` as a library

The producer and worker in `examples/demo_service/` are reference code for what a real consumer would write. The integration surface is small.

## Enqueuing (same in both styles)

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

## Worker — decorator style (recommended)

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

The `@taskqueue.task("send_email")` line is two function calls at import time: it builds a closure capturing `"send_email"`, then runs it on the decorated function and stores the pair in a module-level dict. The decorated function is returned unchanged and is still directly callable in your own code.

**The handler module import is load-bearing.** Without `import myapp.handlers`, the decorators never run and the registry stays empty — every job would nack with "no handler registered." Lint configs should treat `taskqueue.task`-decorated modules as not-actually-unused.

## Worker — dependency-injection style (still supported)

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

Prefer this when you need multiple workers with different handler sets in one process, or in tests where global registry state would leak between cases. `Worker(handlers={...})` overrides the default-from-registry behavior — handlers registered via `@task` are ignored.

## Running locally without Kubernetes

You can exercise the producer/worker loop without minikube by pointing them at any reachable Postgres. The easiest source is the minikube Postgres via `kubectl port-forward`.

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

`Ctrl+C` on either service triggers a clean shutdown: in-flight handlers finish; no new dequeues.

## Configuration reference

### Worker env vars

| Variable | Default | Purpose |
| --- | --- | --- |
| `WORKER_ID` | `$HOSTNAME` | Recorded on each claimed row for lease traceability |
| `WORKER_CONCURRENCY` | `1` | Handler threads per pod (1 = serial, no pool) |
| `POLL_INTERVAL_S` | `5.0` | Max time `listen()` blocks before re-checking the queue |
| `LEASE_SECONDS` | `60` | Lease duration on each claimed job |

### Producer env vars

| Variable | Default | Purpose |
| --- | --- | --- |
| `PRODUCER_INTERVAL_S` | `1.0` | Sleep between enqueues |
| `PRODUCER_MAX_PRIORITY` | `9` | Top of the random priority range |
| `PRODUCER_MAX_ATTEMPTS` | `3` | `max_attempts` set on each enqueued job |

### Docker image roles

The image dispatches on `ROLE`:

```bash
docker build -t taskqueue .

docker run -e ROLE=worker   -e DATABASE_URL=postgres://... taskqueue
docker run -e ROLE=producer -e DATABASE_URL=postgres://... taskqueue
docker run -e ROLE=reaper   -e DATABASE_URL=postgres://... taskqueue
```

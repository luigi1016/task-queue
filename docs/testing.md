# Testing

Two test entry points, separated by speed and what they cover:

| Script | Runtime | Covers |
| --- | --- | --- |
| `./bin/test` | ~1s | Python + SQL logic — enqueue, dequeue, ack, nack, reaper query, backoff helper. The fast loop you run constantly. |
| `./bin/smoke-cronjob` | ~30s | The reaper CronJob end-to-end — image build, K8s deploy, scheduling template, container startup, log retrieval. Run before pushing manifest changes. |

## `./bin/test` — fast pytest suite

Tests hit a real Postgres — they cover the `UNIQUE` constraint and `SELECT ... FOR UPDATE SKIP LOCKED` semantics that mocks can't simulate.

One-time setup (creates a separate test DB on the minikube Postgres):

```bash
kubectl exec deploy/postgres -- psql -U taskqueue -d postgres -c "CREATE DATABASE taskqueue_test;"
```

Then run the suite via the wrapper script:

```bash
./bin/test                          # all tests
./bin/test -v                       # verbose
./bin/test tests/test_dequeue.py    # one file
```

The script handles the `kubectl port-forward`, `TEST_DATABASE_URL` export, and auto-discovers `.venv/bin/pytest` so you don't need to activate the venv first. Cleans up on exit.

If you'd rather run pytest directly: forward the service in a separate terminal (`kubectl port-forward svc/postgres 5432:5432`), export `TEST_DATABASE_URL=postgres://taskqueue:taskqueue-dev-password@localhost:5432/taskqueue_test`, then `pytest`. Tests are skipped if `TEST_DATABASE_URL` is unset.

## `./bin/smoke-cronjob` — reaper CronJob end-to-end

Verifies the full deployment path that `./bin/test` can't reach: builds the image into minikube, seeds a fake stuck job in the live Postgres, triggers the CronJob template, asserts the reaper logged "reclaimed 1" and that the row was flipped back to `queued`. Cleans up on success or failure.

```bash
./bin/smoke-cronjob
```

Requires the CronJob to already be applied (`kubectl apply -f k8s/reaper-cronjob.yaml`) and the Postgres pod to be ready. Takes ~30 seconds — most of it pulling/starting the reaper container.

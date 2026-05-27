# Operations cheatsheet

Day-to-day commands for running the cluster. Grouped by what you're trying to do, not by which subsystem they touch.

For first-time bootstrap (minikube install, Postgres deploy, first migration), see [setup.md](./setup.md).

## Deploy or update code

After changing source or manifests, get the new image into the cluster and roll pods.

```bash
# Always re-point docker at minikube's daemon in a new shell — the env var doesn't persist
eval $(minikube docker-env)

docker build -t taskqueue:latest .

# Manifest changes
kubectl apply -f k8s/worker-deployment.yaml
kubectl apply -f k8s/producer-deployment.yaml
kubectl apply -f k8s/reaper-cronjob.yaml

# Code-only changes (image rebuilt, manifest unchanged) — force a fresh pull
kubectl rollout restart deployment/taskqueue-worker
kubectl rollout restart deployment/taskqueue-producer
```

Migrations are a one-shot Job, not part of the deployment:

```bash
kubectl delete job taskqueue-migrate --ignore-not-found
kubectl apply -f k8s/migrate-job.yaml
kubectl wait --for=condition=complete job/taskqueue-migrate --timeout=60s
```

## Scale

Both workers and producers are stateless — scale up/down freely.

```bash
kubectl scale deployment/taskqueue-worker   --replicas=3
kubectl scale deployment/taskqueue-producer --replicas=2
```

Each worker pod uses `SELECT ... FOR UPDATE SKIP LOCKED` on dequeue, so no two pods ever claim the same job — no coordination needed.

## Inspect

```bash
# What's running?
kubectl get pods
kubectl get pods -l app=taskqueue-worker

# Live logs across all replicas of a deployment
kubectl logs -l app=taskqueue-worker   -f --prefix=true
kubectl logs -l app=taskqueue-producer -f --prefix=true

# Why is this pod unhappy?
kubectl describe pod <pod-name>

# Open a psql session against the live database
kubectl exec -it deploy/postgres -- psql -U taskqueue -d taskqueue
```

## Stop and restart without losing data

`scale --replicas=0` pauses a workload while preserving the Deployment, Service, and any in-flight Postgres data:

```bash
kubectl scale deployment/taskqueue-worker   --replicas=0
kubectl scale deployment/taskqueue-producer --replicas=0
# Bring them back
kubectl scale deployment/taskqueue-worker   --replicas=3
kubectl scale deployment/taskqueue-producer --replicas=1
```

Pausing the whole cluster:

```bash
minikube stop    # everything halts; PVC and built images are preserved
minikube start   # pods come back automatically — no need to re-apply manifests
```

## Tear down

Be deliberate about which destroys which:

| Command | What it deletes | What survives |
| --- | --- | --- |
| `kubectl scale deployment/X --replicas=0` | Running pods of X | Deployment, Service, PVC, Postgres data |
| `kubectl delete deployment taskqueue-worker` | Worker Deployment + pods | Postgres, PVC, data |
| `kubectl delete -f k8s/` | **All** manifests in the dir, including the PVC — **wipes data** | Nothing in the namespace |
| `minikube delete` | The entire cluster | Nothing |

`kubectl delete -f k8s/` is the footgun — it includes `postgres-pvc.yaml`, so the next `kubectl apply -f k8s/` gives you a fresh empty database. If you want to remove just the workloads and keep the data, delete by file or by name:

```bash
kubectl delete -f k8s/worker-deployment.yaml \
                -f k8s/producer-deployment.yaml \
                -f k8s/reaper-cronjob.yaml
```

## Common queries on the `jobs` table

Open a psql session first:

```bash
kubectl exec -it deploy/postgres -- psql -U taskqueue -d taskqueue
```

```sql
-- Status counts (what's the queue doing right now?)
SELECT status, count(*) FROM jobs GROUP BY status ORDER BY status;

-- Currently running, by worker
SELECT worker_id, count(*), max(lease_expires_at) AS latest_lease
FROM jobs
WHERE status = 'running'
GROUP BY worker_id
ORDER BY worker_id;

-- Throughput per worker over the last hour (completed + dead-lettered).
-- worker_id is NULL on terminal rows because it's a lease field, so the
-- attribution lives in processed_by_worker_id instead.
SELECT processed_by_worker_id, count(*)
FROM jobs
WHERE status IN ('succeeded', 'dead_letter')
  AND completed_at > now() - interval '1 hour'
GROUP BY processed_by_worker_id
ORDER BY count DESC;

-- Backlog age (oldest queued job)
SELECT job_type, count(*), min(created_at) AS oldest
FROM jobs
WHERE status = 'queued'
GROUP BY job_type
ORDER BY oldest;

-- Dead-lettered jobs with the failure reason
SELECT id, job_type, attempt_count, completed_at, error_message
FROM jobs
WHERE status = 'dead_letter'
ORDER BY completed_at DESC
LIMIT 20;

-- Expired leases the reaper should pick up next tick
SELECT id, worker_id, lease_expires_at
FROM jobs
WHERE status = 'running' AND lease_expires_at < now();
```

## Running the reaper on demand

The CronJob fires once per minute (`schedule: "* * * * *"` — Kubernetes' minimum granularity). To trigger an immediate run without waiting for the next tick:

```bash
kubectl create job --from=cronjob/taskqueue-reaper reaper-now
kubectl wait --for=condition=complete job/reaper-now --timeout=60s
kubectl logs job/reaper-now            # expected: "reaper: reclaimed N expired leases"
kubectl delete job reaper-now
```

`concurrencyPolicy: Forbid` on the CronJob prevents overlapping runs if a tick is slow.

## Worker graceful shutdown

`terminationGracePeriodSeconds: 90` on the worker Deployment must exceed `LEASE_SECONDS` plus the longest expected handler runtime — otherwise k8s will `SIGKILL` mid-handler and the reaper has to clean up the orphaned lease. Bump both together if you add a slower handler.

## Troubleshooting

### `ErrImageNeverPull`

Worker / producer pods show `ErrImageNeverPull` in `kubectl describe`. The deployments use `imagePullPolicy: Never` (they expect the image to already be in minikube's local Docker daemon), so this means the build didn't land there:

```bash
eval $(minikube docker-env)        # critical — in a fresh shell, easy to forget
docker build -t taskqueue:latest .
kubectl rollout restart deployment/taskqueue-worker
```

### `kubectl port-forward` drops the connection

Long-running `port-forward` sessions die on network blips or laptop sleep. The fix is to restart the forward; nothing in the cluster is broken:

```bash
kubectl port-forward svc/postgres 5432:5432
```

If this is happening constantly during testing, wrap it: `while true; do kubectl port-forward svc/postgres 5432:5432; sleep 1; done`.

### Pods stuck `Pending`

Usually a resource issue on the minikube node. Check what's blocking:

```bash
kubectl describe pod <pod-name> | tail -20    # look at Events
kubectl top nodes                              # cpu/memory pressure?
```

Common causes: minikube started with too-low `--memory` / `--cpus`; PVC waiting on a non-existent StorageClass; image pull (see above).

### Postgres pod won't start after `minikube start`

PVC is fine but the pod won't bind. Usually transient — wait a beat and check:

```bash
kubectl get pods -l app=postgres
kubectl describe pod -l app=postgres | tail -30
```

If it's actually wedged, deleting the pod (not the deployment) lets the Deployment recreate it against the same PVC: `kubectl delete pod -l app=postgres`.

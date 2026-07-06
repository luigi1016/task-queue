# Telemetry & metrics

Prometheus + Grafana observability for the queue. Two metric sources feed one dashboard:

1. **App metrics (RED)** — counters/histograms emitted from inside each producer and worker process by `src/taskqueue/metrics.py`, exposed on a per-pod `/metrics` endpoint.
2. **Queue-state metrics** — gauges computed from Postgres on every scrape by the exporter (`src/taskqueue/metrics_exporter.py`, `ROLE=exporter`), using the SQL in `src/taskqueue/queries/metrics.py`.

The split matters: app metrics tell you what each process *did* (rates, latencies), the exporter tells you what the database *is* (depth, backlog, dead letters) — regardless of which process touched a row. When the two disagree, trust the exporter; it reads ground truth.

## Accessing the dashboards

```bash
# Grafana — the "Task Queue" dashboard, no login required
kubectl port-forward svc/grafana 3000:3000
open http://localhost:3000/d/taskqueue

# Prometheus — raw queries and target health
kubectl port-forward svc/prometheus 9092:9090
open http://localhost:9092/targets
```

Every worker, producer, and exporter pod should show as `UP` on the targets page. A missing target usually means the pod template lost its `prometheus.io/scrape` annotation; a `DOWN` exporter usually means it can't reach Postgres.

To eyeball a raw endpoint:

```bash
kubectl port-forward deploy/taskqueue-worker 9090:9090
curl -s localhost:9090/metrics | grep taskqueue_
```

## SLIs

These five are the queue's service-level indicators. Phase 2 (benchmarking, #18) uses them as its success metrics.

| SLI | Definition | PromQL |
| --- | --- | --- |
| **Queue wait** | Time from enqueue to *first* dequeue, p95. The purest signal of "are there enough workers?" — excludes retry backoff by construction. | `histogram_quantile(0.95, sum by (le) (rate(taskqueue_queue_wait_seconds_bucket[5m])))` |
| **Handler duration** | Wall-clock handler runtime per job type, p95. Regressions here are code, not capacity. | `histogram_quantile(0.95, sum by (le, job_type) (rate(taskqueue_handler_duration_seconds_bucket[5m])))` |
| **End-to-end latency** | Enqueue to successful completion, p95, including retries and backoff. What a caller actually experiences. | `histogram_quantile(0.95, sum by (le) (rate(taskqueue_job_e2e_seconds_bucket[5m])))` |
| **Throughput** | Successfully completed jobs per second. | `sum(rate(taskqueue_jobs_acked_total[5m]))` |
| **Error rate** | Fraction of dequeues that end in a nack; the `dead_lettered` outcome is the permanent-failure subset. | `sum(rate(taskqueue_jobs_nacked_total[5m])) / sum(rate(taskqueue_jobs_dequeued_total[5m]))` |

## Metric reference

### App metrics (per producer/worker process)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `taskqueue_jobs_enqueued_total` | counter | `job_type` | Committed enqueues. Lives in the **producer** process. |
| `taskqueue_jobs_dequeued_total` | counter | `job_type` | Claims, including retry attempts. |
| `taskqueue_jobs_acked_total` | counter | `job_type` | Jobs resolved as succeeded. |
| `taskqueue_jobs_nacked_total` | counter | `job_type`, `outcome` | Failures, split into `retrying` vs `dead_lettered`. |
| `taskqueue_queue_wait_seconds` | histogram | `job_type` | Enqueue → first dequeue, DB clock (`started_at - created_at`). First attempt only. |
| `taskqueue_handler_duration_seconds` | histogram | `job_type` | Handler wall-clock time, observed on success and failure. |
| `taskqueue_job_e2e_seconds` | histogram | `job_type` | Enqueue → successful completion, DB clock, retries included. |
| `taskqueue_jobs_in_flight` | gauge | — | Jobs this worker process is executing right now. Sum by `pod` for fleet view. |

### Queue-state metrics (exporter)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `taskqueue_queue_depth` | gauge | `status` | Row count per status. Zero-filled — every status is always present. |
| `taskqueue_backlog_age_seconds` | gauge | `job_type` | Age of the oldest queued job. Includes jobs waiting out retry backoff. |
| `taskqueue_dead_letter_jobs` | gauge | — | Current dead-letter population. A gauge, not a counter: cleanup deletes rows, so it can go down. |

## Consumers: exposing metrics from your own service

The library records metrics unconditionally (they're inert in-memory objects), but only processes that start an HTTP endpoint get scraped. In your `main()`:

```python
from taskqueue import metrics

def main():
    metrics.maybe_start_metrics_server()   # reads METRICS_PORT, default 9090; 0 disables
    ...
```

Then annotate the pod template so Prometheus finds it:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"
```

## Caveats worth knowing

- **Registries are per-process.** Enqueue counts live in whichever process called `enqueue()` — a producer that doesn't expose `/metrics` silently drops out of `jobs_enqueued_total`. The exporter's gauges are the cross-check, since they read the database directly.
- **Counters reset when pods restart.** Always wrap counters in `rate()`/`increase()`; never read them raw.
- **`job_type` is a label.** Keep it a small fixed set; never derive it from user input, or you'll blow up series cardinality.
- **The reaper and cleanup CronJobs are not instrumented.** They're short-lived processes, which don't fit Prometheus' pull model without a Pushgateway. Their effects are visible through the exporter's gauges (`queue_depth{status="queued"}` jumping when leases are reclaimed, `dead_letter_jobs` for poison pills). Revisit if a Pushgateway ever joins the stack.
- **Prometheus storage is an `emptyDir` with 2-day retention.** History is lost when the Prometheus pod restarts. Fine for dev; a real cluster (Phase 4) needs a PVC.
- **Grafana is anonymous-admin.** Dev-only convenience — replace with real auth before any shared environment.
- **Histogram buckets are first guesses**, sized for the demo handlers (0.05–0.5 s) and the 60 s default lease. Tune the constants in `src/taskqueue/metrics.py` when real workloads arrive.

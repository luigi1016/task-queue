"""Peak-throughput benchmark: how many jobs/second can the queue sustain?

Enqueues a batch of no-op jobs, lets the workers drain it, and measures the
drain rate from database timestamps. The no-op handler returns immediately,
so the result isolates queue overhead (the dequeue and ack transactions)
from handler work — it is the ceiling for any real workload.

The benchmark sweeps worker configurations (replicas x concurrency), stepping
up until a step improves throughput by less than 10%, then reports the best
step as the peak. Each step:

1. scale the worker deployment to 0 replicas
2. reset the jobs table
3. preload --jobs no-op jobs (multi-threaded, through the connection pool)
4. scale workers to W replicas with WORKER_CONCURRENCY=C and pool size C+2
5. wait until the queue drains
6. compute throughput and latency percentiles from DB timestamps

Measurements (Postgres clocks only, immune to host clock skew):

- throughput   = jobs / (max(completed_at) - min(started_at))
- service time = completed_at - started_at, p50/p95 (per-job dequeue-to-ack)

Safety rails: a step is skipped if worker pods would need more than
--max-db-connections (default 80, under Postgres's default max_connections
of 100). Each worker pod uses at most C+3 connections: a pool of C+2 plus
one dedicated LISTEN connection.

Intended for a dev/demo cluster: each step TRUNCATEs the jobs table, and the
demo producer is scaled to 0 for the duration of the run (restored after).

Requires kubectl pointed at the cluster and DATABASE_URL reachable from this
process. ./bin/benchmark wraps both (port-forward + env), so normally:

    ./bin/benchmark
    ./bin/benchmark --jobs 50000 --steps 2x8,4x8,4x16
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import psycopg

import taskqueue
from taskqueue import db

from demo_service.handlers import NOOP

WORKER_DEPLOYMENT = "taskqueue-worker"
WORKER_POD_LABEL = "app=taskqueue-worker"
PRODUCER_DEPLOYMENT = "taskqueue-producer"
MIN_GAIN = 0.10  # sweep stops when a step improves on the best by less


@dataclass
class StepResult:
    workers: int
    concurrency: int
    jobs: int
    seconds: float
    throughput: float
    svc_p50_ms: float
    svc_p95_ms: float
    started_at: str  # ISO-8601 UTC, from the DB clock
    finished_at: str
    # (ISO-8601 UTC, jobs remaining) samples taken ~1/s during the drain.
    backlog_samples: list[tuple[str, int]]


def kubectl(*args: str) -> str:
    proc = subprocess.run(
        ["kubectl", *args], check=True, capture_output=True, text=True
    )
    return proc.stdout.strip()


def scale(deployment: str, replicas: int) -> None:
    kubectl("scale", f"deployment/{deployment}", f"--replicas={replicas}")


def get_replicas(deployment: str) -> int:
    out = kubectl(
        "get", f"deployment/{deployment}", "-o", "jsonpath={.spec.replicas}"
    )
    return int(out or "0")


def get_worker_concurrency() -> str:
    return kubectl(
        "get", f"deployment/{WORKER_DEPLOYMENT}", "-o",
        "jsonpath={.spec.template.spec.containers[0].env[?(@.name=='WORKER_CONCURRENCY')].value}",
    )


def wait_worker_pods_gone(timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        out = kubectl("get", "pods", "-l", WORKER_POD_LABEL, "-o", "name")
        if not out:
            return
        time.sleep(1.0)
    raise TimeoutError("worker pods did not terminate in time")


def wait_worker_rollout(timeout: float = 180.0) -> None:
    kubectl(
        "rollout", "status", f"deployment/{WORKER_DEPLOYMENT}",
        f"--timeout={int(timeout)}s",
    )


def preload(n: int, threads: int) -> float:
    """Enqueue n no-op jobs from `threads` threads. Returns elapsed seconds."""
    counts = [n // threads + (1 if i < n % threads else 0) for i in range(threads)]

    def enqueue_batch(count: int) -> None:
        for _ in range(count):
            with db.pool().connection() as conn:
                taskqueue.enqueue(
                    conn,
                    idempotency_key=str(uuid.uuid4()),
                    job_type=NOOP,
                    payload={},
                )

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for future in [executor.submit(enqueue_batch, c) for c in counts]:
            future.result()
    return time.perf_counter() - t0


def wait_drain(
    conn: psycopg.Connection, expected: int, timeout: float
) -> list[tuple[str, int]]:
    """Poll the backlog until it reaches 0. Returns the sampled drain curve."""
    deadline = time.monotonic() + timeout
    last_report = 0.0
    samples: list[tuple[str, int]] = []
    while time.monotonic() < deadline:
        row = conn.execute(
            "SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')"
        ).fetchone()
        assert row is not None
        remaining = row[0]
        samples.append((datetime.now(timezone.utc).isoformat(), remaining))
        if remaining == 0:
            return samples
        now = time.monotonic()
        if now - last_report > 15.0:
            print(f"    draining... {remaining}/{expected} jobs remaining", flush=True)
            last_report = now
        time.sleep(1.0)
    raise TimeoutError(f"queue did not drain within {timeout:.0f}s")


def measure(
    conn: psycopg.Connection,
    workers: int,
    concurrency: int,
    backlog_samples: list[tuple[str, int]],
) -> StepResult:
    row = conn.execute(
        """
        SELECT
          count(*),
          min(started_at),
          max(completed_at),
          percentile_cont(0.5) WITHIN GROUP
            (ORDER BY extract(epoch FROM completed_at - started_at)),
          percentile_cont(0.95) WITHIN GROUP
            (ORDER BY extract(epoch FROM completed_at - started_at))
        FROM jobs
        WHERE status = 'succeeded'
        """
    ).fetchone()
    assert row is not None
    jobs, t0, t1, svc_p50, svc_p95 = row
    seconds = (t1 - t0).total_seconds()
    return StepResult(
        workers=workers,
        concurrency=concurrency,
        jobs=jobs,
        seconds=seconds,
        throughput=jobs / seconds,
        svc_p50_ms=svc_p50 * 1000.0,
        svc_p95_ms=svc_p95 * 1000.0,
        started_at=t0.isoformat(),
        finished_at=t1.isoformat(),
        backlog_samples=backlog_samples,
    )


def run_step(
    conn: psycopg.Connection, workers: int, concurrency: int, args: argparse.Namespace
) -> StepResult:
    print(f"\n=== step: {workers} workers x concurrency {concurrency} ===", flush=True)
    scale(WORKER_DEPLOYMENT, 0)
    wait_worker_pods_gone()
    kubectl(
        "set", "env", f"deployment/{WORKER_DEPLOYMENT}",
        f"WORKER_CONCURRENCY={concurrency}",
        f"POOL_MAX_SIZE={concurrency + 2}",
    )
    conn.execute("TRUNCATE jobs")
    elapsed = preload(args.jobs, args.enqueue_threads)
    print(f"    preloaded {args.jobs} jobs in {elapsed:.1f}s", flush=True)
    scale(WORKER_DEPLOYMENT, workers)
    wait_worker_rollout()
    samples = wait_drain(conn, args.jobs, args.drain_timeout)

    failed = conn.execute(
        "SELECT count(*) FROM jobs WHERE status != 'succeeded'"
    ).fetchone()
    assert failed is not None
    if failed[0] != 0:
        raise RuntimeError(f"{failed[0]} jobs did not succeed; result invalid")

    result = measure(conn, workers, concurrency, samples)
    print(
        f"    {result.throughput:,.0f} jobs/s over {result.seconds:.1f}s "
        f"(service time p50 {result.svc_p50_ms:.1f}ms, p95 {result.svc_p95_ms:.1f}ms)",
        flush=True,
    )
    return result


def parse_steps(spec: str) -> list[tuple[int, int]]:
    steps = []
    for part in spec.split(","):
        w, _, c = part.strip().partition("x")
        steps.append((int(w), int(c)))
    return steps


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure peak sustained queue throughput (no-op jobs).",
    )
    parser.add_argument("--jobs", type=int, default=20000,
                        help="jobs preloaded per step (default 20000)")
    parser.add_argument("--steps", default="1x8,2x8,4x8,4x16",
                        help="sweep as WxC pairs, e.g. 1x8,2x8,4x8 (workers x concurrency)")
    parser.add_argument("--enqueue-threads", type=int, default=16,
                        help="threads used to preload jobs (default 16)")
    parser.add_argument("--max-db-connections", type=int, default=80,
                        help="skip steps whose worker pods would exceed this (default 80)")
    parser.add_argument("--drain-timeout", type=float, default=900.0,
                        help="per-step drain timeout in seconds (default 900)")
    parser.add_argument("--json", metavar="PATH",
                        help="also write results to PATH as JSON")
    args = parser.parse_args()

    steps = parse_steps(args.steps)
    original_workers = get_replicas(WORKER_DEPLOYMENT)
    original_producers = get_replicas(PRODUCER_DEPLOYMENT)
    original_concurrency = get_worker_concurrency()

    control = db.get_connection()
    control.autocommit = True

    results: list[StepResult] = []
    best: StepResult | None = None
    try:
        scale(PRODUCER_DEPLOYMENT, 0)
        for workers, concurrency in steps:
            needed = workers * (concurrency + 3)
            if needed > args.max_db_connections:
                print(
                    f"\nstopping: {workers}x{concurrency} needs ~{needed} DB "
                    f"connections (budget {args.max_db_connections})",
                    flush=True,
                )
                break
            result = run_step(control, workers, concurrency, args)
            results.append(result)
            if best is not None and result.throughput < best.throughput * (1 + MIN_GAIN):
                if result.throughput > best.throughput:
                    best = result
                print(f"\nstopping: gain over best is below {MIN_GAIN:.0%}", flush=True)
                break
            best = result
    finally:
        # Put the cluster back the way we found it. POOL_MAX_SIZE- removes
        # the env var (it isn't in the manifest).
        kubectl(
            "set", "env", f"deployment/{WORKER_DEPLOYMENT}",
            f"WORKER_CONCURRENCY={original_concurrency}", "POOL_MAX_SIZE-",
        )
        scale(WORKER_DEPLOYMENT, original_workers)
        scale(PRODUCER_DEPLOYMENT, original_producers)
        control.close()

    if not results:
        print("no steps completed")
        sys.exit(1)

    assert best is not None
    print("\n=== results ===")
    print(f"{'config':>10}  {'jobs':>7}  {'drained in':>10}  {'jobs/s':>8}  {'svc p50':>8}  {'svc p95':>8}")
    for r in results:
        print(
            f"{r.workers}x{r.concurrency:>2}{'':>4}  {r.jobs:>7,}  {r.seconds:>9.1f}s  "
            f"{r.throughput:>8,.0f}  {r.svc_p50_ms:>6.1f}ms  {r.svc_p95_ms:>6.1f}ms"
        )
    total_jobs = sum(r.jobs for r in results)
    print(
        f"\ntotal drained across {len(results)} steps: {total_jobs:,} jobs "
        f"(every job verified succeeded)"
    )
    print(
        f"peak: {best.throughput:,.0f} jobs/s "
        f"({best.workers} workers x concurrency {best.concurrency}, "
        f"{best.jobs:,} jobs in {best.seconds:.1f}s)"
    )
    t0 = datetime.fromisoformat(best.started_at)
    t1 = datetime.fromisoformat(best.finished_at)
    pad_ms = 30_000
    print(
        "dashboard window for the peak step: "
        f"from={int(t0.timestamp() * 1000) - pad_ms} "
        f"to={int(t1.timestamp() * 1000) + pad_ms} "
        f"({best.started_at} -> {best.finished_at})"
    )

    if args.json:
        with open(args.json, "w") as f:
            json.dump(
                {"jobs_per_step": args.jobs, "results": [asdict(r) for r in results],
                 "peak": asdict(best)},
                f, indent=2,
            )
        print(f"results written to {args.json}")


if __name__ == "__main__":
    main()

"""Example handlers used by the demo worker.

In a real consumer of the ``taskqueue`` library, these would do actual
work — call an API, generate a PDF, send an email, etc. Here they just
sleep and (for one of them) randomly fail, so we can demonstrate the
full success / retry / dead-letter lifecycle without external systems.

Handlers receive the job's payload (a ``dict``) and either:

- return a ``dict`` (or ``None``) → worker calls ``ack``
- raise any exception → worker calls ``nack`` (retry or dead-letter)

The ``@taskqueue.task("...")`` decorator registers each function in the
library's default registry as a side effect of importing this module. The
worker's ``build_worker()`` then picks them up automatically.
"""

from __future__ import annotations

import random
import time
from typing import Any

import taskqueue


@taskqueue.task("sleep")
def sleep_handler(payload: dict[str, Any]) -> dict[str, Any]:
    """Sleep for ``payload['duration_s']`` seconds, then succeed."""
    duration = float(payload.get("duration_s", 0.1))
    time.sleep(duration)
    return {"slept_for": duration}


@taskqueue.task("flaky")
def flaky_handler(payload: dict[str, Any]) -> dict[str, Any]:
    """Sleep briefly, then succeed or raise based on ``payload['fail_rate']``."""
    time.sleep(float(payload.get("duration_s", 0.1)))
    fail_rate = float(payload.get("fail_rate", 0.3))
    if random.random() < fail_rate:
        raise RuntimeError("simulated failure")
    return {"ok": True}


# Producer-side: the job_type strings this demo emits. The producer doesn't
# need the handler functions themselves — only the names it can enqueue.
JOB_TYPES = ["sleep", "flaky"]

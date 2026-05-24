"""Default handler registry for the ``@taskqueue.task`` decorator.

Consumers can register handlers in one of two equivalent ways:

1. Decorator (more ergonomic when handlers are spread across modules)::

    @taskqueue.task("send_email")
    def send_email(payload):
        ...

   A ``Worker`` constructed without an explicit ``handlers`` argument picks
   up everything that was registered this way.

2. Dependency injection (still supported; preferred for tests and for
   running multiple workers with different handler sets in one process)::

    worker = taskqueue.Worker(handlers={"send_email": send_email}, ...)

The decorator is just sugar over a module-level dict — registration is a
side effect of importing the handler module, which is why a consumer's
``worker_main`` must ``import myapp.handlers`` (or similar) before calling
``worker.run()``. Without that import, the decorator never executes and
the registry stays empty.
"""

from __future__ import annotations

from typing import Any, Callable

HandlerFn = Callable[[dict[str, Any]], dict[str, Any] | None]

_REGISTRY: dict[str, HandlerFn] = {}


def task(job_type: str) -> Callable[[HandlerFn], HandlerFn]:
    """Decorator: register ``fn`` as the handler for ``job_type``.

    The decorated function is returned unchanged and remains directly
    callable. Registering two handlers under the same ``job_type`` raises
    ``ValueError`` — silently overwriting would be a nightmare to debug.
    """
    def decorator(fn: HandlerFn) -> HandlerFn:
        if job_type in _REGISTRY:
            raise ValueError(
                f"handler for job_type={job_type!r} already registered: "
                f"{_REGISTRY[job_type]!r}"
            )
        _REGISTRY[job_type] = fn
        return fn
    return decorator


def registered_handlers() -> dict[str, HandlerFn]:
    """Snapshot copy of the default registry.

    ``Worker`` uses this when constructed with ``handlers=None``. Returning
    a copy means the caller can't accidentally mutate the registry by
    poking at the dict.
    """
    return dict(_REGISTRY)


def clear_registry() -> None:
    """Drop all registered handlers. Intended for tests."""
    _REGISTRY.clear()

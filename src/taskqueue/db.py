import os
import threading

import psycopg
from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()


def get_connection() -> psycopg.Connection:
    """Return a psycopg connection using DATABASE_URL from the environment."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg.connect(dsn)


def pool() -> ConnectionPool:
    """Return the process-wide connection pool, created on first use.

    Sized via POOL_MIN_SIZE / POOL_MAX_SIZE (defaults 1 / 10). Checkout
    pings the connection (check_connection) so long-lived workers get a
    fresh socket after a Postgres restart instead of crashing on a stale
    one — the ping is a fraction of the connect cost the pool saves.
    """
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ConnectionPool(
                os.environ["DATABASE_URL"],
                min_size=int(os.environ.get("POOL_MIN_SIZE", "1")),
                max_size=int(os.environ.get("POOL_MAX_SIZE", "10")),
                open=True,
                check=ConnectionPool.check_connection,
            )
        return _pool

from __future__ import annotations

import os
import pathlib

import psycopg
import pytest

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parents[1] / "migrations"


def _dsn() -> str:
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not set", allow_module_level=False)
    return dsn


@pytest.fixture(scope="session", autouse=True)
def _apply_migrations() -> None:
    dsn = _dsn()
    # Mirror TEST_DATABASE_URL into DATABASE_URL so library code that calls
    # taskqueue.db.get_connection() (the Worker, the demo service) ends up
    # pointing at the same test database the fixtures use.
    os.environ.setdefault("DATABASE_URL", dsn)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
            cur.execute(migration.read_text())
        conn.commit()


@pytest.fixture
def conn():
    dsn = _dsn()
    with psycopg.connect(dsn) as c:
        with c.cursor() as cur:
            cur.execute("TRUNCATE jobs")
        c.commit()
        yield c


@pytest.fixture
def make_conn():
    """Factory for additional connections (e.g. concurrent tests)."""
    dsn = _dsn()
    opened: list[psycopg.Connection] = []

    def _make() -> psycopg.Connection:
        c = psycopg.connect(dsn)
        opened.append(c)
        return c

    yield _make
    for c in opened:
        c.close()

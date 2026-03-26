"""Postgres persistence for translator run state."""

import logging
from contextlib import contextmanager
from typing import Generator
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

from src.services.config import SETTINGS

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS translator_runs (
    run_id UUID PRIMARY KEY,
    dsl TEXT NOT NULL,
    phase VARCHAR NOT NULL DEFAULT 'pending',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


@contextmanager
def get_connection(
    database_url: str = SETTINGS.DATABASE_URL,
) -> Generator[psycopg.Connection, None, None]:
    """Open a Postgres connection as a context manager.

    Args:
        database_url: Postgres connection string.

    Yields:
        Active psycopg connection.
    """
    conn = psycopg.connect(conninfo=database_url)
    try:
        yield conn
    finally:
        conn.close()


def init_db(conn: psycopg.Connection) -> None:
    """Create the translator_runs table if it does not exist.

    Args:
        conn: Active Postgres connection.
    """
    with conn.cursor() as cur:
        cur.execute(query=CREATE_TABLE_SQL)
    conn.commit()
    logger.info("translator_runs table initialized")


def create_run(conn: psycopg.Connection, *, dsl: str) -> UUID:
    """Insert a new run with phase 'pending'.

    Args:
        conn: Active Postgres connection.
        dsl: Raw DSL string from the user request.

    Returns:
        UUID of the created run.
    """
    run_id = uuid4()
    sql = """
    INSERT INTO translator_runs (run_id, dsl, phase)
    VALUES (%s, %s, 'pending');
    """
    with conn.cursor() as cur:
        cur.execute(query=sql, params=(run_id, dsl))
    conn.commit()
    logger.info("created run: run_id=%s", run_id)
    return run_id


def get_run(conn: psycopg.Connection, *, run_id: UUID) -> dict | None:
    """Fetch a run by ID.

    Args:
        conn: Active Postgres connection.
        run_id: UUID of the run to fetch.

    Returns:
        Dict with run data, or None if not found.
    """
    sql = """
    SELECT run_id, dsl, phase, error, created_at, updated_at
    FROM translator_runs
    WHERE run_id = %s;
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query=sql, params=(run_id,))
        return cur.fetchone()


def update_run(
    conn: psycopg.Connection,
    *,
    run_id: UUID,
    phase: str,
    error: str | None = None,
) -> None:
    """Update the phase and error of a run.

    Args:
        conn: Active Postgres connection.
        run_id: UUID of the run to update.
        phase: New phase value.
        error: Error message, or None to clear.
    """
    sql = """
    UPDATE translator_runs
    SET phase = %s, error = %s, updated_at = NOW()
    WHERE run_id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query=sql, params=(phase, error, run_id))
    conn.commit()
    logger.info("updated run: run_id=%s, phase=%s", run_id, phase)


if __name__ == "__main__":
    with get_connection() as conn:
        init_db(conn=conn)
        rid = create_run(
            conn=conn,
            dsl='{"collect": {"year": 2023, "month": 1, "taxi_type": "yellow"}}',
        )
        print(f"created run: {rid}")
        run = get_run(conn=conn, run_id=rid)
        print(f"fetched run: {run}")
        update_run(conn=conn, run_id=rid, phase="completed")
        run = get_run(conn=conn, run_id=rid)
        print(f"updated run: {run}")

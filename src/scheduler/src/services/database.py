"""Postgres job state persistence for the scheduler service."""

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

import psycopg2
from psycopg2.extensions import connection as PgConnection
from pydantic import BaseModel, ConfigDict, Field

from src.services.config import SETTINGS

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS job_state (
    job_id SERIAL PRIMARY KEY,
    object_name TEXT NOT NULL,
    bucket TEXT NOT NULL,
    current_step TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    completed_steps JSONB NOT NULL DEFAULT '[]',
    failed_step TEXT,
    dag_steps JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (object_name, bucket)
);
"""

ADD_DAG_STEPS_COLUMN_SQL = """
ALTER TABLE job_state ADD COLUMN IF NOT EXISTS dag_steps JSONB;
"""


class JobRecord(BaseModel):
    """A job state row from the database."""

    model_config = ConfigDict(frozen=True)

    job_id: int
    object_name: str
    bucket: str
    current_step: str | None = None
    status: str = "pending"
    completed_steps: list[str] = Field(default_factory=list)
    failed_step: str | None = None
    dag_steps: list[dict[str, object]] | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))


@contextmanager
def get_connection(
    database_url: str = SETTINGS.DATABASE_URL,
) -> Generator[PgConnection, None, None]:
    """Open a Postgres connection as a context manager.

    Args:
        database_url: Postgres connection string.

    Yields:
        Active psycopg2 connection.
    """
    conn = psycopg2.connect(dsn=database_url)
    try:
        yield conn
    finally:
        conn.close()


def init_schema(conn: PgConnection) -> None:
    """Create the job_state table if it does not exist.

    Also adds the dag_steps column for existing databases.

    Args:
        conn: Active Postgres connection.
    """
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(ADD_DAG_STEPS_COLUMN_SQL)
    conn.commit()
    logger.info("job_state table initialized")


def save_job_state(
    conn: PgConnection,
    *,
    object_name: str,
    bucket: str,
    current_step: str | None,
    status: str,
    completed_steps: list[str],
    failed_step: str | None,
    dag_steps: list[dict[str, object]] | None = None,
) -> None:
    """Upsert a job state row.

    Args:
        conn: Active Postgres connection.
        object_name: S3 object path.
        bucket: S3 bucket name.
        current_step: Current pipeline step.
        status: Job status.
        completed_steps: List of completed step names.
        failed_step: Step that failed, if any.
        dag_steps: Serialized DAG step definitions for resume, or None for linear.
    """
    sql = """
    INSERT INTO job_state (object_name, bucket, current_step, status, completed_steps, failed_step, dag_steps, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (object_name, bucket)
    DO UPDATE SET
        current_step = EXCLUDED.current_step,
        status = EXCLUDED.status,
        completed_steps = EXCLUDED.completed_steps,
        failed_step = EXCLUDED.failed_step,
        dag_steps = EXCLUDED.dag_steps,
        updated_at = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                object_name,
                bucket,
                current_step,
                status,
                json.dumps(completed_steps),
                failed_step,
                json.dumps(dag_steps) if dag_steps is not None else None,
            ),
        )
    conn.commit()
    logger.info("saved job state: object_name=%s, status=%s", object_name, status)


def get_in_progress_jobs(conn: PgConnection) -> list[JobRecord]:
    """Fetch all jobs with status 'in_progress'.

    Args:
        conn: Active Postgres connection.

    Returns:
        List of in-progress JobRecord instances.
    """
    sql = """
    SELECT job_id, object_name, bucket, current_step, status,
           completed_steps, failed_step, dag_steps, created_at, updated_at
    FROM job_state
    WHERE status = 'in_progress';
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [_row_to_record(row=row) for row in rows]


def get_failed_jobs(conn: PgConnection) -> list[JobRecord]:
    """Fetch all jobs with status 'failed'.

    Args:
        conn: Active Postgres connection.

    Returns:
        List of failed JobRecord instances.
    """
    sql = """
    SELECT job_id, object_name, bucket, current_step, status,
           completed_steps, failed_step, dag_steps, created_at, updated_at
    FROM job_state
    WHERE status = 'failed';
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [_row_to_record(row=row) for row in rows]


def get_job_history(conn: PgConnection) -> list[JobRecord]:
    """Fetch all job state rows ordered by creation time.

    Args:
        conn: Active Postgres connection.

    Returns:
        List of all JobRecord instances.
    """
    sql = """
    SELECT job_id, object_name, bucket, current_step, status,
           completed_steps, failed_step, dag_steps, created_at, updated_at
    FROM job_state
    ORDER BY created_at;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [_row_to_record(row=row) for row in rows]


def _row_to_record(*, row: tuple) -> JobRecord:
    """Convert a database row tuple to a JobRecord.

    Args:
        row: Tuple of column values from the job_state table.

    Returns:
        JobRecord instance.
    """
    return JobRecord(
        job_id=row[0],
        object_name=row[1],
        bucket=row[2],
        current_step=row[3],
        status=row[4],
        completed_steps=row[5],
        failed_step=row[6],
        dag_steps=row[7],
        created_at=row[8],
        updated_at=row[9],
    )


if __name__ == "__main__":
    with get_connection() as conn:
        init_schema(conn=conn)
        save_job_state(
            conn=conn,
            object_name="yellow/2022/01/yellow_tripdata_2022-01.parquet",
            bucket="raw-data",
            current_step="descriptive_statistics",
            status="in_progress",
            completed_steps=[],
            failed_step=None,
        )
        history = get_job_history(conn=conn)
        print(f"Job history: {[r.model_dump() for r in history]}")

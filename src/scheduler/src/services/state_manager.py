"""Thin wrapper around Postgres for job state queries."""

import logging

from psycopg2.extensions import connection as PgConnection

from src.services.database import JobRecord
from src.services.database import get_failed_jobs as db_get_failed_jobs
from src.services.database import get_in_progress_jobs as db_get_in_progress_jobs

logger = logging.getLogger(__name__)


class StateManager:
    """Queries job state from Postgres.

    Attributes:
        _conn: Active Postgres connection.
    """

    def __init__(self, *, conn: PgConnection) -> None:
        self._conn = conn

    def get_failed_jobs(self) -> list[JobRecord]:
        """Fetch all failed jobs from Postgres.

        Returns:
            List of failed JobRecord instances.
        """
        return db_get_failed_jobs(conn=self._conn)

    def get_in_progress_jobs(self) -> list[JobRecord]:
        """Fetch all in-progress jobs from Postgres.

        Returns:
            List of in-progress JobRecord instances.
        """
        return db_get_in_progress_jobs(conn=self._conn)


if __name__ == "__main__":
    from src.services.database import get_connection, init_schema, save_job_state

    with get_connection() as conn:
        init_schema(conn=conn)
        save_job_state(
            conn=conn,
            object_name="yellow/2022/01/yellow_tripdata_2022-01.parquet",
            bucket="raw-data",
            current_step="descriptive_statistics",
            status="failed",
            completed_steps=[],
            failed_step="descriptive_statistics",
        )
        manager = StateManager(conn=conn)
        failed = manager.get_failed_jobs()
        print(f"Failed jobs: {[r.model_dump() for r in failed]}")

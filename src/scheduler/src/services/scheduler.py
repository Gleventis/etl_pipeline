"""Core orchestration logic for the scheduler service."""

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, wait

from src.server.models import FileStatus, ResumedJob
from src.services.config import Settings
from src.services.database import get_connection
from src.services.prefect_flows import process_file_flow
from src.services.state_manager import StateManager

logger = logging.getLogger(__name__)


class SchedulerService:
    """Orchestrates the analytical pipeline for batches of files.

    Triggers Prefect flow runs per file instead of managing threads directly.

    Attributes:
        _settings: Scheduler configuration.
        _db_url: Postgres connection string for flow runs.
    """

    def __init__(self, *, settings: Settings, db_url: str) -> None:
        self._settings = settings
        self._db_url = db_url

    def schedule_batch(
        self,
        *,
        bucket: str,
        objects: list[str],
        skip_checkpoints: list[str] | None = None,
    ) -> list[FileStatus]:
        """Schedule a batch of files for pipeline processing.

        Checks for in-progress jobs, then triggers a Prefect flow run
        per file and waits for all to complete.

        Args:
            bucket: MinIO bucket where the files reside.
            objects: List of S3 object paths to process.
            skip_checkpoints: Step names for which checkpoint persistence is skipped.

        Returns:
            List of FileStatus with per-file scheduling outcome.
        """
        statuses: list[FileStatus] = []
        processable: list[str] = []

        with get_connection(database_url=self._db_url) as conn:
            manager = StateManager(conn=conn)
            in_progress = {r.object_name for r in manager.get_in_progress_jobs()}

        for obj in objects:
            if obj in in_progress:
                statuses.append(
                    FileStatus(object_name=obj, status="already_in_progress")
                )
                continue
            statuses.append(FileStatus(object_name=obj, status="started"))
            processable.append(obj)

        pipeline_run_id = uuid.uuid4().hex
        self._run_flows_concurrently(
            pipeline_run_id=pipeline_run_id,
            args=[{"object_name": obj, "bucket": bucket} for obj in processable],
            skip_checkpoints=skip_checkpoints or [],
        )

        return statuses

    def resume_failed(self) -> list[ResumedJob]:
        """Resume all failed jobs from their failed step.

        Reads failed jobs from Postgres, triggers a Prefect flow run
        per failed job with start_step set to the failed step.

        Returns:
            List of ResumedJob with object name and restart step.
        """
        with get_connection(database_url=self._db_url) as conn:
            manager = StateManager(conn=conn)
            failed_records = manager.get_failed_jobs()

        if not failed_records:
            return []

        resumed: list[ResumedJob] = []
        flow_args: list[dict[str, str]] = []
        for record in failed_records:
            if not record.failed_step:
                continue
            resumed.append(
                ResumedJob(
                    object_name=record.object_name,
                    restart_step=record.failed_step,
                )
            )
            flow_args.append(
                {
                    "object_name": record.object_name,
                    "bucket": record.bucket,
                    "start_step": record.failed_step,
                }
            )

        pipeline_run_id = uuid.uuid4().hex
        self._run_flows_concurrently(
            pipeline_run_id=pipeline_run_id,
            args=flow_args,
        )

        return resumed

    def _run_flows_concurrently(
        self,
        *,
        pipeline_run_id: str,
        args: list[dict[str, str]],
        skip_checkpoints: list[str] | None = None,
    ) -> None:
        """Submit flow runs to a thread pool and wait for all to complete.

        Args:
            pipeline_run_id: Unique identifier for this pipeline run.
            args: List of dicts with per-flow keyword arguments
                  (object_name, bucket, and optionally start_step).
            skip_checkpoints: Step names for which checkpoint persistence is skipped.
        """
        with ThreadPoolExecutor(max_workers=len(args) or 1) as executor:
            futures = [
                executor.submit(
                    process_file_flow,
                    object_name=kw["object_name"],
                    bucket=kw["bucket"],
                    settings=self._settings,
                    db_url=self._db_url,
                    pipeline_run_id=pipeline_run_id,
                    start_step=kw.get("start_step"),
                    skip_checkpoints=skip_checkpoints or [],
                )
                for kw in args
            ]
            wait(futures)


if __name__ == "__main__":
    from src.services.config import SETTINGS

    service = SchedulerService(
        settings=SETTINGS,
        db_url=SETTINGS.DATABASE_URL,
    )
    statuses = service.schedule_batch(
        bucket="data-collector",
        objects=["yellow/2022/01/yellow_tripdata_2022-01.parquet"],
    )
    print(f"Statuses: {[s.model_dump() for s in statuses]}")

"""Tests for the executor module."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import httpx
import pytest

from src.services.executor import execute_run
from src.services.parser import (
    AggregateCommand,
    AnalyzeCommand,
    CollectCommand,
    ParsedDSL,
)


@pytest.fixture()
def run_id():
    """Generate a fresh run ID for each test."""
    return uuid4()


@pytest.fixture()
def mock_conn():
    """Mock DB connection returned by get_connection."""
    conn = MagicMock()
    return conn


@pytest.fixture()
def _patch_db(mock_conn):
    """Patch get_connection to yield mock_conn."""
    with patch(
        "src.services.executor.get_connection",
    ) as mock_get:
        mock_get.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get.return_value.__exit__ = MagicMock(return_value=False)
        yield


FULL_DSL = ParsedDSL(
    collect=CollectCommand(year=2024, month=1, taxi_type="yellow"),
    analyze=AnalyzeCommand(bucket="data-collector", objects=["yellow/2024-01.parquet"]),
    aggregate=AggregateCommand(
        endpoint="descriptive-stats", params={"taxi_type": "yellow"}
    ),
)


@pytest.mark.usefixtures("_patch_db")
class TestExecuteRun:
    """Tests for execute_run."""

    @patch("src.services.executor.call_aggregator", return_value={"data": [1, 2]})
    @patch("src.services.executor.call_scheduler", return_value={"status": "ok"})
    @patch(
        "src.services.executor.call_collector", return_value={"files": ["a.parquet"]}
    )
    @patch("src.services.executor.update_run")
    def test_full_pipeline_updates_phases(
        self, mock_update, mock_collector, mock_scheduler, mock_aggregator, run_id
    ):
        """Full DSL runs collect → analyze → aggregate → completed with correct phases."""
        execute_run(run_id=run_id, parsed=FULL_DSL)

        mock_collector.assert_called_once()
        mock_scheduler.assert_called_once()
        mock_aggregator.assert_called_once()

        phases = [c.kwargs["phase"] for c in mock_update.call_args_list]
        assert phases == ["collecting", "analyzing", "aggregating", "completed"]

    @patch(
        "src.services.executor.call_collector", return_value={"files": ["a.parquet"]}
    )
    @patch("src.services.executor.update_run")
    def test_collect_only_phases(self, mock_update, mock_collector, run_id):
        """DSL with only collect sets collecting → completed."""
        parsed = ParsedDSL(
            collect=CollectCommand(year=2024, month=1, taxi_type="yellow"),
        )
        execute_run(run_id=run_id, parsed=parsed)

        mock_collector.assert_called_once()
        phases = [c.kwargs["phase"] for c in mock_update.call_args_list]
        assert phases == ["collecting", "completed"]

    @patch("src.services.executor.call_scheduler", return_value={"status": "ok"})
    @patch("src.services.executor.update_run")
    def test_analyze_only_phases(self, mock_update, mock_scheduler, run_id):
        """DSL with only analyze sets analyzing → completed."""
        parsed = ParsedDSL(
            analyze=AnalyzeCommand(
                bucket="data-collector", objects=["yellow/2024-01.parquet"]
            ),
        )
        execute_run(run_id=run_id, parsed=parsed)

        mock_scheduler.assert_called_once()
        phases = [c.kwargs["phase"] for c in mock_update.call_args_list]
        assert phases == ["analyzing", "completed"]

    @patch("src.services.executor.call_aggregator", return_value={"data": [1]})
    @patch("src.services.executor.update_run")
    def test_aggregate_only_phases(self, mock_update, mock_aggregator, run_id):
        """DSL with only aggregate sets aggregating → completed."""
        parsed = ParsedDSL(
            aggregate=AggregateCommand(
                endpoint="descriptive-stats", params={"taxi_type": "yellow"}
            ),
        )
        execute_run(run_id=run_id, parsed=parsed)

        mock_aggregator.assert_called_once()
        phases = [c.kwargs["phase"] for c in mock_update.call_args_list]
        assert phases == ["aggregating", "completed"]

    @patch(
        "src.services.executor.call_collector",
        side_effect=httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock()
        ),
    )
    @patch("src.services.executor.update_run")
    @patch("src.services.executor.get_connection")
    def test_collector_failure_sets_failed(
        self, mock_get_conn, mock_update, mock_collector, run_id
    ):
        """Downstream failure sets phase to 'failed' with error."""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        parsed = ParsedDSL(
            collect=CollectCommand(year=2024, month=1, taxi_type="yellow"),
        )
        execute_run(run_id=run_id, parsed=parsed)

        failed_calls = [
            c for c in mock_update.call_args_list if c.kwargs.get("phase") == "failed"
        ]
        assert len(failed_calls) == 1
        assert failed_calls[0].kwargs["error"] is not None

    @patch("src.services.executor.call_aggregator", return_value={})
    @patch("src.services.executor.update_run")
    @patch("src.services.executor.get_connection")
    def test_aggregator_empty_response_stores_412(
        self, mock_get_conn, mock_update, mock_aggregator, run_id
    ):
        """Aggregator returning empty data stores 412 error."""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        parsed = ParsedDSL(
            aggregate=AggregateCommand(
                endpoint="descriptive-stats", params={"taxi_type": "yellow"}
            ),
        )
        execute_run(run_id=run_id, parsed=parsed)

        failed_calls = [
            c for c in mock_update.call_args_list if c.kwargs.get("phase") == "failed"
        ]
        assert len(failed_calls) == 1
        assert "412" in failed_calls[0].kwargs["error"]

    @patch("src.services.executor.call_aggregator", return_value=[])
    @patch("src.services.executor.update_run")
    @patch("src.services.executor.get_connection")
    def test_aggregator_empty_list_stores_412(
        self, mock_get_conn, mock_update, mock_aggregator, run_id
    ):
        """Aggregator returning empty list also stores 412 error."""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        parsed = ParsedDSL(
            aggregate=AggregateCommand(endpoint="data-quality", params={}),
        )
        execute_run(run_id=run_id, parsed=parsed)

        failed_calls = [
            c for c in mock_update.call_args_list if c.kwargs.get("phase") == "failed"
        ]
        assert len(failed_calls) == 1
        assert "412" in failed_calls[0].kwargs["error"]

    @patch(
        "src.services.executor.call_scheduler",
        side_effect=httpx.HTTPStatusError(
            "503", request=MagicMock(), response=MagicMock()
        ),
    )
    @patch(
        "src.services.executor.call_collector", return_value={"files": ["a.parquet"]}
    )
    @patch("src.services.executor.update_run")
    @patch("src.services.executor.get_connection")
    def test_failure_mid_pipeline_stops_execution(
        self, mock_get_conn, mock_update, mock_collector, mock_scheduler, run_id
    ):
        """Failure in analyze step stops before aggregate."""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        execute_run(run_id=run_id, parsed=FULL_DSL)

        # Should never reach "completed"
        completed_calls = [
            c
            for c in mock_update.call_args_list
            if c.kwargs.get("phase") == "completed"
        ]
        assert len(completed_calls) == 0

        # Should have "failed"
        failed_calls = [
            c for c in mock_update.call_args_list if c.kwargs.get("phase") == "failed"
        ]
        assert len(failed_calls) == 1

    @patch("src.services.executor.call_aggregator", return_value={"data": [1, 2]})
    @patch("src.services.executor.call_scheduler", return_value={"status": "ok"})
    @patch(
        "src.services.executor.call_collector", return_value={"files": ["a.parquet"]}
    )
    @patch("src.services.executor.update_run")
    def test_full_pipeline_passes_correct_commands(
        self, mock_update, mock_collector, mock_scheduler, mock_aggregator, run_id
    ):
        """HTTP clients receive the correct command objects from parsed DSL."""
        execute_run(run_id=run_id, parsed=FULL_DSL)

        mock_collector.assert_called_once_with(cmd=FULL_DSL.collect)
        mock_scheduler.assert_called_once_with(cmd=FULL_DSL.analyze)
        mock_aggregator.assert_called_once_with(cmd=FULL_DSL.aggregate)

    @patch(
        "src.services.executor.call_collector",
        side_effect=httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock()
        ),
    )
    @patch("src.services.executor.update_run")
    @patch("src.services.executor.get_connection")
    def test_failure_in_collect_skips_analyze_and_aggregate(
        self, mock_get_conn, mock_update, mock_collector, run_id
    ):
        """When collect fails, analyze and aggregate are never called."""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch("src.services.executor.call_scheduler") as mock_scheduler,
            patch("src.services.executor.call_aggregator") as mock_aggregator,
        ):
            execute_run(run_id=run_id, parsed=FULL_DSL)

            mock_scheduler.assert_not_called()
            mock_aggregator.assert_not_called()

    @patch("src.services.executor.call_aggregator", return_value={})
    @patch("src.services.executor.update_run")
    @patch("src.services.executor.get_connection")
    def test_aggregator_empty_does_not_reach_completed(
        self, mock_get_conn, mock_update, mock_aggregator, run_id
    ):
        """Empty aggregator result stops at failed, never reaches completed."""
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)

        parsed = ParsedDSL(
            aggregate=AggregateCommand(
                endpoint="descriptive-stats", params={"taxi_type": "yellow"}
            ),
        )
        execute_run(run_id=run_id, parsed=parsed)

        phases = [c.kwargs["phase"] for c in mock_update.call_args_list]
        assert "completed" not in phases
        assert "failed" in phases

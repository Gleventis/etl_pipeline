"""Tests for translator database operations."""

from uuid import UUID, uuid4

from src.services.db import create_run, get_run, update_run


class TestCreateRun:
    """Tests for create_run function."""

    def test_returns_uuid(self, conn):
        run_id = create_run(conn=conn, dsl='{"collect": {}}')
        assert isinstance(run_id, UUID)

    def test_inserts_row_with_pending_phase(self, conn):
        run_id = create_run(conn=conn, dsl="test dsl")
        row = get_run(conn=conn, run_id=run_id)
        assert row is not None
        assert row["run_id"] == run_id
        assert row["dsl"] == "test dsl"
        assert row["phase"] == "pending"
        assert row["error"] is None


class TestGetRun:
    """Tests for get_run function."""

    def test_returns_none_for_missing_id(self, conn):
        result = get_run(conn=conn, run_id=uuid4())
        assert result is None

    def test_returns_all_columns(self, conn):
        run_id = create_run(conn=conn, dsl="some dsl")
        row = get_run(conn=conn, run_id=run_id)
        assert row["run_id"] == run_id
        assert row["dsl"] == "some dsl"
        assert row["phase"] == "pending"
        assert row["error"] is None
        assert row["created_at"] is not None
        assert row["updated_at"] is not None


class TestUpdateRun:
    """Tests for update_run function."""

    def test_changes_phase(self, conn):
        run_id = create_run(conn=conn, dsl="test")
        update_run(conn=conn, run_id=run_id, phase="collecting")
        row = get_run(conn=conn, run_id=run_id)
        assert row["phase"] == "collecting"

    def test_sets_error(self, conn):
        run_id = create_run(conn=conn, dsl="test")
        update_run(conn=conn, run_id=run_id, phase="failed", error="boom")
        row = get_run(conn=conn, run_id=run_id)
        assert row["phase"] == "failed"
        assert row["error"] == "boom"

    def test_clears_error(self, conn):
        run_id = create_run(conn=conn, dsl="test")
        update_run(conn=conn, run_id=run_id, phase="failed", error="boom")
        update_run(conn=conn, run_id=run_id, phase="collecting", error=None)
        row = get_run(conn=conn, run_id=run_id)
        assert row["phase"] == "collecting"
        assert row["error"] is None

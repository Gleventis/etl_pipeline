"""Tests for get_ready_steps in dag.py."""

import pytest

from src.services.dag import get_ready_steps

# Reusable spec DAG: desc → clean → [temporal, geospatial] → fare
SPEC_STEPS = [
    "descriptive_statistics",
    "data_cleaning",
    "temporal_analysis",
    "geospatial_analysis",
    "fare_revenue_analysis",
]
SPEC_EDGES = [
    ("data_cleaning", "descriptive_statistics"),
    ("temporal_analysis", "data_cleaning"),
    ("geospatial_analysis", "data_cleaning"),
    ("fare_revenue_analysis", "temporal_analysis"),
    ("fare_revenue_analysis", "geospatial_analysis"),
]


class TestGetReadySteps:
    """Tests for get_ready_steps function."""

    def test_empty_completed_returns_entry_points(self) -> None:
        result = get_ready_steps(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps=set(),
        )

        assert result == ["descriptive_statistics"]

    def test_after_first_step_returns_successor(self) -> None:
        result = get_ready_steps(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps={"descriptive_statistics"},
        )

        assert result == ["data_cleaning"]

    def test_after_cleaning_returns_parallel_branches(self) -> None:
        result = get_ready_steps(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps={"descriptive_statistics", "data_cleaning"},
        )

        assert set(result) == {"temporal_analysis", "geospatial_analysis"}

    def test_partial_branch_does_not_unlock_fan_in(self) -> None:
        """Only temporal done — fare_revenue needs both temporal AND geospatial."""
        result = get_ready_steps(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps={
                "descriptive_statistics",
                "data_cleaning",
                "temporal_analysis",
            },
        )

        assert result == ["geospatial_analysis"]

    def test_both_branches_done_unlocks_fan_in(self) -> None:
        result = get_ready_steps(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps={
                "descriptive_statistics",
                "data_cleaning",
                "temporal_analysis",
                "geospatial_analysis",
            },
        )

        assert result == ["fare_revenue_analysis"]

    def test_all_completed_returns_empty(self) -> None:
        result = get_ready_steps(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps=set(SPEC_STEPS),
        )

        assert result == []

    def test_no_edges_all_ready_immediately(self) -> None:
        steps = ["a", "b", "c"]

        result = get_ready_steps(
            all_steps=steps,
            edges=[],
            completed_steps=set(),
        )

        assert set(result) == {"a", "b", "c"}

    def test_single_step_no_edges(self) -> None:
        result = get_ready_steps(
            all_steps=["only"],
            edges=[],
            completed_steps=set(),
        )

        assert result == ["only"]

    def test_single_step_already_completed(self) -> None:
        result = get_ready_steps(
            all_steps=["only"],
            edges=[],
            completed_steps={"only"},
        )

        assert result == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

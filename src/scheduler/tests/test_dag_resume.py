"""Tests for get_incomplete_with_dependents in dag.py."""

import pytest

from src.services.dag import get_incomplete_with_dependents

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


class TestGetIncompleteWithDependents:
    """Tests for get_incomplete_with_dependents function."""

    def test_partial_branch_failure_returns_incomplete_and_downstream(self) -> None:
        """4a (geospatial) done, 4b (temporal) failed → returns [temporal, fare]."""
        result = get_incomplete_with_dependents(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps={
                "descriptive_statistics",
                "data_cleaning",
                "geospatial_analysis",
            },
        )

        assert result == ["temporal_analysis", "fare_revenue_analysis"]

    def test_full_failure_returns_all_steps(self) -> None:
        result = get_incomplete_with_dependents(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps=set(),
        )

        assert len(result) == len(SPEC_STEPS)
        assert result[0] == "descriptive_statistics"
        assert result[-1] == "fare_revenue_analysis"

    def test_all_completed_returns_empty(self) -> None:
        result = get_incomplete_with_dependents(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps=set(SPEC_STEPS),
        )

        assert result == []

    def test_only_first_step_completed(self) -> None:
        result = get_incomplete_with_dependents(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps={"descriptive_statistics"},
        )

        assert result[0] == "data_cleaning"
        assert "descriptive_statistics" not in result
        assert len(result) == 4

    def test_preserves_topological_order(self) -> None:
        """Returned steps must be in valid execution order."""
        result = get_incomplete_with_dependents(
            all_steps=SPEC_STEPS,
            edges=SPEC_EDGES,
            completed_steps={"descriptive_statistics"},
        )

        # data_cleaning must come before temporal/geospatial
        assert result.index("data_cleaning") < result.index("temporal_analysis")
        assert result.index("data_cleaning") < result.index("geospatial_analysis")
        # fare_revenue must come after both branches
        assert result.index("fare_revenue_analysis") > result.index("temporal_analysis")
        assert result.index("fare_revenue_analysis") > result.index(
            "geospatial_analysis"
        )

    def test_single_step_incomplete(self) -> None:
        result = get_incomplete_with_dependents(
            all_steps=["only"],
            edges=[],
            completed_steps=set(),
        )

        assert result == ["only"]

    def test_single_step_completed(self) -> None:
        result = get_incomplete_with_dependents(
            all_steps=["only"],
            edges=[],
            completed_steps={"only"},
        )

        assert result == []

    def test_no_edges_returns_all_incomplete(self) -> None:
        steps = ["a", "b", "c"]

        result = get_incomplete_with_dependents(
            all_steps=steps,
            edges=[],
            completed_steps={"b"},
        )

        assert set(result) == {"a", "c"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

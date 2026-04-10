"""Tests for topological sort in dag.py."""

import pytest

from src.services.dag import CycleDetectedError, topological_sort


class TestTopologicalSort:
    """Tests for topological_sort function."""

    def test_linear_chain_preserves_order(self) -> None:
        steps = ["a", "b", "c"]
        edges = [("b", "a"), ("c", "b")]

        result = topological_sort(steps=steps, edges=edges)

        assert result == ["a", "b", "c"]

    def test_branching_dag_respects_dependencies(self) -> None:
        """The spec DAG: desc_stats → cleaning → [temporal, geospatial] → fare."""
        steps = [
            "descriptive_statistics",
            "data_cleaning",
            "temporal_analysis",
            "geospatial_analysis",
            "fare_revenue_analysis",
        ]
        edges = [
            ("data_cleaning", "descriptive_statistics"),
            ("temporal_analysis", "data_cleaning"),
            ("geospatial_analysis", "data_cleaning"),
            ("fare_revenue_analysis", "temporal_analysis"),
            ("fare_revenue_analysis", "geospatial_analysis"),
        ]

        result = topological_sort(steps=steps, edges=edges)

        assert result[0] == "descriptive_statistics"
        assert result[1] == "data_cleaning"
        assert set(result[2:4]) == {"temporal_analysis", "geospatial_analysis"}
        assert result[4] == "fare_revenue_analysis"

    def test_single_node_no_edges(self) -> None:
        result = topological_sort(steps=["only"], edges=[])

        assert result == ["only"]

    def test_no_edges_returns_all_steps(self) -> None:
        steps = ["a", "b", "c"]

        result = topological_sort(steps=steps, edges=[])

        assert set(result) == set(steps)

    def test_direct_cycle_raises(self) -> None:
        steps = ["a", "b"]
        edges = [("a", "b"), ("b", "a")]

        with pytest.raises(CycleDetectedError, match="cycle"):
            topological_sort(steps=steps, edges=edges)

    def test_self_loop_raises(self) -> None:
        steps = ["a"]
        edges = [("a", "a")]

        with pytest.raises(CycleDetectedError, match="cycle"):
            topological_sort(steps=steps, edges=edges)

    def test_indirect_cycle_raises(self) -> None:
        steps = ["a", "b", "c"]
        edges = [("b", "a"), ("c", "b"), ("a", "c")]

        with pytest.raises(CycleDetectedError, match="cycle"):
            topological_sort(steps=steps, edges=edges)

    def test_diamond_dag(self) -> None:
        """A → [B, C] → D (diamond shape)."""
        steps = ["a", "b", "c", "d"]
        edges = [("b", "a"), ("c", "a"), ("d", "b"), ("d", "c")]

        result = topological_sort(steps=steps, edges=edges)

        assert result[0] == "a"
        assert set(result[1:3]) == {"b", "c"}
        assert result[3] == "d"

    def test_fan_out_only(self) -> None:
        """A → [B, C, D] — one root, three leaves."""
        steps = ["a", "b", "c", "d"]
        edges = [("b", "a"), ("c", "a"), ("d", "a")]

        result = topological_sort(steps=steps, edges=edges)

        assert result[0] == "a"
        assert set(result[1:]) == {"b", "c", "d"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

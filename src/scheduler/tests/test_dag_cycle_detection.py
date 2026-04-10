"""Tests for cycle detection in dag.py."""

import pytest

from src.services.dag import CycleDetectedError, topological_sort


class TestCycleDetection:
    """Tests for cycle detection via topological_sort."""

    def test_direct_cycle_raises(self) -> None:
        """A depends on B, B depends on A."""
        steps = ["a", "b"]
        edges = [("a", "b"), ("b", "a")]

        with pytest.raises(CycleDetectedError, match="cycle"):
            topological_sort(steps=steps, edges=edges)

    def test_self_loop_raises(self) -> None:
        """A depends on itself."""
        steps = ["a"]
        edges = [("a", "a")]

        with pytest.raises(CycleDetectedError, match="cycle"):
            topological_sort(steps=steps, edges=edges)

    def test_valid_acyclic_graph_does_not_raise(self) -> None:
        """A → B → C is acyclic and should succeed."""
        steps = ["a", "b", "c"]
        edges = [("b", "a"), ("c", "b")]

        result = topological_sort(steps=steps, edges=edges)

        assert result == ["a", "b", "c"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

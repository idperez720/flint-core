"""Unit tests for the multi-output matricial pipeline orchestration engine."""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import patch
import pytest

from flint_core.core.catalog import DataCatalog
from flint_core.core.exceptions import (
    CircularDependencyError,
    PipelineExecutionError,
)
from flint_core.core.pipeline import Node, Pipeline, PipelineRegistry, PipelineRunner


def test_node_positional_tuple_unpacking(tmp_path: Any) -> None:
    """Asserts that a node returning a tuple correctly maps to multiple outputs."""
    pd = pytest.importorskip("pandas")

    catalog_dir = tmp_path / "conf" / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)
    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\nname = "test-pipeline-unpack"\n')

    with open(catalog_dir / "pipe.yaml", "w", encoding="utf-8") as f:
        f.write(
            "input_ds:\n  engine: 'pandas'\n  format: 'csv'\n"
            "  storage_path: 'data/in.csv'\n"
            "out_a:\n  engine: 'pandas'\n  format: 'csv'\n"
            "  storage_path: 'data/a.csv'\n"
            "out_b:\n  engine: 'pandas'\n  format: 'csv'\n"
            "  storage_path: 'data/b.csv'\n"
        )
    (tmp_path / "data").mkdir(exist_ok=True)
    with open(tmp_path / "data/in.csv", "w", encoding="utf-8") as f:
        f.write("id\n1\n2\n")

    catalog = DataCatalog(catalog_path=catalog_dir)

    def split_transformation(input_ds: Any) -> tuple[Any, Any]:
        df_a = input_ds[input_ds["id"] == 1]
        df_b = input_ds[input_ds["id"] == 2]
        return df_a, df_b

    node = Node(
        name="split_node",
        func=split_transformation,
        inputs=["input_ds"],
        outputs=["out_a", "out_b"],
    )

    res = node.run(catalog=catalog)
    assert "out_a" in res
    assert "out_b" in res
    assert len(res["out_a"]) == 1
    assert len(res["out_b"]) == 1


def test_pipeline_topological_sorting_kahn() -> None:
    """Asserts that Kahn's algorithm correctly sorts sequential dependencies."""

    def dummy_fn() -> str:
        return "data"

    node_c = Node("Node_C", dummy_fn, inputs=["dataset_b"], outputs=["dataset_c"])
    node_b = Node("Node_B", dummy_fn, inputs=["dataset_a"], outputs=["dataset_b"])
    node_a = Node("Node_A", dummy_fn, inputs=[], outputs=["dataset_a"])

    pipeline = Pipeline(nodes=[node_c, node_a, node_b])
    sorted_steps = pipeline._resolve_topological_order()

    assert sorted_steps[0].name == "Node_A"
    assert sorted_steps[1].name == "Node_B"
    assert sorted_steps[2].name == "Node_C"


def test_pipeline_circular_dependency_raises_error() -> None:
    """Asserts that cyclic dependency loops abort execution defensively."""

    def dummy_fn() -> str:
        return "loop"

    node_a = Node("Node_A", dummy_fn, inputs=["dataset_b"], outputs=["dataset_a"])
    node_b = Node("Node_B", dummy_fn, inputs=["dataset_a"], outputs=["dataset_b"])

    pipeline = Pipeline(nodes=[node_a, node_b])
    with pytest.raises(CircularDependencyError):
        pipeline._resolve_topological_order()


def test_runner_matricial_filtration_matrix() -> None:
    """Asserts that PipelineRunner cascades combined tag intersection rules."""
    PipelineRegistry.clear()

    def dummy_fn() -> str:
        return "filtered"

    node_1 = Node("N1", dummy_fn, [], ["out1"], tags=["daily", "core"])
    node_2 = Node("N2", dummy_fn, [], ["out2"], tags=["hourly", "core"])
    node_3 = Node("N3", dummy_fn, [], ["out3"], tags=["daily", "satellite"])

    pipe_finance = Pipeline(nodes=[node_1, node_2])
    pipe_hr = Pipeline(nodes=[node_3])

    PipelineRegistry.register_pipeline("finance", pipe_finance)
    PipelineRegistry.register_pipeline("hr", pipe_hr)

    runner = PipelineRunner(catalog_context=None)

    # Patch the Pipeline class inside the module directly to avoid binding anomalies
    with patch("flint_core.core.pipeline.Pipeline") as mock_pipeline_class:
        runner.run_with_filters(pipelines=["finance"], tags=["daily"])
        mock_pipeline_class.assert_called_once()

        # Safely extract nodes passed to the transient Pipeline initialization
        kwargs = mock_pipeline_class.call_args[1]
        filtered_nodes = kwargs["nodes"]

        assert len(filtered_nodes) == 1
        assert filtered_nodes[0].name == "N1"

    # Assert that a non-matching filtration matrix raises PipelineExecutionError
    with pytest.raises(PipelineExecutionError) as exc_info:
        runner.run_with_filters(pipelines=["hr"], tags=["hourly"])
    assert "No execution nodes matched" in str(exc_info.value)

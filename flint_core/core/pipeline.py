"""Deterministic data pipeline and execution node orchestration layer."""

from __future__ import annotations

import inspect
from typing import Any, Callable, ClassVar, Dict, List, Optional

from flint_core.core.exceptions import (
    CircularDependencyError,
    PipelineExecutionError,
)
from flint_core.core.io import DataLoader, DataSaver


class Node:
    """Stateless execution unit anchoring functional transforms to catalog assets."""

    __slots__ = ("name", "func", "inputs", "outputs", "tags")

    def __init__(
        self,
        name: str,
        func: Callable[..., Any],
        inputs: List[str],
        outputs: List[str],
        tags: Optional[List[str]] = None,
    ) -> None:
        """Initializes an autonomous processing Node bound to multiple outputs.

        Args:
            name: Explicit unique string identifier for the execution node.
            func: Executable Python transformation callable logic target.
            inputs: List of declarative catalog dataset lookup keys as inputs.
            outputs: List of destination catalog token keys to persist results.
            tags: Optional operational categorization metadata attributes.
        """
        if not name or not isinstance(name, str):
            raise ValueError("Node 'name' must be a non-empty string.")
        if not callable(func):
            raise TypeError(f"Node '{name}' execution target must be callable.")
        if not outputs:
            raise ValueError(f"Node '{name}' must declare at least one output.")

        self.name: str = name
        self.func: Callable[..., Any] = func
        self.inputs: List[str] = inputs
        self.outputs: List[str] = outputs
        self.tags: List[str] = tags if tags is not None else []

    def _unpack_and_persist(self, raw_output: Any, saver: DataSaver, spark: Optional[Any] = None) -> Dict[str, Any]:
        """Maps polymorphic function returns into explicit catalog targets.

        Args:
            raw_output: Unprocessed output object returned by the function.
            saver: Unified persistence engine tracking storage pathways.
            spark: Optional distributed global session cluster context.

        Returns:
            Dict[str, Any]: Map of output configuration names to dataframes.

        Raises:
            PipelineExecutionError: If return values mismatch declared outputs.
        """
        output_map: Dict[str, Any] = {}

        if isinstance(raw_output, dict):
            for out_key in self.outputs:
                if out_key not in raw_output:
                    raise PipelineExecutionError(
                        f"Node '{self.name}' expected dict key '{out_key}' inside functional return payload."
                    )
                output_map[out_key] = raw_output[out_key]
        elif isinstance(raw_output, (tuple, list)):
            if len(raw_output) != len(self.outputs):
                raise PipelineExecutionError(
                    f"Node '{self.name}' mismatch: expected {len(self.outputs)} "
                    f"positional outputs, got {len(raw_output)} items."
                )
            for idx, out_key in enumerate(self.outputs):
                output_map[out_key] = raw_output[idx]
        else:
            if len(self.outputs) == 1:
                output_map[self.outputs[0]] = raw_output
            else:
                raise PipelineExecutionError(
                    f"Node '{self.name}' returned a single asset but requires "
                    f"multiple positional outputs: {self.outputs}"
                )

        for out_name, df in output_map.items():
            saver.save(df, out_name, mode="overwrite", spark=spark)

        return output_map

    def run(self, spark: Optional[Any] = None, catalog: Optional[Any] = None) -> Dict[str, Any]:
        """Executes this single node in isolation pulling directly from storage.

        Args:
            spark: Optional active PySpark session manager context.
            catalog: Optional central DataCatalog tracking pipeline contexts.

        Returns:
            Dict[str, Any]: Map of output catalog keys to computed dataframes.

        Raises:
            PipelineExecutionError: If the internal callable logic crashes.
        """
        loader = DataLoader(catalog=catalog)
        saver = DataSaver(catalog=catalog)

        node_inputs: Dict[str, Any] = {}
        for input_key in self.inputs:
            node_inputs[input_key] = loader.load(input_key, spark=spark)

        sig = inspect.signature(self.func)
        final_args = {p_name: node_inputs[p_name] for p_name in sig.parameters if p_name in node_inputs}

        try:
            raw_output = self.func(**final_args)
        except Exception as runtime_error:
            raise PipelineExecutionError(
                f"Fatal compute crash inside node '{self.name}': {runtime_error}"
            ) from runtime_error

        return self._unpack_and_persist(raw_output, saver, spark=spark)


class Pipeline:
    """Orchestrates multi-node dependency sequences using topological ordering."""

    __slots__ = ("nodes", "_catalog_context")

    def __init__(self, nodes: List[Node], catalog_context: Optional[Any] = None) -> None:
        """Initializes a Pipeline validating node graph acyclic properties.

        Args:
            nodes: List of processing node blocks to evaluate in the graph.
            catalog_context: Optional operational catalog reference container.
        """
        self.nodes: List[Node] = nodes
        self._catalog_context: Optional[Any] = catalog_context

    def _resolve_topological_order(self) -> List[Node]:
        """Executes Kahn's algorithm supporting multi-output producer splits.

        Returns:
            List[Node]: Stably sorted sequential path matrix of execution nodes.

        Raises:
            CircularDependencyError: If cyclic references form deadlocks.
        """
        in_degree: Dict[Node, int] = {node: 0 for node in self.nodes}
        dataset_producers: Dict[str, List[Node]] = {}

        for node in self.nodes:
            for out_ds in node.outputs:
                if out_ds not in dataset_producers:
                    dataset_producers[out_ds] = []
                dataset_producers[out_ds].append(node)

        for node in self.nodes:
            for input_ds in node.inputs:
                if input_ds in dataset_producers:
                    in_degree[node] += len(dataset_producers[input_ds])

        queue = [node for node, deg in in_degree.items() if deg == 0]
        sorted_nodes: List[Node] = []

        while queue:
            current = queue.pop(0)
            sorted_nodes.append(current)

            for out_ds in current.outputs:
                for node in self.nodes:
                    if out_ds in node.inputs:
                        in_degree[node] -= 1
                        if in_degree[node] == 0:
                            queue.append(node)

        if len(sorted_nodes) != len(self.nodes):
            raise CircularDependencyError("A circular dependency loop was intercepted during DAG analysis.")
        return sorted_nodes

    def run(self, spark: Optional[Any] = None) -> None:
        """Executes all sorted nodes sequentially through direct storage channels.

        Args:
            spark: Optional active cluster execution session manager reference.
        """
        sorted_steps = self._resolve_topological_order()
        for node in sorted_steps:
            node.run(spark=spark, catalog=self._catalog_context)


class PipelineRegistry:
    """Thread-safe centralized registry for compiled pipeline discovery."""

    _pipelines: ClassVar[Dict[str, Pipeline]] = {}

    @classmethod
    def register_pipeline(cls, name: str, pipeline: Pipeline) -> None:
        """Registers a compiled pipeline instance into the global workspace.

        Args:
            name: Structural name tag descriptor to cache the pipeline under.
            pipeline: Active Pipeline instance holding computational steps.
        """
        cls._pipelines[name.lower().strip()] = pipeline

    @classmethod
    def get_all_pipelines(cls) -> Dict[str, Pipeline]:
        """Returns the complete map of registered pipelines.

        Returns:
            Dict[str, Pipeline]: Thread-safe lookup catalog container.
        """
        return cls._pipelines

    @classmethod
    def clear(cls) -> None:
        """Flushes the active pipeline tracking storage mappings map."""
        cls._pipelines.clear()


class PipelineRunner:
    """Handles multi-dimensional filtration matrix execution pipelines."""

    __slots__ = ("_catalog_context",)

    def __init__(self, catalog_context: Optional[Any] = None) -> None:
        """Initializes the runner attached to an operational catalog context.

        Args:
            catalog_context: Optional orchestration catalog tracking container.
        """
        self._catalog_context: Optional[Any] = catalog_context

    def run_with_filters(
        self,
        pipelines: Optional[List[str]] = None,
        nodes: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        spark: Optional[Any] = None,
    ) -> None:
        """Filters all workspace nodes and executes them in topological order.

        Args:
            pipelines: Target list of pipeline scopes to isolate.
            nodes: Specific explicit node identifier names to filter.
            tags: Intersecting operational metadata tags targeting blocks.
            spark: Active distributed execution cluster session context.

        Raises:
            PipelineExecutionError: If no nodes match the criteria filters.
        """
        all_registered = PipelineRegistry.get_all_pipelines()
        execution_pool: List[Node] = []

        if pipelines:
            cleaned_pipes = [p.strip().lower() for p in pipelines if p.strip()]
            for p_name in cleaned_pipes:
                if p_name in all_registered:
                    execution_pool.extend(all_registered[p_name].nodes)
        else:
            for pipe in all_registered.values():
                execution_pool.extend(pipe.nodes)

        if nodes:
            cleaned_nodes = {n.strip() for n in nodes if n.strip()}
            execution_pool = [node for node in execution_pool if node.name in cleaned_nodes]

        if tags:
            filter_tags = {t.strip().lower() for t in tags if t.strip()}
            execution_pool = [
                node for node in execution_pool if filter_tags.intersection({tag.lower() for tag in node.tags})
            ]

        if not execution_pool:
            raise PipelineExecutionError("No execution nodes matched the specified criteria matrix filters.")

        transient_pipeline = Pipeline(nodes=execution_pool, catalog_context=self._catalog_context)
        transient_pipeline.run(spark=spark)

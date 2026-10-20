"""Compile the complete graph before accessing dataset contents."""

import heapq
import inspect
from dataclasses import dataclass
from functools import partial
from typing import Tuple

from flint_core.core.catalog.v1 import CatalogSnapshot
from flint_core.core.catalog.v1.models import DeltaTableTarget, FileTarget
from flint_core.core.exceptions import CatalogParseError
from flint_core.runtime.errors import PlanError
from flint_core.runtime.models import Node, Output, WritePolicy


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ExecutionPlan:
    project_id: str
    snapshot: CatalogSnapshot
    pipeline: str
    engine: str
    nodes: Tuple[Node, ...]
    external_inputs: Tuple[str, ...]
    publications: Tuple[str, ...]

    def to_dict(self):
        return {
            "pipeline": self.pipeline,
            "engine": self.engine,
            "environment": self.snapshot.environment,
            "nodes": [node.name for node in self.nodes],
            "external_inputs": list(self.external_inputs),
            "publications": list(self.publications),
        }


def compile_plan(project, selection=None) -> ExecutionPlan:
    name = selection if selection is not None else project._default
    if not isinstance(name, str) or not name or name not in project.pipelines:
        raise PlanError(
            "Select exactly one registered pipeline, or configure default_pipeline.", code="pipeline_selection"
        )
    pipeline = project.pipelines[name]
    if pipeline.engine not in ("pandas", "spark"):
        raise PlanError("Pipeline engine must be explicitly pandas or spark.")
    if not pipeline.nodes:
        raise PlanError("A pipeline must contain at least one node.")
    snapshot = project.catalog.snapshot
    producers = {}
    node_names = set()
    identities = set()
    destinations = set()
    for index, node in enumerate(pipeline.nodes):
        if not isinstance(node, Node) or not isinstance(node.name, str) or not node.name.strip():
            raise PlanError("Invalid node declaration.")
        if id(node) in identities or node.name in node_names:
            raise PlanError("Repeated node object or node name.")
        identities.add(id(node))
        node_names.add(node.name)
        if not isinstance(node.outputs, Output) or not isinstance(node.outputs.dataset, str):
            raise PlanError("Each node must declare exactly one Output.")
        output = node.outputs.dataset
        if output not in snapshot.definitions:
            raise PlanError(
                "Output requires a declared logical dataset.", code="unknown_output", node=node.name, dataset=output
            )
        if output in producers:
            raise PlanError(
                "Each dataset can have only one producer.", code="duplicate_producer", node=node.name, dataset=output
            )
        producers[output] = index
        if not callable(node.func):
            raise PlanError("Node transformation must be callable.")
        transformation = node.func
        # Classify partials of callable instances as well as ordinary functions.
        while isinstance(transformation, partial):
            transformation = transformation.func
        if not inspect.isroutine(transformation):
            transformation = transformation.__call__
        if any(
            check(transformation)
            for check in (inspect.iscoroutinefunction, inspect.isasyncgenfunction, inspect.isgeneratorfunction)
        ):
            raise PlanError(
                "Transformations must be synchronous functions returning one DataFrame.",
                code="unsupported_callable",
                node=node.name,
            )
        for parameter, dataset in node.inputs.items():
            if not isinstance(parameter, str) or not isinstance(dataset, str) or dataset not in snapshot.definitions:
                raise PlanError("Input mappings must reference declared logical datasets.")
        if set(node.resources) & set(node.inputs) or any(
            not isinstance(parameter, str) or resource != "spark" or pipeline.engine != "spark"
            for parameter, resource in node.resources.items()
        ):
            raise PlanError("Resources must map distinct parameters to the pipeline's explicit Spark session.")
        try:
            signature = inspect.signature(node.func)
            if any(
                parameter.kind in (parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
                for parameter in signature.parameters.values()
            ):
                raise PlanError("Positional-only and variadic signatures are not supported.")
            signature.bind(**dict.fromkeys((*node.inputs, *node.resources), object()))
        except (TypeError, ValueError):
            raise PlanError(
                "Node inputs do not bind the transformation signature.", code="signature_mismatch", node=node.name
            ) from None
        policy = node.outputs.write
        if policy is not None:
            if not isinstance(policy, WritePolicy) or policy.mode != "create" or policy.atomicity != "required":
                raise PlanError("Only explicit create with required atomicity is supported.")
            try:
                binding = snapshot.require_binding(output, access="write")
            except CatalogParseError:
                raise PlanError("A persisted output requires a writable binding.") from None
            _binding_capability(binding, pipeline.engine, write=True)
            if any(
                binding.target.path.is_relative_to(path) or path.is_relative_to(binding.target.path)
                for path in destinations
            ):
                raise PlanError("Publication targets cannot coincide or overlap.")
            destinations.add(binding.target.path)
    dependencies = []
    consumers = [set() for node in pipeline.nodes]
    external = []
    for index, node in enumerate(pipeline.nodes):
        parents = {producers[dataset] for dataset in node.inputs.values() if dataset in producers}
        dependencies.append(len(parents))
        for parent in parents:
            consumers[parent].add(index)
        for dataset in node.inputs.values():
            if dataset not in producers and dataset not in external:
                try:
                    binding = snapshot.require_binding(dataset)
                except CatalogParseError:
                    raise PlanError("An external input requires a readable binding.") from None
                _binding_capability(binding, pipeline.engine)
                if any(
                    binding.target.path.is_relative_to(path) or path.is_relative_to(binding.target.path)
                    for path in destinations
                ):
                    raise PlanError("External input and publication targets cannot overlap.")
                external.append(dataset)
    ready = [index for index, degree in enumerate(dependencies) if degree == 0]
    heapq.heapify(ready)
    ordered = []
    while ready:
        index = heapq.heappop(ready)
        ordered.append(pipeline.nodes[index])
        for consumer in sorted(consumers[index]):
            dependencies[consumer] -= 1
            if dependencies[consumer] == 0:
                heapq.heappush(ready, consumer)
    if len(ordered) != len(pipeline.nodes):
        raise PlanError("Pipeline graph contains a cycle.", code="graph_cycle")
    return ExecutionPlan(
        project._namespace,
        snapshot,
        name,
        pipeline.engine,
        tuple(ordered),
        tuple(external),
        tuple(node.outputs.dataset for node in ordered if node.outputs.write is not None),
    )


def _binding_capability(binding, engine, *, write=False):
    supported = {
        "pandas.csv": ("pandas", "csv", FileTarget),
        "pandas.delta": ("pandas", "delta", DeltaTableTarget),
        "spark.delta": ("spark", "delta", DeltaTableTarget),
    }
    capability = supported.get(binding.adapter)
    if (
        capability is None
        or capability[:2] != (engine, binding.format)
        or not isinstance(binding.target, capability[2])
    ):
        raise PlanError("Binding and pipeline engine capabilities do not match; implicit conversions are forbidden.")
    if binding.adapter == "pandas.csv" and binding.options:
        raise PlanError("Unsupported CSV options.")
    if binding.format == "delta":
        if set(binding.options) - {"version"}:
            raise PlanError("Unsupported Delta options.")
        if "version" in binding.options and (
            write
            or type(binding.options["version"]) is not int
            or binding.options["version"] < 0
            or binding.access != "read_only"
        ):
            raise PlanError("Historical Delta versions require a read-only binding.")

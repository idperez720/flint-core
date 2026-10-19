"""Compile the complete graph before accessing dataset contents."""

import heapq
import inspect
from dataclasses import dataclass
from typing import Tuple

from flint_core.core.catalog.v1 import CatalogSnapshot
from flint_core.core.exceptions import CatalogParseError
from flint_core.runtime.errors import PlanError
from flint_core.runtime.models import Node, Output, WritePolicy


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ExecutionPlan:
    project_id: str
    snapshot: CatalogSnapshot
    pipeline: str
    nodes: Tuple[Node, ...]
    external_inputs: Tuple[str, ...]
    publications: Tuple[str, ...]

    def to_dict(self):
        return {
            "pipeline": self.pipeline,
            "environment": self.snapshot.environment,
            "nodes": [node.name for node in self.nodes],
            "external_inputs": list(self.external_inputs),
            "publications": list(self.publications),
        }


def compile_plan(project, selection=None) -> ExecutionPlan:
    name = selection if selection is not None else project._default
    if not isinstance(name, str) or not name or name not in project.pipelines:
        raise PlanError("Select exactly one registered pipeline, or configure default_pipeline.")
    pipeline = project.pipelines[name]
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
        if output not in snapshot.definitions or output in producers:
            raise PlanError("Unknown output definition or duplicate dataset producer.")
        producers[output] = index
        if not callable(node.func):
            raise PlanError("Node transformation must be callable.")
        for parameter, dataset in node.inputs.items():
            if not isinstance(parameter, str) or not isinstance(dataset, str) or dataset not in snapshot.definitions:
                raise PlanError("Input mappings must reference declared logical datasets.")
        try:
            signature = inspect.signature(node.func)
            if any(
                parameter.kind in (parameter.POSITIONAL_ONLY, parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
                for parameter in signature.parameters.values()
            ):
                raise PlanError("Positional-only and variadic signatures are not supported.")
            signature.bind(**dict.fromkeys(node.inputs, object()))
        except (TypeError, ValueError):
            raise PlanError("Node inputs do not bind the transformation signature.") from None
        policy = node.outputs.write
        if policy is not None:
            if not isinstance(policy, WritePolicy) or policy.mode != "create" or policy.atomicity != "required":
                raise PlanError("Only explicit create with required atomicity is supported.")
            try:
                binding = snapshot.require_binding(output, access="write")
            except CatalogParseError:
                raise PlanError("A persisted output requires a writable binding.") from None
            if binding.adapter != "pandas.csv" or binding.format != "csv" or binding.options:
                raise PlanError("Unsupported write capability.")
            if binding.target.path in destinations:
                raise PlanError("Multiple outputs cannot publish to the same physical target.")
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
                if binding.adapter != "pandas.csv" or binding.format != "csv" or binding.options:
                    raise PlanError("Unsupported read capability.")
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
        raise PlanError("Pipeline graph contains a cycle.")
    return ExecutionPlan(
        project._namespace,
        snapshot,
        name,
        tuple(ordered),
        tuple(external),
        tuple(node.outputs.dataset for node in ordered if node.outputs.write is not None),
    )

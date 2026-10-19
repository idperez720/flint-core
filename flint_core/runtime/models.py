"""Immutable declarations for the opt-in sequential runtime."""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional, Sequence

from flint_core.runtime.errors import PlanError


@dataclass(frozen=True, slots=True)
class WritePolicy:
    mode: str = "create"
    atomicity: str = "required"


@dataclass(frozen=True, slots=True)
class Output:
    dataset: str
    write: Optional[WritePolicy] = None


@dataclass(frozen=True, slots=True)
class Node:
    name: str
    func: Callable[..., Any]
    inputs: Mapping[str, str]
    outputs: Output

    def __post_init__(self) -> None:
        if not isinstance(self.inputs, Mapping):
            raise PlanError("Node inputs must be an explicit parameter-to-dataset mapping.")
        object.__setattr__(self, "inputs", MappingProxyType(dict(self.inputs)))


@dataclass(frozen=True, slots=True)
class Pipeline:
    name: str
    nodes: Sequence[Node]

    def __post_init__(self) -> None:
        if not isinstance(self.nodes, (list, tuple)):
            raise PlanError("Pipeline nodes must be an ordered list or tuple.")
        object.__setattr__(self, "nodes", tuple(self.nodes))

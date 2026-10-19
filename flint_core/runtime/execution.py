"""Sequential run-scoped execution with truthful publication outcomes."""

from collections import Counter
from dataclasses import dataclass
from typing import Optional, Tuple
from uuid import uuid4

from flint_core.runtime.errors import DataValidationError, PublicationUncertain
from flint_core.runtime.local_csv import LocalCsvAdapter


@dataclass(frozen=True, slots=True)
class Publication:
    dataset: str
    state: str
    cleanup_pending: bool = False


@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: str
    pipeline: str
    environment: str
    status: str
    completed_nodes: Tuple[str, ...]
    publications: Tuple[Publication, ...]
    failed_node: Optional[str] = None
    error_code: Optional[str] = None
    cleanup_pending: bool = False

    @property
    def success(self):
        return self.status == "success"

    def to_dict(self):
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "environment": self.environment,
            "status": self.status,
            "completed_nodes": list(self.completed_nodes),
            "publications": [
                {"dataset": item.dataset, "state": item.state, "cleanup_pending": item.cleanup_pending}
                for item in self.publications
            ],
            "failed_node": self.failed_node,
            "error_code": self.error_code,
            "cleanup_pending": self.cleanup_pending,
        }


def execute(plan):
    adapter = LocalCsvAdapter()
    snapshot = plan.snapshot
    completed = []
    publications = []
    run_id = uuid4().hex
    node = None
    phase = "preflight"
    cache = {}
    uses = Counter(dataset for step in plan.nodes for dataset in step.inputs.values())
    try:
        for dataset in plan.external_inputs:
            adapter.preflight(snapshot, dataset)
        for dataset in plan.publications:
            adapter.preflight(snapshot, dataset, write=True)
        for node in plan.nodes:
            phase = "read"
            arguments = {}
            for parameter, dataset in node.inputs.items():
                if dataset not in cache:
                    cache[dataset] = adapter.read(snapshot, dataset)
                arguments[parameter] = cache[dataset].copy(deep=True)
            phase = "transformation"
            raw_output = node.func(**arguments)
            phase = "validation"
            output = node.outputs.dataset
            owned = adapter.validate(raw_output, snapshot.definitions[output])
            if node.outputs.write is not None:
                phase = "publication"
                try:
                    receipt = adapter.publish(snapshot, output, owned)
                except PublicationUncertain as error:
                    publications.append(Publication(output, "uncertain", error.cleanup_pending))
                    raise
                publications.append(Publication(output, "committed", receipt.cleanup_pending))
            cache[output] = owned
            completed.append(node.name)
            for dataset in node.inputs.values():
                uses[dataset] -= 1
                if not uses[dataset]:
                    cache.pop(dataset, None)
            if not uses[output]:
                cache.pop(output, None)
        return RunResult(
            run_id,
            plan.pipeline,
            snapshot.environment,
            "success",
            tuple(completed),
            tuple(publications),
            cleanup_pending=any(item.cleanup_pending for item in publications),
        )
    except Exception as error:
        status = "uncertain" if isinstance(error, PublicationUncertain) else "failed"
        code = "publication_uncertain" if status == "uncertain" else phase + "_failed"
        if isinstance(error, DataValidationError):
            code = "data_validation_failed"
        return RunResult(
            run_id,
            plan.pipeline,
            snapshot.environment,
            status,
            tuple(completed),
            tuple(publications),
            node.name if node is not None else None,
            code,
            bool(getattr(error, "cleanup_pending", False)) or any(item.cleanup_pending for item in publications),
        )

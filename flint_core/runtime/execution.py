"""Sequential run-scoped execution with truthful publication outcomes."""

from collections import Counter
from dataclasses import dataclass, replace
from typing import Optional, Tuple
from uuid import uuid4

from flint_core.runtime.errors import DataValidationError, PublicationUncertain
from flint_core.runtime.local_csv import LocalCsvAdapter
from flint_core.runtime.local_delta import PandasDeltaAdapter, SparkDeltaAdapter


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


def execute(plan, *, spark=None, spark_local=False):
    snapshot = plan.snapshot
    completed = []
    publications = []
    run_id = uuid4().hex
    node = None
    phase = "preflight"
    cache = {}
    frames = None
    session = None
    owns_session = False
    cleanup_pending = False
    uses = Counter(dataset for step in plan.nodes for dataset in step.inputs.values())
    try:
        if plan.engine == "spark":
            from flint_core.runtime.spark import SparkFrames, open_session

            session, owns_session = open_session(spark, local=spark_local)
            frames = SparkFrames(session)
            validator = frames
        else:
            from flint_core.runtime.frames import _pandas

            _pandas()
            validator = LocalCsvAdapter()
        adapters = {"pandas.csv": LocalCsvAdapter(), "pandas.delta": PandasDeltaAdapter()}
        if frames is not None:
            adapters["spark.delta"] = SparkDeltaAdapter(frames)
        for dataset in plan.external_inputs:
            adapters[snapshot.bindings[dataset].adapter].preflight(snapshot, dataset)
        for dataset in plan.publications:
            adapters[snapshot.bindings[dataset].adapter].preflight(snapshot, dataset, write=True)
        for node in plan.nodes:
            phase = "read"
            arguments = {}
            for parameter, dataset in node.inputs.items():
                if dataset not in cache:
                    cache[dataset] = adapters[snapshot.bindings[dataset].adapter].read(snapshot, dataset)
                arguments[parameter] = (
                    frames.copy(cache[dataset]) if frames is not None else cache[dataset].copy(deep=True)
                )
            for parameter in node.resources:
                arguments[parameter] = session
            phase = "transformation"
            raw_output = node.func(**arguments)
            phase = "validation"
            output = node.outputs.dataset
            owned = validator.validate(raw_output, snapshot.definitions[output])
            if node.outputs.write is not None:
                phase = "publication"
                try:
                    receipt = adapters[snapshot.bindings[output].adapter].publish(snapshot, output, owned)
                except PublicationUncertain as error:
                    publications.append(Publication(output, "uncertain", error.cleanup_pending))
                    raise
                publications.append(Publication(output, "committed", receipt.cleanup_pending))
            cache[output] = owned
            completed.append(node.name)
            for dataset in node.inputs.values():
                uses[dataset] -= 1
                if not uses[dataset]:
                    released = cache.pop(dataset, None)
                    if frames is not None and released is not None:
                        try:
                            frames.release(released)
                        except Exception:
                            cleanup_pending = True
            if not uses[output]:
                cache.pop(output, None)
                if frames is not None:
                    try:
                        frames.release(owned)
                    except Exception:
                        cleanup_pending = True
        result = RunResult(
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
        result = RunResult(
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
    finally:
        if frames is not None:
            cleanup_pending = frames.close() or cleanup_pending
        if owns_session:
            try:
                session.stop()
            except Exception:
                cleanup_pending = True
    return replace(result, cleanup_pending=result.cleanup_pending or cleanup_pending)

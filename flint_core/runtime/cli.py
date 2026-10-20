"""Versioned project commands sharing the Python facade."""

import json
import os
from contextlib import contextmanager, redirect_stderr, redirect_stdout

import click

from flint_core import FlintProject
from flint_core.runtime.errors import PlanError, ProjectError

_PLAN_HINTS = {
    "pipeline_selection": "Select a registered --pipeline or configure default_pipeline in pyproject.toml.",
    "unknown_output": "Declare each node output in the selected dataset definitions.",
    "duplicate_producer": "Give each logical dataset exactly one producer in the pipeline.",
    "signature_mismatch": "Match node input and resource parameter names to the transformation signature.",
    "unsupported_callable": "Use synchronous transformations returning one DataFrame; avoid iterators and coroutines.",
    "graph_cycle": "Remove circular dataset dependencies, including nodes reading their own output.",
}
_HINTS = {
    "project_configuration_failed": "Check pyproject.toml, --env, catalog manifests and the pipeline factory.",
    "plan_failed": "Check node declarations, signatures, bindings, engine compatibility and graph dependencies.",
    "configuration_or_plan_failed": "Check project configuration and trusted factory code using the Python API.",
    "preflight_failed": "Check engine extras, sources and absent output targets with existing parents.",
    "read_failed": "Check input bindings, local files and the declared dataset schema.",
    "transformation_failed": "Test the failed node's transformation directly with representative inputs.",
    "validation_failed": "Return one DataFrame matching the output dataset contract.",
    "data_validation_failed": "Check engine setup, column types, nullability, uniqueness and local dataset paths.",
    "publication_failed": "Inspect the output target and cleanup flag; create never replaces an existing dataset.",
    "publication_uncertain": "Inspect the target and staging artifacts manually; do not retry automatically.",
}


def _diagnostic(summary):
    code = summary.get("error_code")
    hint = _PLAN_HINTS.get(summary.get("plan_error")) or _HINTS.get(code)
    if hint:
        summary["hint"] = hint
    return summary


def _perform(path, environment, pipeline, dry_run, metadata_only, spark_local=False):
    with FlintProject.load(path, environment=environment) as project:
        if metadata_only:
            snapshot = project.catalog.snapshot
            return {
                "status": "valid",
                "environment": snapshot.environment,
                "definitions": len(snapshot.definitions),
                "bindings": len(snapshot.bindings),
            }, 0
        if dry_run:
            return {"status": "planned", **project.compile(pipeline).to_dict()}, 0
        result = project.run(pipeline, spark_local=spark_local)
        return result.to_dict(), 0 if result.success else 3 if result.status == "uncertain" else 2


@contextmanager
def _quiet_native(sink):
    saved = []
    try:
        for descriptor in (1, 2):
            saved.append((descriptor, os.dup(descriptor)))
            os.dup2(sink.fileno(), descriptor)
        yield
    finally:
        for descriptor, duplicate in reversed(saved):
            os.dup2(duplicate, descriptor)
            os.close(duplicate)


def _command(path, environment, pipeline, dry_run, metadata_only, json_output, spark_local=False):
    try:
        if json_output:
            with open(os.devnull, "w") as sink, redirect_stdout(sink), redirect_stderr(sink), _quiet_native(sink):
                summary, code = _perform(path, environment, pipeline, dry_run, metadata_only, spark_local)
        else:
            summary, code = _perform(path, environment, pipeline, dry_run, metadata_only, spark_local)
    except PlanError as error:
        summary, code = (
            {
                "status": "failed",
                "error_code": "plan_failed",
                "plan_error": error.code
                if isinstance(error.code, str) and error.code in _PLAN_HINTS
                else "invalid_plan",
            },
            2,
        )
    except ProjectError:
        summary, code = {"status": "failed", "error_code": "project_configuration_failed"}, 2
    except Exception:
        summary, code = {"status": "failed", "error_code": "configuration_or_plan_failed"}, 2
    _diagnostic(summary)
    if json_output:
        click.echo(json.dumps(summary))
    elif code:
        click.echo(summary["status"] + ": " + summary["error_code"] + ". " + summary.get("hint", ""))
    else:
        click.echo(
            summary["status"] + ": " + json.dumps({key: value for key, value in summary.items() if key != "status"})
        )
    if code:
        raise click.exceptions.Exit(code)


@click.command("validate")
@click.option("--project", "path", default=".", type=click.Path(file_okay=False))
@click.option("--env", "environment", default="dev")
@click.option("--json", "json_output", is_flag=True)
def validate(path, environment, json_output):
    """Validate metadata without importing project pipelines or accessing datasets."""
    _command(path, environment, None, False, True, json_output)


@click.command("run")
@click.option("--project", "path", default=".", type=click.Path(file_okay=False))
@click.option("--env", "environment", default="dev")
@click.option("--pipeline", default=None)
@click.option("--dry-run", is_flag=True)
@click.option("--spark-local", is_flag=True, help="Explicitly create and close a local Delta-enabled Spark session.")
@click.option("--json", "json_output", is_flag=True)
def run(path, environment, pipeline, dry_run, spark_local, json_output):
    """Compile or execute one explicit/default pipeline using the Python facade."""
    _command(path, environment, pipeline, dry_run, False, json_output, spark_local)

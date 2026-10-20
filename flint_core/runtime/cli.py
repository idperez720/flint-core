"""Versioned project commands sharing the Python facade."""

import json
import os
from contextlib import contextmanager, redirect_stderr, redirect_stdout

import click

from flint_core import FlintProject


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
    except Exception:
        summary, code = {"status": "failed", "error_code": "configuration_or_plan_failed"}, 2
    click.echo(json.dumps(summary) if json_output else summary["status"] + ": " + summary.get("error_code", "ok"))
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

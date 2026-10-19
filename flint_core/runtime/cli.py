"""Versioned project commands sharing the Python facade."""

import json
import os
from contextlib import redirect_stderr, redirect_stdout

import click

from flint_core import FlintProject


def _perform(path, environment, pipeline, dry_run, metadata_only):
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
        result = project.run(pipeline)
        return result.to_dict(), 0 if result.success else 3 if result.status == "uncertain" else 2


def _command(path, environment, pipeline, dry_run, metadata_only, json_output):
    try:
        if json_output:
            with open(os.devnull, "w") as sink, redirect_stdout(sink), redirect_stderr(sink):
                summary, code = _perform(path, environment, pipeline, dry_run, metadata_only)
        else:
            summary, code = _perform(path, environment, pipeline, dry_run, metadata_only)
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
@click.option("--json", "json_output", is_flag=True)
def run(path, environment, pipeline, dry_run, json_output):
    """Compile or execute one explicit/default pipeline using the Python facade."""
    _command(path, environment, pipeline, dry_run, False, json_output)

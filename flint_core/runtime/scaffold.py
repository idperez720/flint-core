"""Generate the minimum executable v1 project without installing dependencies."""

import json
import re
from pathlib import Path

import yaml

from flint_core.core.initializer import ScaffoldContext
from flint_core.runtime.errors import ProjectError

_PIPELINES = """from flint_core import Node, Output, Pipeline, WritePolicy


def clean_customers(customers):
    return customers.assign(name=customers["name"].str.strip())


def build_pipelines():
    return [
        Pipeline(
            name="customers",
            nodes=[
                Node(
                    name="clean_customers",
                    func=clean_customers,
                    inputs={"customers": "raw.customers"},
                    outputs=Output("curated.customers", WritePolicy()),
                )
            ],
        )
    ]
"""


def _definition():
    return {
        "schema": {
            "extra_columns": "forbid",
            "columns": {
                "id": {"type": "int64", "nullable": False},
                "name": {"type": "string", "nullable": True},
                "active": {"type": "boolean", "nullable": False},
                "score": {"type": "float64", "nullable": True},
            },
        },
        "constraints": [{"id": "unique_id", "type": "unique", "columns": ["id"], "severity": "error"}],
    }


def create_project(path, *, name=None, version="0.1.0", description="A minimal Flint project", author="Anonymous"):
    from flint_core import __version__

    requested = Path(path)
    if requested.is_symlink():
        raise ProjectError("Scaffold root cannot be a symlink.")
    root = requested.resolve()
    name = name if name is not None else root.name
    if not isinstance(name, str) or re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", name) is None:
        raise ProjectError("Project name must contain letters, numbers, underscores or hyphens.")
    if not isinstance(version, str) or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version) is None:
        raise ProjectError("Project version must contain three numeric components.")
    if not isinstance(description, str) or not isinstance(author, str):
        raise ProjectError("Project description and author must be strings.")
    context = ScaffoldContext(root, name, version, description, author)
    manifest = f"""[project]
name = {json.dumps(name)}
version = {json.dumps(version)}
description = {json.dumps(description)}
authors = [{{name = {json.dumps(author)}}}]
requires-python = ">=3.11,<4.0"
dependencies = ["flint-core[pandas]=={__version__}"]

[tool.flint]
schema_version = 1
project_name = {json.dumps(name)}
pipeline_factory = "project_code.pipelines:build_pipelines"
definitions = ["catalog/definitions.yaml"]
default_pipeline = "customers"

[tool.flint.environments.dev]
bindings = ["conf/bindings.dev.yaml"]
"""
    definitions = {
        "schema_version": 1,
        "kind": "DatasetDefinitions",
        "datasets": {"raw.customers": _definition(), "curated.customers": _definition()},
    }
    bindings = {"schema_version": 1, "kind": "DatasetBindings", "environment": "dev", "bindings": {}}
    for dataset, filename, access in (
        ("raw.customers", "source.csv", "read_only"),
        ("curated.customers", "customers.csv", "read_write"),
    ):
        bindings["bindings"][dataset] = {
            "adapter": "pandas.csv",
            "format": "csv",
            "target": {"kind": "file", "path": "data/" + filename},
            "access": access,
            "options": {},
        }
    readme = f"""# {name}

This is the minimum local CSV workflow, not a scheduler or Utilities package.
Python 3.11+ and a POSIX filesystem with hard links and directory fsync are required.
Generation does not install anything or change an active environment.

## Install

Create an environment with `python -m venv .venv`. Install the tested wheel from
this branch, with the Pandas extra:

```sh
.venv/bin/python -m pip install "/absolute/path/to/flint_core-{__version__}-py3-none-any.whl[pandas]"
```

These changes are unreleased. Do not substitute the previously published package
with the same version for the tested branch wheel. After release, use the new
release version instead. Installing the project itself is not required.

## Validate and Run

```sh
.venv/bin/flint validate --project . --env dev --json
.venv/bin/flint run --project . --env dev --dry-run --json
.venv/bin/flint run --project . --env dev --json
```

The first run creates `data/customers.csv`. The second run fails and preserves it.
There is no implicit overwrite or global rollback. Inspect the structured result
before taking any recovery action.

## Python

```python
from flint_core import FlintProject

with FlintProject.load(".", environment="dev") as project:
    result = project.run()
    print(result.to_dict())
```

Transformations are ordinary functions in `src/project_code/pipelines.py`.
Use relative imports for other project modules. Catalog definitions and physical
bindings are separate. The source CSV uses explicit typed values; `\\N` is the
null marker and leading backslashes in non-null strings must be escaped.
"""
    files = {
        "pyproject.toml": manifest,
        "README.md": readme,
        "src/project_code/__init__.py": "",
        "src/project_code/pipelines.py": _PIPELINES,
        "catalog/definitions.yaml": yaml.safe_dump(definitions, sort_keys=False),
        "conf/bindings.dev.yaml": yaml.safe_dump(bindings, sort_keys=False),
        "data/source.csv": "id,name,active,score\n1, Alice ,true,1.5\n2,Bob,false,2.0\n",
    }
    try:
        for relative in files:
            destination = context.check_path(root / relative)
            if destination.exists():
                raise FileExistsError("A scaffold target already exists.")
        for relative, contents in files.items():
            destination = root / relative
            context.make_directory(destination.parent)
            context.write_text(destination, contents)
    except Exception:
        context.rollback_created(final=True)
        raise ProjectError(
            "Versioned scaffold generation failed; existing or changed resources were retained."
        ) from None
    return root

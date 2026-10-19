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

_SPARK_PIPELINES = """from flint_core import Node, Output, Pipeline, WritePolicy


def source_customers(session):
    return session.createDataFrame(
        [(1, " Alice ", True, 1.5), (2, "Bob", False, 2.0)],
        "id long, name string, active boolean, score double",
    )


def clean_customers(customers):
    from pyspark.sql import functions

    return customers.withColumn("name", functions.trim(functions.col("name")))


def build_pipelines():
    return [Pipeline("customers", [
        Node("source_customers", source_customers, {}, Output("raw.customers"), resources={"session": "spark"}),
        Node("clean_customers", clean_customers, {"customers": "raw.customers"},
             Output("curated.customers", WritePolicy())),
    ], engine="spark")]
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


def create_project(
    path, *, name=None, version="0.1.0", description="A minimal Flint project", author="Anonymous", engine="pandas"
):
    from flint_core import __version__

    if engine not in ("pandas", "spark"):
        raise ProjectError("The v1 scaffold engine must be pandas or spark.")
    extra = "spark-delta" if engine == "spark" else "pandas"
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
dependencies = ["flint-core[{extra}]=={__version__}"]

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
    if engine == "spark":
        bindings["bindings"] = {
            "curated.customers": {
                "adapter": "spark.delta",
                "format": "delta",
                "target": {"kind": "delta_table", "path": "data/customers"},
                "access": "read_write",
                "options": {},
            }
        }
        files.pop("data/source.csv")
        files["data/.gitkeep"] = ""
        files["src/project_code/pipelines.py"] = _SPARK_PIPELINES
        files["conf/bindings.dev.yaml"] = yaml.safe_dump(bindings, sort_keys=False)
        files["README.md"] = f"""# {name}

This Draft v1 project executes Spark DataFrames and atomically creates a local Delta table.
It adds no scheduler or Utilities. Generation installs nothing and starts no session.

## Install

Use Python 3.11+ and a working Java 17 JDK. Set JAVA_HOME and PYSPARK_PYTHON for
your environment. Use a trusted local macOS/Linux filesystem with exclusive
directory rename and directory fsync. Remote publication is not supported.

```sh
python -m venv .venv
.venv/bin/python -m pip install "/absolute/path/to/flint_core-{__version__}-py3-none-any.whl[spark-delta]"
```

These changes are unreleased; use the branch wheel, not the old published package
with the same version. Select a new release version before publication.

## Validate and Run

```sh
.venv/bin/flint validate --project . --env dev --json
.venv/bin/flint run --project . --env dev --dry-run --json
.venv/bin/flint run --project . --env dev --spark-local --json
```

Only the final command explicitly creates a local session and resolves Delta JARs.
The first run creates data/customers. A second create fails without changing it.
No overwrite, append, retry, or multi-output rollback is implicit. Inspect status,
publications and cleanup_pending before recovery. Unknown publication outcomes
require manual review; never remove the create policy or switch to overwrite.

## Python

```python
from flint_core import FlintProject

with FlintProject.load(".", environment="dev") as project:
    result = project.run(spark_local=True)
    print(result.to_dict())
```

Alternatively pass an existing configured classic Spark session using
project.run(spark=existing_session). Flint never stops that borrowed session or
adopts an unrelated global session. Node resources map explicit function arguments
to that session; DataFrames are not converted to Pandas or collected to the driver.

Functions are in src/project_code/pipelines.py. The source node produces an
unbound in-memory dataset; only the explicitly persisted output gets a binding.
Schema, nullability, finiteness, and uniqueness checks apply to the full materialized
batch. Run-owned local checkpoints are released, never caller-owned caches.
Executor/checkpoint loss fails the run rather than rereading a newer snapshot.
This is not crash recovery, distributed scheduling, or cloud/Databricks certification.
"""
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

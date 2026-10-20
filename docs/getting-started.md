# Getting started

Flint Core 0.7.0 provides a sequential Runtime and strict Catalog v1 for local
Pandas CSV/Delta and Spark Delta. API v1 remains Draft. Python 3.11 or 3.12 and
a trusted local POSIX filesystem are the verified targets.

## Pandas example

Run these commands in a new working directory:

```sh
python -m venv .venv
.venv/bin/python -m pip install "flint-core[pandas]==0.7.0"
.venv/bin/flint init --format v1 --path ./customers-project --no-input
.venv/bin/flint validate --project ./customers-project --json
.venv/bin/flint run --project ./customers-project --dry-run --json
.venv/bin/flint run --project ./customers-project --json
```

Registry installation requires the version to be published. For an unreleased
checkout, build its wheel and install that wheel with the same extra instead.
The generated example includes source data, a transformation, logical dataset
schemas and physical bindings. The first run creates
`customers-project/data/customers.csv`. A second run fails and preserves the file.

Edit `catalog/definitions.yaml` to declare columns, nullability and uniqueness;
edit `conf/bindings.dev.yaml` to bind logical IDs to local paths. Put synchronous
DataFrame transformations in `src/project_code/pipelines.py`. Output directories
must exist; only outputs with an explicit `WritePolicy()` are persisted.

## Spark example

Install `flint-core[spark-delta]==0.7.0` in the environment, set `JAVA_HOME` to a
working Java 17 JDK and `PYSPARK_PYTHON` to that environment's Python executable.
Then generate and execute a separate project:

```sh
.venv/bin/flint init --format v1 --engine spark --path ./spark-customers --no-input
.venv/bin/flint validate --project ./spark-customers --json
.venv/bin/flint run --project ./spark-customers --dry-run --json
.venv/bin/flint run --project ./spark-customers --spark-local --json
```

Execution explicitly starts a local Spark session and resolves Delta JARs;
validation and compilation do neither. The example generates its source in
memory and creates `spark-customers/data/customers` as a Delta table. See the
[Spark guide](spark-delta-v1.md) for borrowed sessions and platform requirements.

## Diagnose failures

CLI summaries include an error category and a safe recovery hint. Exit 2 means
a known failure, exit 3 means publication is uncertain. Inspect `publications`
and `cleanup_pending` before recovery. Earlier committed outputs survive later
failures. Never retry an uncertain publication automatically.

`validate` checks metadata without importing pipeline code. `run --dry-run`
checks the full graph without framework dataset I/O. Neither certifies the data
or connectivity. Use the [Runtime API reference](runtime-api.md) for typed plan
errors, result contracts and external orchestration boundaries.

The [full project workflow](project-workflow.md) documents configuration and CSV
encoding. The separate [legacy initializer guide](legacy-getting-started.md)
remains available for existing users; its global engine and I/O contracts differ.
Flint Core adds no scheduling, cloud bindings, distributed Spark, append or overwrite.

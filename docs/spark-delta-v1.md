# Spark-First Delta Workflow v1

**Draft API — implemented for local classic Spark and Pandas Delta.**

Spark is a first-class v1 execution engine, not a conversion through Pandas.
Runtime and Catalog remain the pillars; Utilities and a scheduler are not added.
The wider Architecture RFC remains Draft. This implementation does not approve
every pending ADR or certify cloud/Databricks execution.

## Generate and Run a Spark Project

Install the published 0.7.0 package with the Spark Delta extra. For an
unreleased checkout, build and install its wheel instead. See the
[release review](release-0.7.0.md) for verification gates.

```sh
python -m venv .venv
.venv/bin/python -m pip install "flint-core[spark-delta]==0.7.0"
.venv/bin/flint init --format v1 --engine spark --path ./customers --no-input
.venv/bin/flint validate --project ./customers --json
.venv/bin/flint run --project ./customers --dry-run --json
.venv/bin/flint run --project ./customers --spark-local --json
```

Java 17 must be available through `JAVA_HOME`; set `PYSPARK_PYTHON` to the selected
environment's Python when necessary. The tested pair is **PySpark 4.1.2 / Delta
Spark 4.4.1**. The extra constrains Spark to 4.1.x and Delta Spark to 4.4.x; the
lockfile records the tested pair. This is not certification of every patch version.

Generation, validation, and dry-run require no Spark installation or JVM and do
not touch dataset contents. Only `--spark-local` explicitly starts a local
two-worker Delta-enabled session and may resolve Maven JARs into the Ivy cache.
Flint closes the session it creates, including after failures.

The generated source is an ordinary Spark function with an explicitly injected
session. Its output stays in memory; the next function cleans it and explicitly
creates `data/customers`. A second run fails without replacing that table.
The default scaffold without `--engine` remains the existing Pandas/CSV example.
Neither scaffold installs dependencies or changes an environment.

## Explicit Python API and Session Ownership

```python
from flint_core import FlintProject, Node, Output, Pipeline, WritePolicy


def source(session):
    return session.range(10)


def transform(records):
    return records.where("id >= 2")


def build_pipelines():
    return [Pipeline("main", [
        Node("source", source, {}, Output("raw.records"), resources={"session": "spark"}),
        Node("transform", transform, {"records": "raw.records"},
             Output("curated.records", WritePolicy(mode="create", atomicity="required"))),
    ], engine="spark")]


with FlintProject.load("/work/project", environment="dev") as project:
    result = project.run(spark=configured_session)
```

The definitions and selected bindings must describe the referenced IDs. This
factory is registered through the existing explicit `pipeline_factory` project
setting, not a global registry. `Pipeline.engine` is `spark` or `pandas` (the
backward-compatible default). The complete pipeline uses one representation;
cross-engine bindings, implicit conversions, and mixed engine outputs are rejected.

`Node.resources` maps distinct function parameters to the resource ID `spark`.
Resources are immutable declarations and participate in full signature validation.
The session is passed explicitly, never discovered by a transformation through
a Flint global context. Resource names cannot overlap input parameter names.

`project.run(spark=session)` borrows a live, configured classic local session.
Flint never stops it or clears caller-owned caches. Standard Delta extensions and
`DeltaCatalog` configuration are checked before transformations. An incompatible
JVM Delta API fails preflight rather than being treated as a valid integration.

`project.run(spark_local=True)` explicitly creates/closes the same local session
used by CLI. Existing Spark contexts are not silently adopted: pass one explicitly.
Supplying both choices fails. Passing Spark options to a Pandas pipeline fails.

Local targets require a `local`/`local[...]` Spark master. Remote executors may
not share the driver's filesystem; they are rejected rather than publishing a
log whose data files exist only on another machine. Shared/remote/cloud storage,
Spark Connect, Databricks catalogs, and orchestrator-owned distributed sessions
need their own future binding/capability contracts.

## Versioned Binding Contract

Logical definitions retain the same four types, nullability, extra-column policy,
and uniqueness rules. Physical details remain in the selected bindings manifest:

```yaml
schema_version: 1
kind: DatasetBindings
environment: dev
bindings:
  raw.records:
    adapter: spark.delta
    format: delta
    target: {kind: delta_table, path: data/source}
    access: read_only
    options: {version: 0}
  curated.records:
    adapter: spark.delta
    format: delta
    target: {kind: delta_table, path: data/result}
    access: read_write
    options: {}
```

The source binding is needed only for an external input, not for an in-memory
producer such as the example above. `version` is an optional non-negative YAML
integer on a read-only binding. Zero is historical, not latest. Missing/invalid
versions fail without fallback. Without a version, the first read selects and
pins a native snapshot for that dataset; no multi-source transaction is promised.

`pandas.delta` uses the same `delta` / `delta_table` contract, with Pandas frames
and the `pandas,delta` extras. That engine may combine `pandas.csv` and
`pandas.delta` bindings. The Delta extra now includes PyArrow. Spark needs neither
Pandas nor PyArrow; an installed-wheel check verifies that environment explicitly.

Unknown options, timestamps, URIs, credentials, partitioning, schema evolution,
and provider-specific write controls are rejected, not ignored. Existing
project-relative path and explicit-variable rules still apply. Bindings never
grant implicit persistence permission.

Delta paths/project roots additionally reject `%`, `?`, and `#` before engine
access. These characters have ambiguous native URI handling; silently decoding
them could redirect I/O. Ordinary spaces are supported and tested in both engines.
Active data references with ambiguous/double-encoded URI components also fail.

## Full Data Validation, Not Sampling

Spark requires exactly `LongType`, `DoubleType`, `BooleanType`, and `StringType`
for `int64`, `float64`, `boolean`, and `string`. IntegerType, DecimalType,
timestamps, and dates are not aliases for this v1 subset. Extra columns, when
allowed, remain strings/missing values, consistently with the Pandas/CSV contract.

Every row is checked for missing required values, non-finite floats, and declared
uniqueness; duplicate/missing/extra column labels and wrong physical types fail.
Float NaN is treated as missing, so non-nullable floats reject it. There is no
implicit cast, repair, sampling, projection, or `toPandas` fallback.

Each Spark input/output is eagerly materialized into a run-owned local checkpoint
before validation. Validation and publication use that exact materialized batch,
not repeated evaluation of a nondeterministic transformation. Only aggregate
validation metrics are returned to the driver, not the full dataset. Consumers
receive distinct DataFrame objects over the owned batch. Intermediates are freed
after their last consumer and on failure.

Only owned checkpoint RDDs are unpersisted; Delta/Spark metadata caches and
caller-owned persisted data are not globally cleared. The release hook uses a
version-specific classic Spark logical-RDD API and is covered by the pinned
matrix. Local checkpoints are ephemeral, not durable recovery storage: executor
or checkpoint loss fails the run rather than rereading a newer source or
recomputing unchecked data. Incomplete cleanup is reported as `cleanup_pending`.
See Spark's [local checkpoint contract](https://spark.apache.org/docs/4.1.2/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.localCheckpoint.html)
for its non-durable executor-storage boundary.

Pandas Delta reads validate native Arrow types before constructing declared
nullable Pandas arrays. Their rows undergo the same logical checks as CSV.

## Fail-Closed Create and Recovery

Only explicit `WritePolicy(mode="create", atomicity="required")` is supported.
Append, overwrite, conditional replacement, and weaker atomicity are rejected at
compile time or manifest validation. Legacy Delta mode tests do not enable those
operations in v1. Native `error` mode is used only inside an exclusive owned stage.

The output parent must already exist on a trusted local filesystem. A private
sibling directory receives a complete native version-zero Delta table. The writer
checks that its log contains portable relative data references, checks the owned
inventory, fsyncs files and directories, atomically renames to the absent final
name **without replacement**, and fsyncs the parent. It uses macOS `renamex_np`
with exclusive rename or Linux `renameat2` with `RENAME_NOREPLACE`; there is no
overwrite-rename fallback. Unsupported platforms fail closed.
The platform primitives are documented by
[Apple](https://developer.apple.com/documentation/foundation/urlresourcekey/volumesupportsexclusiverenamingkey)
and the [Linux manual](https://man7.org/linux/man-pages/man2/rename.2.html).

Concurrent creators cannot overwrite each other, even if another creator makes
an empty directory. Preparation failure leaves the final target absent. Cleanup
checks ownership/identity; changed stages are retained rather than deleting
foreign resources. A failed Spark staging job may still have work to inspect, so
its stage is conservatively retained with `cleanup_pending`, not blindly removed.
No committed output is deleted as rollback.

An error after attempted publication/durability checking is `uncertain`; no
automatic retry is made. A later node failure retains earlier `committed`
publications. Python and CLI share summaries and exits 0/2/3. JSON mode suppresses
ordinary Python and native console output during execution; it is not a sandbox
for hostile project code or a general logging security boundary.

Read-side guards reject symlinked logs/files, non-local file references, and data
outside the bound table. Parent replacement races, malicious mutation of owned
stages, crash/power-loss recovery, arbitrary filesystem behavior, distributed
writers, advanced Delta features, and multi-output atomicity are not certified.

## SAFE / CORE Acceptance Matrix

| Work item | Spark acceptance evidence |
|---|---|
| SAFE-00 | Distinguishes legacy native Delta checks from actual v1 execution; exact engine/JVM versions recorded |
| SAFE-01 | Explicit create, compile-time mode rejection, real second-create preservation and competing creators |
| SAFE-02 | Strict rejection of conditional/provider controls; failed validation/preparation never becomes overwrite |
| SAFE-03 | Spark scaffold starts no engine/installer; collisions, partial rollback, owned stages, uncertain cleanup and committed-output retention |
| SAFE-04 | Catalog tests run for `spark.delta`, `pandas.delta`, and `pandas.csv`, including duplicate keys/IDs and atomic reload |
| SAFE-05 | Utilities remain absent; no SCD/deduplication API is restored with the adapter |
| CORE-01 | Engine-free versioned definitions/bindings; strict target/option/type/access validation |
| CORE-02 | Explicit engine/resources, separate projects/namespaces, borrowed session preservation and owned session closure |
| CORE-03 | Complete graph suite runs for Spark and Pandas; signatures/resources, cycles, producer uniqueness, access, modes and overlapping paths checked before execution |
| CORE-04 | Actual Spark full-batch type/null/uniqueness/finiteness checks, stable materialization, pinned reads and atomic Delta create |
| CORE-05 | Actual Python/CLI execution, JSON summaries, exits, partial/uncertain outcomes, no legacy engine-registry dispatch |
| CORE-06 | Executable Spark scaffold, locked dependencies, required Spark CI invocation, installed-wheel Spark execution without Pandas/PyArrow |

Run source regressions with `pytest tests --spark-delta`; see
[Delta testing](delta-testing.md) for the JVM environment. Installed-wheel checks:

```sh
uv build --wheel --out-dir dist
python tests/distribution/check_spark_delta_wheel.py dist/flint_core-0.7.0-py3-none-any.whl
python tests/distribution/check_wheel.py dist/flint_core-0.7.0-py3-none-any.whl
```

CI is configured to provide Java 17 and run the Spark-enabled complete suite on
Python 3.11/3.12. Local evidence does not claim that remote CI has executed.

The final local source run on October 18, 2026 reports **728 passed**, no skips or
deselections, and **89.10%** source statement coverage. It includes 273 catalog
cases across three adapters, 38 graph cases across both engines, and 51 v1 Delta
cases. Fresh installed-wheel checks also pass: Spark runs without Pandas/PyArrow,
owned sessions close, borrowed sessions survive, and repeated create preserves data.

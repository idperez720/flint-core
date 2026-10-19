# Minimum Safe Project Workflow

**Draft API — implemented for local canonical CSV and Delta on supported POSIX filesystems.**

The opt-in Runtime/Catalog workflow is separate from legacy `DataCatalog` and
`flint_core.core.pipeline`. It adds no Utilities, scheduler, distributed
graph execution or cloud publication. The wider Architecture RFC stays Draft.
For the first-class Spark engine, explicit resources/session ownership, atomic
Delta creation, and SAFE/CORE coverage, start with the
[Spark-first Delta workflow](spark-delta-v1.md). This page retains the minimal
Pandas/CSV example; `pandas.delta` is also a supported binding.

## Generate and Run

Using the wheel built from this branch, with the Pandas extra installed:

```sh
flint init --format v1 --path ./customers-project --no-input
flint validate --project ./customers-project --env dev --json
flint run --project ./customers-project --env dev --dry-run --json
flint run --project ./customers-project --env dev --json
```

Generation only writes a project. It does not install dependencies or change an
environment. The generated README explains environment creation and installation
of the branch wheel with its `pandas` extra. Metadata validation and compilation
work without Pandas; execution requires it.

The first run creates `data/customers.csv`. The second fails and preserves its
bytes. Archive/remove a disposable example output deliberately before repeating
the demonstration; Flint never chooses overwrite for you.

Default `flint init` remains legacy. `--format v1` generates only the minimal
`dev`/local-CSV example. Legacy manager/pattern/environment/extras flags are
rejected in this mode. Collisions and symlinks block generation; partial failure
cleans only ownership-verified resources, preserving unrelated/changed content.

These changes are **unreleased**. Source `__version__` now matches the existing
distribution metadata, `0.5.0`; no release or version bump was made. Use the
branch wheel, not the old published package with the same version. Select a new
release version before publishing these breaking changes.

## Project and Factory Contract

```toml
[tool.flint]
schema_version = 1
project_name = "customers-project"
pipeline_factory = "project_code.pipelines:build_pipelines"
definitions = ["catalog/definitions.yaml"]
default_pipeline = "customers"
source_dir = "src"

[tool.flint.environments.dev]
bindings = ["conf/bindings.dev.yaml"]
# variables = { DATA_ROOT = "data" }
```

`source_dir` defaults to `src`; `default_pipeline` and environment `variables`
are optional. Other displayed fields are required. Unknown fields/environments
fail. Paths resolve from the explicit project root, not the invocation directory.
There is no `FLINT_ENV` fallback or inheritance. See [Catalog v1](catalog-v1.md)
for strict logical definitions and bindings.

`FlintProject.load` reads configuration/metadata only. The explicit factory is
imported lazily for compilation or `project.pipelines`. It must return a
non-empty list/tuple of named `Pipeline` instances; duplicate names fail.

Each project owns a catalog, read-only registry, and unique import namespace.
Use relative imports between project modules. No `sys.path` or current-directory
mutation, shared active project, global pipeline registry, or cross-run dataset
cache is used. External imports retain normal Python behavior. Factory and
transformation code is trusted, not sandboxed.

`close()` removes only the owned module namespace. Spark sessions are explicitly
borrowed per run with `run(spark=session)` or created/closed with
`run(spark_local=True)`; closing a project never stops a borrowed session.
Reloading project code requires a fresh project load.

## Functions, Compilation, and Explicit Persistence

```python
from flint_core import FlintProject, Node, Output, Pipeline, WritePolicy


def clean_customers(customers):
    return customers.assign(name=customers["name"].str.strip())


def build_pipelines():
    return [
        Pipeline("customers", [
            Node(
                name="clean_customers",
                func=clean_customers,
                inputs={"customers": "raw.customers"},
                outputs=Output("curated.customers", WritePolicy()),
            )
        ])
    ]


with FlintProject.load("/work/customers-project", environment="dev") as project:
    plan = project.compile("customers")
    result = project.run(plan)
    print(result.to_dict())
```

`project.run("customers")` uses the same compilation. Without a selection, only
`default_pipeline` is used; its absence is an error, never “run everything.”
Partial node/tag selection is not supported.

Inputs explicitly map parameter names to logical IDs; two arguments may consume
the same ID. Each node returns one DataFrame in its pipeline's explicit engine
(`pandas` by default, or `spark`). No conversion is implicit. An `Output` without `write`
remains run-scoped even when it has a binding. Unbound intermediates are valid;
external inputs and persisted outputs require appropriate bindings.

Only `WritePolicy(mode="create", atomicity="required")` is supported (also the
explicit policy's defaults). Other modes/guarantees fail before dataset I/O.

Compilation checks the entire graph: repeated node objects/names, duplicate
producers, cycles, schemas/references, signatures, access, bindings, policies,
capabilities, and repeated publication destinations. Variadic and positional-only
signatures are rejected. Required parameters must be mapped; Python defaults
remain allowed. Stable topological order gives declaration priority to ready nodes.

`validate` never imports pipelines. `run --dry-run` imports the trusted factory
and compiles without calling transformations or framework dataset I/O. Trusted
module imports may themselves have side effects; neither command is a sandbox or
proof of data quality/connectivity. Foreign, forged, and stale plans are rejected;
catalog reload requires compiling a new plan.

## Executed Data Contract and CSV Codec

External inputs are fully read/validated before consumption. Output frames are
copied and checked before downstream consumption/publication. Pandas consumers
receive separate copies; Spark uses eagerly materialized, owned checkpoints and
distinct DataFrame objects. Run-private frames are released after their final consumer.

- `int64`: numpy/Pandas `int64` or nullable `Int64`.
- `float64`: `float64`/`Float64`, with infinities rejected.
- `boolean`: `bool`/`boolean`.
- `string`: Pandas string dtype or object columns containing strings/missing
  values, not arbitrary objects or all-null numeric columns.
- Missing values are `None`, `pd.NA`, and floating `NaN`;
  `nullable: false` rejects them.
- Presence, unique labels, extra-column policy, and declared uniqueness are
  checked over every row, not a sample. No conversion, projection, or row repair
  is silently substituted for validation.
- `extra_columns: allow` preserves extras and order. Untyped CSV extras must
  contain strings/missing values; numeric extras fail instead of changing type.

The codec is explicit, not permissive Pandas inference: UTF-8, a unique header,
consistent widths, `true`/`false` booleans, range-checked signed decimal int64
without leading zeroes, and finite decimal/exponent floats.
`\\N` is the null marker; empty text and literal `NA` remain strings. Non-null
strings beginning with a backslash escape it with another backslash; the writer
performs that escaping. Empty batches produce typed header-only files.

Decoding constructs declared nullable arrays; malformed values fail rather than
becoming nulls. Generic CSV sources may need an application-owned conversion.
Arbitrary CSV options, compression, and append are not supported. With create,
uniqueness is checked over the entire new table, not just an appended batch.

## Publication, Recovery, and Results

The CSV adapter requires POSIX, no-follow opens, same-filesystem hard links, and
directory fsync. Output parents must already exist. Symlinked dataset components
are rejected. It validates an owned frame, writes/fsyncs an exclusive stage,
checks the path again, links it atomically to the absent final name, and fsyncs
the parent. A competing create cannot be overwritten.

Preparation failure leaves the target unchanged. Only owned stage identities are
cleaned; replaced/uncertain artifacts are retained with `cleanup_pending`.
An error during attempted linking or post-link durability checks is uncertain,
never automatically retried. No committed target is deleted or rolled back.

The directory tree must remain trusted. Malicious parent replacement, concurrent
editing of owned stages, arbitrary hostile filesystem races, crash/power-loss
certification, remote storage, and multi-output transactions are not guaranteed.
Only concurrent final-name creation is certified. No persistent recovery journal
is introduced.

Delta creates a complete native table in an owned sibling directory, verifies
relative data references, fsyncs its contents, and publishes via exclusive
directory rename rather than hard links. Its full contract and platform limits
are in the [Spark/Delta guide](spark-delta-v1.md). The same structured outcomes
apply to Spark and Pandas, including retained committed/uncertain outputs.

`RunResult` exposes only run/pipeline/environment IDs, status, completed nodes,
publication states, failing node/error category, and cleanup flags. It excludes
frames, sessions, variable values, source paths, row samples, and raw exceptions.
A later failure reports earlier publications as `committed`, never as globally
rolled back. Check `status`, `publications`, and `cleanup_pending` before recovery.

Python raises configuration/plan errors before execution and returns runtime
failures as results. CLI uses the same facade:

| Exit | Meaning |
|---|---|
| `0` | Metadata valid, plan compiled, or execution succeeded |
| `2` | Configuration/plan error or known runtime failure |
| `3` | Publication uncertain; manual review required |

Click usage errors retain parser behavior. With `--json`, ordinary Python
stdout/stderr prints are suppressed to protect the summary. Trusted code may
still use its own logging/low-level I/O; this is not a security boundary.

## Installed-Wheel Verification

```sh
uv build --wheel --out-dir dist
python tests/distribution/check_wheel.py dist/flint_core-0.5.0-py3-none-any.whl
```

The checker creates an environment outside the checkout, installs the wheel
without Pandas, verifies its import location/version, generates/validates/compiles
the example, adds Pandas, runs through Python and CLI, and verifies that a second
create preserves bytes. It needs `uv` and potentially dependency downloads;
neither is a Flint runtime dependency.

# Catalog v1 — Minimum Manifest Contract

**Status: Draft — minimum subset implemented as an opt-in API.**

The owner approved a small first implementation: `string`, `boolean`, `int64`,
`float64`, strict declaration validation, and metadata loading without optional
engines. Local bindings now include `spark.delta`, `pandas.delta`, and `pandas.csv`.
This page records the implemented subset for review;
the wider Architecture RFC, project facade, runner, and publication protocol are
not finalized by this increment.

The new entry point is `flint_core.core.catalog.v1.ManifestCatalog`. The existing
`DataCatalog` API and generated legacy manifests remain separate and unchanged.
There is no format guessing or automatic migration. The separate
[project workflow](project-workflow.md) provides the facade, CLI, and executable
local CSV/Delta integration; the [Spark-first guide](spark-delta-v1.md) covers
explicit sessions and execution. This page covers the metadata reader itself.

## Logical Definitions

A definitions manifest requires exactly these top-level fields:

| Field | Accepted value |
|---|---|
| `schema_version` | Integer `1`, not a string, float, or boolean |
| `kind` | `DatasetDefinitions` |
| `datasets` | Mapping from explicit dataset IDs to definitions |

Each definition requires `schema` and may have `description`, `constraints`,
`governance`, and `metadata`. Physical fields such as `engine`, `adapter`,
`storage_path`, and `options` are not allowed here.

Schema fields:

- `columns`: a non-empty mapping of exact, non-blank column names. Each column
  requires `type` and boolean `nullable`; `description` is optional.
- `type`: exactly `string`, `boolean`, `int64`, or `float64`. Aliases such as
  `integer`, dates, timestamps, decimals, and nested types are rejected.
- `extra_columns`: required; exactly `allow` or `forbid`.

Example:

```yaml
schema_version: 1
kind: DatasetDefinitions
datasets:
  raw.customers:
    schema:
      extra_columns: forbid
      columns:
        id:
          type: int64
          nullable: false
        name:
          type: string
          nullable: true
    constraints:
      - id: unique_customer_id
        type: unique
        columns: [id]
        severity: error
    governance:
      owner: data-engineering
    metadata:
      business_description: Customer records
```

`constraints` is an optional list. Only `unique` is recognized in this subset.
Each rule requires `id`, `type`, `columns`, and `severity: error`; no bypass or
warning mode is supported. Rule IDs must be unique within the dataset. The column
list must be non-empty, distinct, and reference columns declared
`nullable: false`.

These are **declarations, not executed checks**. The reader verifies their
structure and references, but does not inspect records, enforce uniqueness, or
certify CSV type interpretation. Those guarantees require the later data-validation
and adapter milestone. Metadata loading must never report a rule as passed.

`governance` accepts only `owner`, `domain`, `classification` (non-blank strings),
and `tags` (a string list). `metadata` is a descriptive mapping of finite
JSON-compatible values, recursively frozen in snapshots. Neither field executes
rules, configures adapters, or grants permissions. Quote annotations resembling
dates, booleans, or numbers when they are intended as text; scalar parsing uses
PyYAML's safe resolver.

## Physical Bindings

A bindings manifest requires `schema_version: 1`, `kind: DatasetBindings`,
`environment`, and a `bindings` mapping:

```yaml
schema_version: 1
kind: DatasetBindings
environment: dev
bindings:
  raw.customers:
    adapter: pandas.csv
    format: csv
    target:
      kind: file
      path: data/customers.csv
    access: read_only
    options: {}
```

Every binding requires `adapter`, `format`, `target`, and `access`.
Accepted combinations are `pandas.csv` / `csv` with `target.kind: file`, and
`spark.delta` or `pandas.delta` / `delta` with `target.kind: delta_table`.
The latter is a local table directory, not a remote catalog identifier.
`access` is `read_only` or `read_write`. `options` defaults to an empty mapping.
CSV accepts no options. Delta additionally permits `version: 0` (or another
non-negative YAML integer), only for read-only bindings. Strings, booleans,
negative versions, timestamps, and provider-specific controls are rejected.

Loading a **binding declaration** is not adapter execution or writer certification.
The binding contains no write mode. Persistence requires an explicit runtime
output policy; `read_write` is not permission to write automatically.

File paths must be non-blank, forward-slash-separated, and project-relative.
Absolute paths, URIs, backslashes, colons, control characters, leading/trailing
whitespace, leading `~`, empty components, and `.`/`..` components are rejected.
The reader joins the validated path to the explicit project root without opening,
checking, or resolving the dataset target.

This lexical check is **not a symlink sandbox** or proof of target permissions.
Runtime target checks and safe publication must address those separately.
Remote locations, catalog-table/query targets, connections, credentials, and advanced
format options are deferred rather than accepted as unenforced configuration.

## Identity, Versions, and References

- Dataset and constraint IDs use
  `[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*`.
  IDs are case-sensitive and are not inferred from directories.
- Dataset IDs are unique across selected definition files. Bindings are unique
  for the selected environment and must reference an existing definition.
  A definition without a binding is valid for an in-memory intermediate.
- Environment identifiers use `[A-Za-z_][A-Za-z0-9_-]*`. Every selected bindings
  document must match the explicit environment. The reader does not consult
  `FLINT_ENV`; checking a project's registered environment list belongs to the
  later project loader.
- Every selected file declares its version and kind. Wrong kinds, unknown fields,
  unsupported versions/types/rules, invalid references, and duplicate declarations
  fail with `CatalogParseError`. There is no legacy or permissive fallback.
- A definitions file may explicitly declare `datasets: {}`; bindings may be empty.
  Empty YAML documents and an absent definitions file list are errors.
- Manifest paths are explicit ordered lists, relative to the supplied root or
  absolute. Missing files, directories, unsupported extensions, and unreadable
  selected files fail. Unselected files are not discovered or parsed.

This initial grammar and the Python model names remain subject to review before
API stabilization. `schema_version` is a document-format version, not a dataset
revision or package version.

## Explicit Variables Only

Only `target.path` may interpolate `${NAME}`, using the `variables` mapping
supplied to the constructor. Variable names use `[A-Za-z_][A-Za-z0-9_]*`;
values must be non-blank strings without nested interpolation.

There is no process-environment fallback, command execution, expression
evaluation, or recursive expansion. Unknown/malformed placeholders fail.
The expanded path must still satisfy the relative-path rules. Interpolation
in logical fields, descriptive metadata, or configuration keys is rejected.
Delta bindings also reject `%`, `?`, and `#` anywhere in the resulting table path
or project root, before native URI interpretation. CSV paths are unaffected.

Variables are copied at construction. Reload reuses the same explicit variables
and environment; create another catalog to change those inputs.

## Python API

The checked-in examples are
[`definitions.yaml`](examples/catalog-v1/definitions.yaml) and
[`bindings.dev.yaml`](examples/catalog-v1/bindings.dev.yaml).
From the repository root:

```python
from pathlib import Path

from flint_core.core.catalog.v1 import ManifestCatalog

catalog = ManifestCatalog(
    project_root=Path.cwd(),
    definitions=["docs/examples/catalog-v1/definitions.yaml"],
    bindings=["docs/examples/catalog-v1/bindings.dev.yaml"],
    environment="dev",
)
snapshot = catalog.snapshot
definition = snapshot.definitions["raw.customers"]
binding = snapshot.require_binding("raw.customers", access="read")
```

`require_binding` checks identity, presence, and declared access only.
It does not instantiate an engine, open a dataset, or verify operating-system
permissions. It rejects write access to `read_only` bindings. The new models
cannot be passed to legacy I/O as if they had already validated data.

Snapshots contain frozen models, mapping proxies, and tuples, including nested
annotations and provenance. Field locations are available through
`snapshot.sources[manifest_path][field_path_tuple]`; each dataset and binding
also carries its declaration location. Diagnostics identify the file, line,
column, and field where applicable without echoing invalid values or YAML excerpts.

`catalog.reload()` constructs a complete candidate under the catalog lock.
Success publishes a new immutable snapshot; failure propagates while preserving
the identical previous snapshot. Retained old snapshots remain valid.
This is not a filesystem transaction: concurrent source-file edits may still
produce a validated combination of different file revisions.

The v1 reader requires only the standard library and PyYAML. It works when the
optional engines are unavailable; legacy package initialization may still attempt
their optional imports. It does not create engine sessions or access dataset
targets.

## YAML Safety and Boundaries

Only one safe YAML document per file is accepted. All mapping keys must be
strings. Duplicate keys at every nesting level, anchors, aliases, and merge keys
are rejected. Python/object tags do not execute.

Limits are 1 MiB per file, 64 composed node levels, and 100,000 composed nodes.
Malformed UTF-8, over-limit inputs, and unsupported YAML constructs fail closed.
These limits do not make trusted project code a sandbox.

The metadata reader itself does not schedule runs, scan registries, convert data,
check rows, or write datasets. Use the separate project workflow for execution.
Default initialization remains legacy; `flint init --format v1` explicitly
generates the new executable example. Utilities remain removed.

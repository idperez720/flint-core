# Migration Notes

## Planned 0.6.0 — Breaking Local Runtime/Catalog Milestone

The next package version is agreed as **0.6.0**, not 1.0.0. The opt-in v1 API and
Architecture RFC remain Draft. A YAML `schema_version: 1` identifies the manifest
format; it does not promise package/API stability or distributed execution.

Before upgrading from 0.5.x, review the removed Utilities below, unsupported write
controls that now fail closed, and duplicate/invalid catalog declarations that
are rejected. Legacy APIs and versioned manifests are separate; migrating to v1
is explicit rather than an automatic catalog rewrite. Verify project-owned
transformations and use disposable outputs for the create-only examples.

The [release review](release-0.6.0.md) records the implemented contract, supported
platform boundary, versioning checks, and gates before merging/publishing.

## Unreleased — Spark and Delta in v1

The opt-in versioned workflow now supports explicit `Pipeline(engine="spark")`,
`Node.resources`, `project.run(spark=session)` or `spark_local=True`, and
`spark.delta` / `pandas.delta` bindings with `target.kind: delta_table`.
See the [Spark-first guide](spark-delta-v1.md). Existing Pandas/CSV defaults remain.

Use `flint init --format v1 --engine spark` for the new executable example.
Metadata/dry-run still require no engines. Real local Spark execution needs the
`spark-delta` extra and Java 17; Pandas Delta uses `pandas,delta` with PyArrow now
included by the Delta extra. No conversion, global session adoption, remote
filesystem support, or implicit append/overwrite is introduced. All v1 writes
remain explicit atomic `create`, regardless of legacy engine mode support.

## Opt-in Catalog v1

The new `flint_core.core.catalog.v1.ManifestCatalog` reads versioned logical
definitions and physical bindings as immutable metadata snapshots. Review the
[minimum contract](catalog-v1.md) before adopting it.

This is separate from legacy `DataCatalog`: there is no format guessing or
automatic rewrite. Do not point a legacy catalog at versioned manifests or treat
declaration validation as executed row/type/uniqueness checks.

The [versioned project workflow](project-workflow.md) executes these manifests
through `FlintProject` and the shared CLI, with a dedicated strict local CSV
adapter rather than permissive legacy I/O. Default initialization remains legacy;
select `--format v1` for the new executable example.

## Unreleased — Utilities Removal

Flint's current scope is Runtime, Catalog, and their I/O integration. Utilities
are deferred. The project owner explicitly chose removal rather than a
deprecation wrapper or runtime-disabled compatibility layer.

The following legacy APIs and implementation modules are removed:

- Top-level exports `flint_core.deduplication` and `flint_core.scd2`.
- The `flint_core.functions` package, including `deduplication` and `scd2`.
- `latest`, `first`, `by_order`, `combined`, and `scd2_process` engine methods.
- The Pandas and Spark SCD2 and deduplication mixin modules.
- Deduplication requirements in the `BaseEngine` interface. Custom I/O engines
  now implement `load` and `save` without transformation utility methods.

Old imports fail instead of silently selecting a replacement algorithm. This
is a breaking change, not a claim that published Utilities have been repaired.
The audited implementations could lose closed SCD2 history or mix values from
different source rows. No replacement Utility API is introduced.

Before upgrading:

1. Find utility imports and direct engine method calls in your project.
2. Replace them with application-owned transformation functions or a separately
   selected implementation, with tests for your business rules.
3. For SCD2, verify that previously closed history, unchanged active records, and
   historical intervals remain intact. For row-selection deduplication, verify
   that every result is an actual source row, including null-valued fields.
4. Re-register those ordinary functions in your existing Flint pipeline nodes.
   Do not copy the retired algorithms without independently validating them.

Runtime nodes, Catalog access, and format adapters remain available. Delta Lake
read/write support belongs to I/O and is not removed with Utilities. This change
does not introduce scheduling or finalize the Draft architecture's future APIs.

# Migration Notes

## Opt-in Catalog v1

The new `flint_core.core.catalog.v1.ManifestCatalog` reads versioned logical
definitions and physical bindings as immutable metadata snapshots. Review the
[minimum contract](catalog-v1.md) before adopting it.

This is separate from the legacy `DataCatalog`: there is no format guessing,
automatic rewrite, new CLI command, or wiring to the existing dataset readers
and writers. Do not point a legacy catalog at versioned manifests or treat
declaration validation as executed row/type/uniqueness checks. The initializer
continues generating the legacy format until the executable-project milestone.

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

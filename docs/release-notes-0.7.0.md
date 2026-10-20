# Flint Core 0.7.0 release notes

Prepared October 20, 2026. Publication is pending the gates in the
[engineering review](release-0.7.0.md). Public API v1 remains Draft.

## Changes

- Compile-time rejection of coroutine, generator and async-generator node
  transformations, including partial functions and callable instances. This
  prevents an invalid deferred transformation from failing only after earlier
  nodes have processed or published data.
- Pandas output validation calls the shared frame validator directly, removing
  an unnecessary CSV-adapter dependency from native Delta execution.
- Public imports for existing `ExecutionPlan`, `RunResult`, `Publication` and
  typed errors from `flint_core.runtime`; existing declaration imports remain.
- Specific plan error codes with optional node/dataset context for Python callers.
- CLI configuration/plan categories, safe actionable hints and useful human
  plan/result summaries. No raw factory/backend exception text is serialized.
- Version-pinned scaffold installation instructions and executable Pandas/Spark
  getting-started guides, with explicit separation from legacy APIs.
- Restored documentation API pages and the configured date plugin dependency;
  strict documentation builds now gate automated releases.
- CI coverage gate raised from 60% to the established 89.16% baseline, with
  two-decimal reporting/comparison and a fractional-threshold regression.

## Compatibility and migration

Synchronous DataFrame transformations, catalog grammar, engine selection,
create-only policies, runtime result statuses and CLI exit codes are unchanged.
Async/generator transformations must become synchronous functions returning one
DataFrame. CLI clients using `configuration_or_plan_failed` must also accept
`project_configuration_failed` and `plan_failed`; `plan_error` and `hint` are
additive fields. English diagnostics are not a machine-readable contract.

No dataset is silently replaced. Per-output commits, uncertain-publication
outcomes, owned-stage cleanup and caller-owned Spark sessions retain their
existing guarantees. Earlier committed outputs survive later failures.

## Limits and next priorities

Supported bindings remain local `pandas.csv`, `pandas.delta` and `spark.delta`;
logical types remain string, boolean, int64 and float64. Append, overwrite,
cloud/distributed execution, scheduling and Utilities remain out of scope.
Legacy extras do not certify new v1 bindings. Adapters are internal seams,
not a newly stabilized plugin API.

For 0.8.0, prioritize evidence from real adapter/orchestrator consumers, an
explicit compatibility policy before stabilizing API v1, and a separately
scoped opt-in diagnostic channel with a privacy contract. Broader storage and
write semantics need their own safety design and certification.

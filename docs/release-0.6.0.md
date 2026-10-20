# 0.6.0 Release Review

**Status: Draft — release preparation, not a published release or merge approval.**

The project owner selected **0.6.0** for the local Runtime/Catalog milestone.
The public v1 API and wider Architecture RFC remain Draft. This review records
the implemented subset and its limits; it does not approve every proposed ADR.
Manifest `schema_version: 1` is independent of the package's pre-1.0 version.

## Contract Review

| Surface | Implemented contract | Deliberate boundary |
|---|---|---|
| Catalog | Strict versioned logical definitions and selected physical bindings; immutable snapshots and atomic reload | Metadata validity is not executed data validation or a multi-file filesystem transaction |
| Identity/schema | Case-sensitive explicit IDs; string, boolean, int64, float64; nullability, extras, unique constraints | No inferred IDs, aliases, dates, timestamps, decimals, warning-only rules, or implicit coercion |
| Project | Explicit root/environment/factory, isolated import namespace, lazy factory loading | Trusted Python code, not a sandbox; no global active project or automatic environment fallback |
| Graph | Complete selected pipeline compiled before framework dataset I/O; signatures, cycles, producers, access, capabilities and path overlaps checked | One DataFrame output per node; no partial selection or implicit engine conversion |
| Spark | Explicit classic local session ownership, full-batch validation over owned materialization | No Spark Connect, remote executors, cloud/shared storage certification, or durable checkpoint recovery |
| Write policy | An output persists only with explicit `create` / required atomicity | No append, overwrite, conditional replacement, schema evolution, or multi-output transaction in v1 |
| Outcomes | Shared Python/CLI execution with committed/uncertain publications and cleanup flags | No deletion of committed outputs, transparent retry, global rollback, or raw exception/data leakage in summaries |
| CLI | `validate` checks metadata; `run --dry-run` compiles trusted code; `run` executes | Exit 0 for success, 2 for known/configuration failures, 3 for uncertain publication |
| Scaffolding | Opt-in v1 Spark Delta or Pandas CSV examples without engine startup or dependency installation | Default initialization remains legacy; Utilities and scheduling are excluded |

See the [Catalog contract](catalog-v1.md), [project workflow](project-workflow.md),
and [Spark-first guide](spark-delta-v1.md) for exact grammars and APIs.

## Safety and Compatibility

Spark is a first-class engine, not a conversion through Pandas. The tested pair
is PySpark 4.1.2 / Delta Spark 4.4.1 with Java 17. Borrowed sessions remain usable;
Flint closes only sessions and materializations it owns. Dataset validation
checks every row, not a sample, without implicit casts or repair.

Local Delta create publication uses owned sibling staging and exclusive rename
on macOS/Linux. CSV uses owned staging and no-replace hard-link publication on
supported POSIX filesystems. Output parents must exist and remain trusted.
Concurrent creators cannot replace each other. Unsupported primitives fail
closed; crash/power-loss certification, malicious parent mutation, and arbitrary
remote filesystem behavior are not promised.

Earlier committed outputs survive later node failures. `uncertain` and
`cleanup_pending` require inspection, not automatic retry or blanket cleanup.
Project imports, transformations, JVMs, and dependency downloads remain trusted
execution, not isolated security sandboxes.

Breaking changes include removal of Utilities without shims, strict rejection
of unsupported write controls, and rejection of duplicate/invalid declarations.
See the [migration checklist](migration.md). The v1 contracts must not be applied
retroactively to legacy I/O handlers or treated as certification of every backend.

## Release Automation Review

The declared release dependency is python-semantic-release 10.6.1 or a compatible
10.x version; the lockfile supplies the tested version. The configuration now
uses `version_variables` (plural) to update `flint_core.__version__` alongside
`project.version`. The previous singular legacy key was ignored by that release
tool and could leave imports, generated project requirements, and wheel metadata
with different versions.

Only the exact `main` branch is release-enabled. With `allow_zero_version = true`
and `major_on_zero = false`, a breaking conventional commit increments the minor
version while the package is 0.x. This preserves the selected 0.6.0 milestone
rather than implicitly declaring a stable 1.0 API. A later 1.0 decision must be
explicit. Retain conventional commit history or use a conventional squash title
that describes the breaking change; an arbitrary merge title is not a substitute.

Regression tests exercise patch/feature/breaking predictions, branch rejection,
read-only prediction, and local application of both version declarations in
disposable repositories. Those checks disable builds, commits, tags, pushes and
VCS releases when applying a test version; no publication is performed.
The actual branch history is also evaluated read-only before preparing the PR.

Source and historic local wheels still declare 0.5.0 until the authorized release
updates the version. Do not install the old PyPI 0.5.0 package to test this work.
Do not reuse an old branch wheel as the 0.6.0 release artifact. The release must
build from the version-updated source and verify installed metadata agrees with
`flint_core.__version__` and generated project requirements.

## Merge Gates

- [x] Owner selected 0.6.0 while keeping the API and RFC Draft.
- [x] Implemented contract and migration boundaries reviewed and documented.
- [x] Read-only version prediction and isolated version-application regressions added.
- [ ] CI passes for the latest PR head on Linux/Python 3.11 and 3.12, including real Spark/Delta.
- [ ] Final review accepts the implementation and any remaining API questions.
- [ ] Owner explicitly authorizes merge/publication after reviewing this PR.

A push to `main` activates the existing semantic-release/build/PyPI workflow
after tests. A green PR does not authorize publishing. Keep the PR Draft until
the remaining reviews are accepted; never enable auto-merge for this preparation.

Distributed Spark, additional Delta write modes, broader types, and orchestrator
adapters belong to later scoped milestones. They are not required to accept this
local milestone, and no Flint scheduler is planned.

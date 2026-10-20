# Flint Core 0.7.0 engineering audit and release gates

Base: `0f4d4909af7f31f0f75f1eee7fa26ba79ec2c97b` (remote main, 0.6.0).
Status: implementation in progress; API v1 remains Draft. No publication claimed.

## Evidence and scope

| Priority | Evidence at baseline | Decision |
| --- | --- | --- |
| Must Fix | `runtime/planning.py` accepts coroutine/generator transformations through `inspect.signature`; execution calls them synchronously and validates the returned coroutine/iterator only after preceding publications | Reject asynchronous and generator callables at compilation, including callable instances and wrapped partials; add regressions |
| Must Fix | `.github/workflows/ci-cd.yml` enforces only `--cov-fail-under=60`, below the required 89.16 baseline | Raise the release CI gate to 89.16; retain full Spark/Delta coverage |
| Should Improve | `runtime/cli.py:_command` catches every exception as `configuration_or_plan_failed` and prints `ok` for successful plans/runs without any context | Distinguish project and graph errors; supply safe actionable hints without raw exception text or data |
| Should Improve | `runtime/planning.py` merges unknown outputs and duplicate producers into one diagnostic; signature errors omit the affected node | Add typed plan codes and node/dataset context for Python callers; keep serialized CLI diagnostics sanitized |
| Should Improve | `runtime/execution.py` selects `LocalCsvAdapter()` as the Pandas output validator even for native Delta pipelines; a regression replacing CSV validation reproduces failure | Call the existing engine-neutral Pandas frame validator directly; keep adapter contracts separate |
| Should Improve | `runtime/__init__.py` exports declarations but omits result, plan and error contracts returned by its facade | Export the existing contracts without exposing adapter implementations or adding a registry |
| Should Improve | `runtime/scaffold.py` generated installation guides claim every version is unreleased, including a published version | Give version-pinned package installation and explicit local-wheel guidance |
| Should Improve | `mkdocs.yml` references absent `docs/api/catalog.md` / `io.md` and a date plugin missing from `docs/requirements.txt` | Restore API pages, declare the configured plugin, and validate the documentation build |
| Future | `runtime/planning.py:_binding_capability`, catalog v1 loader and execution adapter selection deliberately enumerate three certified local bindings | Document these boundaries and extension obligations; no speculative plugin framework or cloud adapters |
| Future | `runtime/project.py` catches trusted factory errors and runtime results intentionally suppress raw backend exceptions | Keep summaries sanitized; a future opt-in diagnostic channel needs a separate privacy contract |

The Runtime consumes immutable CatalogSnapshot declarations; catalog loading does
not start an engine or read datasets. Existing exclusive CSV hard links and Delta
rename, uncertain-outcome handling, per-output commits, session ownership and
catalog reload isolation remain release invariants. No append/overwrite or global
transaction semantics are introduced. Existing dependency extras include legacy
backends; an extra alone does not certify a v1 binding.

## Implementation sequence

1. Harden compilation and export existing public result/error contracts.
2. Add safe CLI diagnostics and correct generated installation instructions.
3. Add regression coverage, run focused checks, then full Python 3.11/3.12,
   real Spark/Delta, formatting, packaging and installation verification.
4. Review the complete diff independently of implementation, fix blockers,
   prepare release notes and version metadata, open PR, verify protections/checks,
   squash merge and publish through the established workflow only if gates pass.

## Release gates

- [ ] Full Python 3.11 and 3.12 tests with real Spark/Delta pass.
- [ ] Coverage >= 89.16% and repository formatting/lint pass.
- [ ] Source/wheel metadata and clean installation verified.
- [ ] Complete-diff review has no critical/high unresolved findings.
- [ ] PR checks and branch protection requirements verified before merge.
- [ ] Merged commit, tag, GitHub release and PyPI installation verified.

The user authorized implementation, PR creation, merge and publication for this
release; repository controls remain mandatory. API stability is a separate future
decision. Orchestration remains external: callers compile and run explicitly and
map RunResult status, publications and cleanup flags to their own task lifecycle;
uncertain publication must never be retried automatically.

## Architecture and safety review boundaries

The audit covered the Runtime facade, planning/execution, both frame validators,
CSV and Delta adapters, exclusive directory publisher, strict Catalog v1 loader,
legacy engine/I/O boundaries, scaffold ownership logic, existing regression
suites, dependency metadata, semantic release configuration and CI. Existing
regressions cover full-batch types/nullability/uniqueness, stale/forged plans,
symlinks, concurrent creators, partial commits, failed durability checks,
cleanup ownership and borrowed Spark sessions. No evidence justified replacing
those mechanisms or introducing a new plugin framework.

Reader/writer contracts deliberately reject unknown options, historical writes,
engine conversion and unsupported targets. Logical schemas and physical bindings
remain separated; catalog-only validation does not import trusted project code.
Legacy cloud/database extras remain available for legacy I/O and are explicitly
excluded from v1 certification. Python compatibility and package/import/version
agreement are verified through the release test and distribution gates rather
than assumed from classifiers. Missing documentation navigation targets were
accidentally excluded by a generic Spark `.gitignore` entry; that entry is removed
so the restored source pages are versioned.

# Runtime API v1 (Draft)

Import declarations from `flint_core` or `flint_core.runtime`. Import result,
plan and error types from `flint_core.runtime`. These names refer to the same
underlying classes; legacy `core.pipeline.Node` remains a separate API.

| Contract | Meaning |
| --- | --- |
| `FlintProject.load(path, environment="dev")` | Read configuration/catalog only; use a context manager to close the project |
| `Node(name, func, inputs, outputs, resources={})` | Synchronous transformation, explicit parameter-to-dataset mapping, one `Output` |
| `Output(dataset, write=None)` | Run-local result; persist only with an explicit write policy |
| `WritePolicy(mode="create", atomicity="required")` | The only supported persistence policy |
| `Pipeline(name, nodes, engine="pandas")` | Ordered declarations with deterministic topological execution |
| `project.compile(name=None) -> ExecutionPlan` | Validate the complete selected graph before processing |
| `project.run(selection=None, spark=None, spark_local=False) -> RunResult` | Compile a name or execute a project-issued current plan |
| `Publication` | Dataset, `committed`/`uncertain` state and cleanup flag |
| `RunResult.to_dict()` | JSON-compatible outcome without data, sessions or raw backend exceptions |

Plans are immutable and project/snapshot scoped. Constructing or copying an
ExecutionPlan does not authorize it for execution. Compile again after catalog
reload. `RunResult.success` is true only for `status == "success"`; cleanup may
still be pending. A run is sequential and commits outputs individually.

## Errors and diagnostics

`ProjectError` describes invalid configuration or factory loading. `PlanError`
describes invalid declarations/graphs and exposes `code`, `node` and `dataset`
context where available. Its default code is `invalid_plan`; specific codes
include `pipeline_selection`, `unknown_output`, `duplicate_producer`,
`signature_mismatch`, `unsupported_callable` and `graph_cycle`.

Execution failures return a RunResult with `failed_node`, `error_code`,
`publications` and `cleanup_pending`. They do not rethrow raw backend exceptions.
`DataValidationError`, `PublicationConflict` (also a FileExistsError), and
`PublicationUncertain` are the existing adapter error categories. Exporting them
does not make adapter classes public. Callers should use the project facade.

The CLI preserves exit codes 0/2/3. Configuration and graph errors now use
`project_configuration_failed` and `plan_failed` respectively; plan failures
include `plan_error`. Summaries add a safe `hint`. Consumers must allow additive
JSON fields and should branch on categories, never parse English messages.
Custom exception text, codes and node/dataset context are not echoed in CLI
configuration/plan errors. Human success output includes metadata/plan/result
context. Runtime result JSON retains its established fields.

## Extension boundaries

Catalog v1 owns strict, versioned declarations and immutable snapshots, without
engine startup or data processing. Runtime planning checks capabilities against
those declarations. Frame validation and physical publication remain separate
responsibilities. Current certified bindings are `pandas.csv`, `pandas.delta`
and `spark.delta`; target types are local file and local Delta table.

Existing adapters implement `preflight(snapshot, dataset, write=False)`,
`read(snapshot, dataset)` and `publish(snapshot, dataset, frame)`. Frame services
validate owned batches and, for Spark, copy/release owned materializations. These
are implementation seams rather than an open v1 plugin protocol. A new binding
requires coordinated strict catalog grammar, capability checks, execution
selection and tests proving schema validation, no-replace publication, uncertainty
and ownership-aware cleanup. Merely registering a legacy BaseEngine/BaseAdapter
or installing an optional extra does not extend these guarantees. No new registry
is introduced in 0.7.0.

External orchestrators can call the public facade with a caller-owned local Spark
session and inspect RunResult. They own schedules, retries and external task state.
A borrowed session is never stopped by Flint. An uncertain publication requires
inspection; even a known failure can follow earlier committed outputs. A retry
policy must account for those outputs rather than assuming a global rollback.
Cloud, distributed Spark, scheduling and new write modes remain future work.

## Migration from 0.6.0

Synchronous DataFrame transformations and existing bindings need no change.
Replace async/generator transformations with synchronous functions returning one
DataFrame; they now fail compilation. Update CLI consumers using the former
catch-all configuration/plan code to accept the two specific categories above.
No write policy, schema grammar, result status or exit code changes are introduced.
API v1 remains Draft pending broader extension and compatibility evidence.

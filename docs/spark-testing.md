# Local Spark Regression Tests

These checks exercise the **legacy Spark adapter** through `DataCatalog`,
`DataLoader`, `DataSaver`, and `SparkEngine`. They do not add Spark support to the
Draft [minimum project workflow](project-workflow.md), whose v1 bindings still
support only local Pandas CSV.

## Environment

The local checks were run with Python 3.12.7, PySpark 4.1.2, and Homebrew
OpenJDK 17.0.17 on macOS arm64. A working JDK is required; importing PySpark alone
does not prove that its JVM or Python workers can start.

Install the test dependencies into a project virtual environment:

```sh
uv venv .venv --python 3.12
uv pip install --python .venv/bin/python -e '.[spark]' 'pyspark==4.1.2' pytest pytest-asyncio
```

Run from the repository root with `JAVA_HOME` pointing to your JDK. The following
path is specific to Homebrew OpenJDK 17; use your installation's JDK home on other
systems. These settings apply to the test process, not your global shell config:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/pytest -q --tb=short \
  tests/core_tests/test_spark_local_io.py \
  tests/core_tests/test_spark_write_modes.py \
  tests/core_tests/test_io.py \
  tests/core_tests/test_hybrid_connectivity.py \
  tests/core_tests/test_core_scope.py
```

The shared fixture uses `local[2]`, disables the Spark UI, fixes the SQL timezone
to UTC, and stops each session in a `finally` block. Missing PySpark produces an
optional-dependency skip; an installed PySpark with a broken JVM fails rather
than being silently skipped. All real writes use disposable test directories.

## Evidence Boundaries

`test_spark_local_io.py` starts real Spark sessions and Python workers, writes
real CSV, Parquet, JSON, and ORC directories, and reads the results back. It checks:

- `error`, `ignore`, `append`, and `overwrite`, both for absent and existing targets.
- Byte-for-byte preservation after rejected `error` writes and `ignore` writes.
- Catalog column projection, declared long/string reads, and CSV header/delimiter options.
- Rejection of invalid modes, reserved operation options, unsupported non-Delta
  conditional overwrite, and missing columns, without changing existing files.
- Nullable boolean, long, double, decimal, date, and timestamp round trips through
  the legacy engine in all four local formats.

The separate writer unit tests use mocks to check delegation. Infrastructure,
JDBC, and Unity Catalog tests check configuration/routing, not real connectivity.
They must not be presented as successful cloud or production database tests.

These results do **not** establish Spark Delta/Iceberg support, remote storage
behavior, cluster deployment, Spark Connect, Airflow/Dagster/Databricks
integration, concurrent-writer safety, or crash-safe/atomic overwrite. Native
Spark append/overwrite remain legacy operations, not the v1 atomic-create
contract. No scheduler or Utilities are introduced, and the RFC remains Draft.

## Initial Local Results — October 18, 2026

- Real local Spark I/O regressions: **36 passed**.
- Complete suite (`pytest tests -q --tb=short`) with the same environment:
  **451 passed**, no skips or deselections.
- No engine implementation changes were needed for these tested combinations.
  This is local source-checkout evidence, not a release or cluster certification.

Subsequent [local Delta validation](delta-testing.md) covers the separate
Delta Spark integration, its required setup, and an observed incompatible version
pair. The initial native-format checks above do not substitute for those tests.

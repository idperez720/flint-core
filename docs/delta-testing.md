# Local Delta Validation

Delta has two distinct integration paths in the legacy API:

- Pandas uses the Rust-based `deltalake` package (delta-rs).
- Spark requires `delta-spark`, matching JVM artifacts, and an explicitly
  configured Spark session. Installing Flint's `delta` extra does not configure
  Spark or install its Delta JARs.

Neither path adds Delta bindings to the Draft v1 `FlintProject` workflow, which
still supports only local Pandas CSV. The architecture RFC remains Draft.

## Reproduce the Checks

The validation environment uses Python 3.12.7, PySpark 4.1.2, Delta Spark 4.4.1,
deltalake 1.6.1, Pandas 3.0.6, PyArrow 26.0.0, and OpenJDK 17.0.17 on macOS arm64.
The Python and Maven Delta versions must match. Review the upstream
[compatibility matrix](https://docs.delta.io/releases/) and
[release notes](https://github.com/delta-io/delta/releases/tag/v4.4.1)
when changing them; a successful package import is insufficient evidence.

Install explicit test dependencies into a project virtual environment:

```sh
uv pip install --python .venv/bin/python -e '.[pandas,spark,delta]' \
  'pyspark==4.1.2' 'delta-spark==4.4.1' 'deltalake[pyarrow]==1.6.1' \
  pytest pytest-asyncio
```

Run the existing Pandas/delta-rs reads, modes, and safety checks:

```sh
.venv/bin/pytest -q --tb=short -k delta \
  tests/core_tests/test_delta_reads.py \
  tests/core_tests/test_write_modes.py \
  tests/core_tests/test_write_safety.py
```

Run real Spark Delta transactions:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
.venv/bin/pytest tests/core_tests/test_spark_delta.py --spark-delta -q --tb=short
```

Adjust `JAVA_HOME` for your JDK installation. For the complete regression suite,
use the same environment and `pytest tests --spark-delta -q --tb=short`.

`--spark-delta` explicitly enables Maven resolution via Delta's
`configure_spark_with_delta_pip` helper. It configures all acquired test Spark
sessions before startup so test order cannot accidentally leave the JVM without
Delta classes. Its initial run may require network access and populate the local
Ivy cache. It does not change the application's session defaults. Without the
flag, only the new Spark Delta integration cases skip; ordinary Spark and
Pandas/delta-rs cases remain independent. With the flag, missing Delta setup or
broken JAR compatibility fails rather than being treated as successful validation.

## Transaction Checks

The tests create disposable local tables and verify actual rows and committed
versions, not just calls to a mocked writer. Spark coverage includes:

- All four write modes on absent and existing tables.
- Conditional overwrite that preserves rows outside `replace_where`.
- Malformed predicates, unknown predicate columns, and incoming rows that violate
  the predicate: errors must propagate without committing a replacement snapshot.
- Rejected operation-control overrides, invalid modes, missing catalog columns,
  and incompatible append schemas.
- Version zero, timestamp-based historical reads, and invalid/conflicting
  snapshot selectors without fallback to latest.
- Nullable typed data and basic Spark/delta-rs interoperability in both directions.
- Missing-table reads that do not create a table.

Failed writes are checked for unchanged committed JSON log entries, version, and
readable snapshot. This does not assert that every backend failure cleans up all
uncommitted/orphan files. No vacuum or destructive cleanup is performed.

## Compatibility Finding and Limits

The initial Delta Spark 4.1.0 / PySpark 4.1.2 combination could write/read a simple
table but failed when loading the JVM `DeltaTable` API because
`org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData` was missing.
Therefore that combination must not be described as validated merely because a
smoke test passed. The subsequent **22 real local Spark Delta cases passed** with
Delta Spark 4.4.1 and PySpark 4.1.2. Existing Delta-related Pandas/delta-rs and
validation checks also passed (33 targeted cases). These results do not establish
support for every 4.x version or advanced feature combination.

The complete local suite with `--spark-delta` reports **473 passed**, no skips or
deselections (October 18, 2026). No engine implementation changes were needed for
the tested combinations. This is source-checkout evidence, not release certification.

These checks do not certify remote storage, Databricks/Unity Catalog, streaming,
MERGE/SCD utilities, advanced table features, external orchestration, concurrent
writers, cross-table transactions, recovery after process/power loss, or other
platform/version combinations. Utilities remain removed; no scheduler is added.

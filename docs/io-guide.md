# 📥 Data Ingestion & Persistence (I/O)

`flint` implements a metadata-driven, environment-agnostic input/output boundary
to isolate data loading and saving routines from physical file locations and storage
backends.

Through `DataLoader` and `DataSaver`, the framework decouples business code logic
from storage-specific client syntax, allowing seamless swaps between computing
drivers.

---

## 1. Decentralized Storage Path Resolution

The framework handles path targets dynamically via an internal resolution proxy. It
evaluates the declared `storage_path` and applies two protection strategies:

1. **Multi-Cloud Protection**: If a URI protocol namespace is intercepted (e.g.,
   `s3://`, `gcs://`, `abfss://`), the pathway is left fully intact, allowing downstream
   cloud drivers to process credentials natively.
2. **Local Root Anchoring**: Relative local paths are automatically anchored and
   resolved against the root workspace directory containing the `pyproject.toml` file.

---

## 2. Ingestion Workflows with DataLoader

The `DataLoader` translates semantic catalog keys into concrete memory DataFrames.

### Options Overriding Hierarchy
When compiling the final parameters for a data source, `flint` merges options dictionary
payloads using a strict prioritization order:
1. Baseline configurations declared under the dataset catalog `options` YAML key.
2. Programmatic runtime overrides passed explicitly to the `.load(options={...})` method.

### Advanced Reading & Lakehouse Time Travel
For advanced analytical formats that support snapshot indexing (such as Delta Lake or
Apache Iceberg), `DataLoader` maps time-travel parameters into standard keys:
* Passing `version=4` injects `versionAsOf` into the underlying driver.
* Passing `as_of="2026-07-09"` injects `timestampAsOf` into the backend.

For the Pandas Delta adapter, `version` accepts a non-negative integer or its
decimal string representation, including version `0`. Timestamp selection uses
a full RFC 3339 timestamp such as `2026-07-09T12:00:00Z`, delegated to
`deltalake.DeltaTable.load_as_version`. Use either a version or a timestamp, not
both, including selectors supplied through catalog or runtime options.
Invalid selectors fail explicitly; native snapshot errors propagate without
falling back to the latest data. The local adapter is tested with
`deltalake==1.6.1`; this does not certify Spark Delta or cloud credentials.

```python
from flint_core.core.io import DataLoader

loader = DataLoader()

# Basic ingestion pulling from catalog definitions
df = loader.load("bronze_users")

# Advanced ingestion with runtime override options and time-travel controls
df_historical = loader.load(
    dataset_name="silver_orders",
    options={"mergeSchema": "false"},
    version=12
)

```

---

## 3. Persistence Workflows & Schema Enforcement Gates

The `DataSaver` handles data platform writes using a highly defensive mechanism.

### The Strict Validation Rule

Before contacting any storage drive or initiating file writing execution, `DataSaver`
submits the DataFrame to the target dataset contract validation layer:

```text
[Your DataFrame] ──> [DataSaver.save()] ──> [validate_schema()] ──> [Storage Backend]
                                                    │
                                     (Fails? ColumnValidationError)

```

If the physical DataFrame columns mismatch or fail the structural requirements defined
in the YAML catalog config, the operation aborts instantly by raising a
`ColumnValidationError`. This protects production storage layers from data corruption.

```python
from flint_core.core.io import DataSaver

saver = DataSaver()

# Secure persistence (aborts if df columns do not match catalog specs)
saver.save(df, "gold_metrics", mode="overwrite")

```

### Operational Save Modes

* **`"error"`** *(Default)*: Fails if the target already exists, without replacing it.
* **`"overwrite"`**: Replaces the declared target with the supplied data.
* **`"append"`**: Preserves existing rows and adds the supplied rows where supported.
* **`"ignore"`**: Skips publication if the target already exists where supported.

Unknown modes raise `ValueError`. Unsupported mode/backend combinations raise
`UnsupportedBackendError` before writing. Provider options cannot set `mode`,
`if_exists`, `append`, `overwrite`, `predicate`, or `replaceWhere`; use the public
`mode` and `replace_where` arguments instead. This also applies to catalog options.

### Pandas Write Capabilities

| Target | Supported modes | Limits |
|---|---|---|
| Local CSV file | `error`, `ignore`, `overwrite`, `append` | Append requires matching column order, a header, no index, no compression, and UTF-8 encoding |
| Local Parquet, JSON, ORC file | `error`, `ignore`, `overwrite` | Append and partitioned directory writes are rejected |
| Remote CSV, Parquet, JSON, ORC | `overwrite` | Other modes are rejected because remote existence and safe append are not implemented |
| Delta table | `error`, `ignore`, `overwrite`, `append` | Modes are passed to the native Delta writer |
| SQLAlchemy relational/Databricks table, BigQuery | `error`, `overwrite`, `append` | Mapped to `fail`, `replace`, and `append`; `ignore` is rejected |
| Pandas Iceberg and Snowflake | `append` | Other modes are rejected instead of silently appending |

Custom Pandas handlers must declare `supported_write_modes`; an undeclared mode is
rejected. Cloud and database capabilities still require the corresponding packages
and backend configuration; the matrix describes dispatch behavior, not certification
of every external integration.

Local files are serialized into a temporary file in the same directory and then
published. Serialization failure preserves the old destination. `error` and `ignore`
use exclusive publication so a concurrently created file is not replaced. CSV append
copies existing bytes, checks column order, writes only new records, and preserves
a single header. This requires a full file copy and temporary disk space proportional
to the existing file. A missing CSV target is created without replacing a racing writer.

Local overwrite and append require one writer per target. Staging does not provide
isolation against concurrent updates to an existing file, filesystem crash durability,
or a transaction across outputs. Use a transactional backend when these guarantees
are required. Compressed/local directory appends and unsupported remote modes must
not be retried as overwrite.

### Node Output Modes

Legacy `Node` declarations still persist their declared destination outputs, but each
output now defaults to `error`, so running a node cannot silently replace an existing
dataset. Declare other modes per output:

```python
from flint_core.core.pipeline import Node

node = Node(
    name="load_customers",
    func=load_customers,
    inputs=[],
    outputs=["raw.customers"],
    write_modes={"raw.customers": "append"},
)
```

Unknown output names or modes are rejected during node construction. This is a
compatibility safety measure; the RFC's separate in-memory output model remains
subject to implementation review. Multiple output publications remain independent.

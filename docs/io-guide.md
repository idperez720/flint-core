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

* **`"error"`** *(Default)*: Throws a `FileExistsError` if data is already present.
* **`"overwrite"`**: Wipes the destination path entirely and writes fresh data.
* **`"append"`**: Appends new rows to the existing files structure destination path.
* **`"ignore"`**: Silently skips execution if files are already present at the target path.

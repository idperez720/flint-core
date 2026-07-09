# 🗺️ Data Catalog & Multi-Environment Isolation

The `DataCatalog` is the central engine of `flint`. It enforces a strict
"Convention over Configuration" layout to parse declarative dataset metadata,
isolate environments, interpolate runtime tokens, and manage cascading session
properties.

---

## 1. Catalog Layout Blueprint

When you initialize a project layout via the CLI, `flint` structures your configuration
directories underneath the root `pyproject.toml` workspace anchor:

```text
your-project/
├── conf/
│   ├── catalog/              # Decentralized declarative dataset manifests
│   │   ├── bronze_marts.yml
│   │   └── silver_contracts.yaml
│   ├── envs/                 # Isolated profile subdirectories
│   │   ├── dev/
│   │   │   ├── variables.yml # Sandbox variables and tokens
│   │   │   └── spark.yml     # Local execution tuning overrides
│   │   └── prod/
│   │       ├── variables.yml # Cloud production storage buckets
│   │       └── spark.yml     # Distributed cluster compute properties
│   └── spark.yml             # Global shared immutable session defaults
└── pyproject.toml            # Root anchor tracking boundary discovery

```

---

## 2. Declarative Dataset Syntax

Every dataset file inside `conf/catalog/` contains logical string lookup keys map
representing your physical platform assets.

### Specification Schema Fields

* **`engine`** *(Required)*: Computational backend runtime framework (`pandas`, `spark`).
* **`format`** *(Required)*: Physical file serialization format (`csv`, `parquet`, `delta`).
* **`storage_path`** *(Required)*: File URI or system path.
* **`columns`** *(Optional)*: Strict schema lists for type coercion and enforcement.

Example (`conf/catalog/orders.yaml`):

```yaml
gold_orders_summary:
  engine: 'spark'
  format: 'parquet'
  storage_path: 's3://${datalake_bucket}/gold/orders_summary/'
  columns:
    - name: 'order_id'
      type: 'integer'
    - name: 'total_amount'
      type: 'decimal(10,2)'

```

If the declarative syntax lacks any required structural fields or defines a corrupt type,
`flint` throws a `CatalogParseError` during the initial tree-walking discovery phase.

---

## 3. Hierarchical Environment Resolution

When instantiating the `DataCatalog`, `flint` resolves the active deployment profile
using a defensive three-step hierarchy:

1. Direct keyword argument override (e.g., `DataCatalog(env="prod")`).
2. System OS environment variable lookup (`FLINT_ENV`).
3. Safe fallback default configuration string: `"dev"`.

```python
from flint_core.core.catalog import DataCatalog

# Resolves automatically via FLINT_ENV or defaults to "dev"
catalog = DataCatalog()

# Hardcoded environment profile isolation override boundary
prod_catalog = DataCatalog(env="prod")

```

---

## 4. Recursive Token Interpolation

To maintain absolute environment-agnostic pathways, you must avoid hardcoding bucket
names or server connections inside your dataset manifests. Instead, leverage the
analytical regex token extraction syntax: `${variable_name}`.

During instantiation, the engine scans your compiled metadata payload recursively. For every token found, it substitutes the string placeholder using a fallback strategy lookup:

1. It queries the active isolated profile profile file: `conf/envs/{env}/variables.yml`.
2. If omitted locally, it performs a fallback check against system environment variables (`os.getenv`) to resolve runtime credentials and injection tokens securely.

### Example Variable Profiles

`conf/envs/dev/variables.yml`:

```yaml
datalake_bucket: "flint-sandbox-dev-bucket"

```

`conf/envs/prod/variables.yml`:

```yaml
datalake_bucket: "enterprise-datalake-production"

```

At runtime, a `DataLoader` targeting `gold_orders_summary` reads from the sandboxed
bucket under `dev`, but mutates cleanly to the secure production cloud cluster URI under
`prod` without changing a single line of Python pipeline code.

---

## 5. Cascading Spark Configuration Merge

When executing distributed workflows, session tuning constraints vary heavily between
deployment boundaries. For instance, local notebook prototyping requires minimal partitions
to avoid unnecessary thread scheduling overhead, while production cloud environments
demand large shuffle counts and memory boundaries to prevent Out-Of-Memory (OOM) crashes.

Calling `.get_spark_configuration()` triggers a deterministic dictionary `.update()`
merge workflow:

1. The engine reads the global baseline from `conf/spark.yml` (if present).
2. It overlays environment-specific properties from `conf/envs/{env}/spark.yml`.

### Inheritance Rule Matrix
* Parameters declared globally but omitted inside the target environment are
  **transparently inherited**.
* Environment-specific parameters **strictly overwrite** global duplicates.

---

### Concrete Blueprint Examples

Here is how to structure your configurations to leverage this cascade behavior seamlessly.

#### Global Baseline (`conf/spark.yml`)
This file establishes shared corporate constraints, performance conventions, and base Jars
packages across every developer instance:

```yaml
# Global baseline session parameters managed by convention via flint
spark.app.name: "flint-core-enterprise-platform"
spark.sql.shuffle.partitions: "2"
spark.default.parallelism: "2"
spark.sql.execution.arrow.pyspark.enabled: "true"

```

#### Production Overrides (`conf/envs/prod/spark.yml`)

When running under `DataCatalog(env="prod")`, `flint` pulls this file to inject massive scaling
allocations and cloud connectors. Note that it overrides the partition metrics
while inheriting the delta extension setups automatically:

```yaml
# Production profile tuning overrides targeting heavy cluster scaling
spark.sql.shuffle.partitions: "200"
spark.default.parallelism: "200"
spark.executor.memory: "8g"
spark.driver.memory: "4g"

# Explicit cloud cluster external dependencies download allocation
spark.jars.packages: "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0"

```

---

### Runtime Compiled Outcome

When a pipeline requests the configuration mapping context under the `prod` profile,
the resolved internal dictionary structure evaluates to this exact merged blueprint:

```python
# Programmatic call inside your application code
spark_opts = catalog.get_spark_configuration()

# Resulting dictionary output (notice the overwritten and inherited fields):
{
    "spark.app.name": "flint-core-enterprise-platform",         # Inherited
    "spark.sql.shuffle.partitions": "200",                       # Overwritten
    "spark.default.parallelism": "200",                          # Overwritten
    "spark.sql.execution.arrow.pyspark.enabled": "true",         # Inherited
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", # Inherited
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.executor.memory": "8g",                               # Prod Specific
    "spark.driver.memory": "4g",                                 # Prod Specific
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4..." # Prod Specific
}

```
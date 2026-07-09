# ⚙️ Engines, Formats, and Cloud Platforms Reference

`flint` uses an Inversion of Control (IoC) registry model to strictly decouple the
computational execution driver (`engine`) from the serialization schema (`format`)
and physical target storage infrastructure (`storage_path` URI protocols).

This reference matrix covers all native and optional pluggable combinations
available via framework dependency extras.

---

## 1. Pluggable Computational Engines

To keep the installation footprint lightweight, core processing runtimes are declared
as optional dependencies. Activate them via your environment package manager:

```bash
# Using uv to install specific engine extras
uv add flint-core --optional pandas --optional spark

# Using Poetry
poetry add flint-core -E pandas -E spark

```

### Supported Core Runtimes

* **`engine: 'pandas'`**: Ideal for single-node memory operations, prototyping,
research notebook zones, and localized data processing batches.
* **`engine: 'spark'`**: Tailored for high-volume distributed clusters processing.
Automatically intercepts and binds active `SparkSession` contexts transparently.

---

## 2. Serialization Formats Matrix

`flint` routes reading and writing routines to appropriate sub-drivers depending
on the combination of the declared format and engine fields.

| Format Token | Supported Engines | Target Use Case Profile |
| --- | --- | --- |
| `csv` | `pandas`, `spark` | Flat exchange plain text boilerplate files |
| `parquet` | `pandas`, `spark` | High-performance columnar compression structures |
| `delta` | `pandas`, `spark` | Transactional Lakehouse version-controlled layers |
| `iceberg` | `spark` | Open enterprise multi-engine lake tables management |
| `table` | `pandas`, `spark` | Relational database mapping channels (JDBC/ODBC) |

---

## 3. Storage Platforms & Cloud Providers Configuration

### Local Storage File System

Standard local development setups anchor paths directly to your `pyproject.toml`
root execution boundary:

```yaml
local_csv_source:
  engine: 'pandas'
  format: 'csv'
  storage_path: 'data/raw/transactions.csv'

```

### Multi-Cloud Objects Storage

`flint` bypasses system validation rules when it catches external protocol namespaces,
injecting secure cloud properties down to `fsspec` or Hadoop file system configurations.

```yaml
# AWS S3 Example (Requires: poetry add flint-core -E aws)
aws_parquet_dataset:
  engine: 'pandas'
  format: 'parquet'
  storage_path: 's3://${datalake_bucket}/gold/analytics.parquet'
  infrastructure:
    key: 'aws-access-key-id'
    secret: 'aws-secret-access-key'

# Google Cloud Storage Example (Requires: poetry add flint-core -E gcp)
gcs_spark_dataset:
  engine: 'spark'
  format: 'parquet'
  storage_path: 'gcs://my-gcp-bucket/silver/events/'
  infrastructure:
    fs.gs.impl: 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem'

# Azure Data Lake Storage Gen2 Example (Requires: poetry add flint-core -E azure)
azure_delta_dataset:
  engine: 'spark'
  format: 'delta'
  storage_path: 'abfss://container@storage.dfs.core.windows.net/orders/'

```

??? warning "Required Big Data JAR Connectors"
    When using distributed runtimes like `spark` combined with cloud storage
    protocols (`s3`, `gcs`, `abfss`) or JDBC bridges, you must explicitly
    declare the required Java archive package dependencies (JARs) inside your
    global `conf/spark.yml` manifest or environment-isolated profiles
    (`conf/envs/{env}/spark.yml`) using the `spark.jars.packages` property.

    If you have doubts about managing session properties, token inheritance,
    or cascade configurations, please refer to the detailed [Spark Session 
    Configuration Guide](catalog.md#5-cascading-spark-configuration-merge).

    


---

## 4. Relational & Cloud Data Warehouses

To interact with traditional relational engines or cloud warehouses, configure a
`format: 'table'` dataset. `flint` uses underlying database adapters to stream data
safely.

### PostgreSQL Connection (`poetry add flint-core -E postgres`)

```yaml
postgres_dim_users:
  engine: 'pandas'
  format: 'table'
  storage_path: 'production_schema.dim_users'
  connector: 'postgresql+psycopg2://user:password@localhost:5432/warehouse'

```

### Snowflake Data Warehouse (`poetry add flint-core -E snowflake`)

```yaml
snowflake_finance_report:
  engine: 'pandas'
  format: 'table'
  storage_path: 'FINANCE_DB.REPORTS.QUARTERLY_SUMMARY'
  connector: 'snowflake://user:password@account/warehouse'

```

### Google BigQuery Warehouse (`poetry add flint-core -E bigquery`)

```yaml
bigquery_marketing_funnel:
  engine: 'pandas'
  format: 'table'
  storage_path: 'enterprise-project.marketing.funnel_metrics'

```

### Databricks Lakehouse SQL (`poetry add flint-core -E databricks`)

```yaml
databricks_external_table:
  engine: 'pandas'
  format: 'table'
  storage_path: 'hive_metastore.default.sales_target'
  connector: 'databricks://token@host.azuredatabricks.net:443/catalog'

```

---

## 5. JDBC / ODBC Cross-Platform Drivers

For legacy data infrastructures or custom warehouse providers lacking abstract connectors,
use generic driver bridging mappings.

### Generic SQL JDBC Integration (`poetry add flint-core -E jdbc`)

```yaml
generic_jdbc_source:
  engine: 'spark'
  format: 'table'
  storage_path: 'legacy_application_table'
  connector: 'jdbc'
  infrastructure:
    url: 'jdbc:oracle:thin:@//localhost:1521/XEPDB1'
    driver: 'oracle.jdbc.OracleDriver'
    user: 'system'
    password: 'password'

```
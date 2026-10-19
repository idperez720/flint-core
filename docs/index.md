# 🚀 Welcome to Flint

`flint` is a minimalist, agnostic framework designed to standardize data
engineering pipelines in Python. It completely eliminates environmental friction,
distributed cluster management boilerplates, and fragile absolute storage path
hardcoding.

Flint focuses on two pillars: Runtime for pipeline execution and Catalog for
declarative dataset metadata. Its I/O adapters support Pandas and PySpark;
transformations remain ordinary user-owned Python functions.

---

## ✨ Core Features

* **Convention over Configuration**: Structure your workspace layout once using
  interactive CLI initializers, and let `flint` handle structural orchestration
  safely.
* **Decentralized Data Catalog**: Declare your platform datasets as semantic
  lookup keys inside distributed, self-contained mini-YAML manifests.
* **Multi-Environment Isolation**: Leverage recursive token interpolation and
  cascading configuration properties (`spark.yml`) to switch sandbox environments
  seamlessly.
* **Elastic Data Loading**: Load data matrices using Pandas or PySpark under the
  exact same interface. `flint` dynamically resolves filesystem boundaries
  and binds active Spark contexts automatically.
* **Schema-Enforced Persistence Gates**: Secure storage paths via write validation
  checks that abort operations if the columns mismatch the catalog definition.

---

## 🚀 Next Steps

Utilities are deferred. The next release intentionally removes the legacy SCD2
and deduplication APIs, including their engine methods. Review the
[migration notes](migration.md) before upgrading an existing pipeline.

Ready to build your first standardized data platform workspace? Hop over to our
step-by-step [Getting Started Guide](getting-started.md) to install the framework and
bootstrap your first repository layout in seconds using modern tooling!

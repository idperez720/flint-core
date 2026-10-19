# Flint Core Framework 🚀

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

## Utilities Removed

The next release removes the legacy SCD2 and deduplication APIs and their Pandas
and Spark implementations. Utilities are deferred, not part of the current core.
This is an intentional breaking change; there are no compatibility shims.
See the [migration notes](docs/migration.md) before upgrading existing pipelines.

---

## 📖 Complete Documentation

For comprehensive guides, testing architecture deep-dives, and complete API references, visit our documentation site:
👉 **[Flint Core Docs](https://flint-core.readthedocs.io/en/latest/)**

---

## ⚖️ License

Distributed under the **MIT License**. Any modification or distribution (including forks) must include the original copyright notice and liability waiver. See `LICENSE` for more information.

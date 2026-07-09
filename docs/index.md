# 🚀 Welcome to Flint

`flint` is a minimalist, agnostic framework designed to standardize data
engineering pipelines in Python. It completely eliminates environmental friction,
distributed cluster management boilerplates, and fragile absolute storage path
hardcoding.

By combining declarative metadata workflows with agnostic functional transformation
utilities, `flint` empowers data platforms to execute seamless computing workloads
safely across both Pandas and PySpark architectures.

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
* **Agnostic Functional Suite**: Run high-performance advanced features directly on your dataframes.
  The core engine automatically resolves the appropriate optimization drivers via Inversion of
  Control.
* **Schema-Enforced Persistence Gates**: Secure storage paths via write validation
  checks that abort operations if the columns mismatch the catalog definition.

---

## 🚀 Next Steps

Ready to build your first standardized data platform workspace? Hop over to our
step-by-step [Getting Started Guide](getting-started.md) to install the framework and
bootstrap your first repository layout in seconds using modern tooling!
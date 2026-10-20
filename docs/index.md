# 🚀 Welcome to Flint

`flint` is a lightweight, extensible framework for structuring, describing, and
executing data engineering pipelines in Python. Its opt-in v1 workflow currently
supports local Spark/Delta and Pandas CSV/Delta; it does not manage clusters.

Flint focuses on two pillars: Runtime for pipeline execution and Catalog for
declarative dataset metadata. Its I/O adapters support Pandas and PySpark;
transformations remain ordinary user-owned Python functions.

---

## ✨ Core Features

* **Executable Project Scaffolds**: Generate a local Spark or Pandas example
  without installing dependencies or starting an engine.
* **Versioned Catalog**: Separate logical dataset definitions from physical
  bindings in strict YAML manifests with explicit environment selection.
* **Explicit Runtime**: Compile the complete graph before execution; invoke the
  same sequential runner from Python or CLI without a scheduler.
* **First-Class Spark**: Pass a session explicitly or request local creation;
  Spark pipelines do not convert through Pandas or silently adopt active sessions.
* **Fail-Closed Persistence**: Validate complete datasets and publish explicit,
  atomic create-only outputs on supported local filesystems.

These are opt-in v1 contracts, not promises about the separate legacy APIs.
The public v1 API and wider Architecture RFC remain Draft in **0.7.0**. See the
[getting-started guide](getting-started.md), [Runtime API reference](runtime-api.md)
and [release review](release-0.7.0.md).

---

## 🚀 Next Steps

Utilities are deferred. Version 0.6.0 removed the legacy SCD2
and deduplication APIs, including their engine methods. Review the
[migration notes](migration.md) before upgrading an existing pipeline.

Ready to build your first standardized data platform workspace? Hop over to our
step-by-step [Getting Started Guide](getting-started.md) to install the framework and
bootstrap your first repository layout in seconds using modern tooling!

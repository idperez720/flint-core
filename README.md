# Flint Core Framework 🚀

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
The public v1 API and wider Architecture RFC remain Draft in **0.7.0**. Start
with the [getting-started guide](docs/getting-started.md), then consult the
[Runtime API reference](docs/runtime-api.md) and
[release review](docs/release-0.7.0.md).

---

## Utilities Removed

Version 0.6.0 removed the legacy SCD2 and deduplication APIs and their Pandas
and Spark implementations. Utilities are deferred, not part of the current core.
This is an intentional breaking change; there are no compatibility shims.
See the [migration notes](docs/migration.md) before upgrading existing pipelines.

---

## 📖 Complete Documentation

The opt-in [Spark-first Delta workflow](docs/spark-delta-v1.md) supports versioned
Catalog manifests, explicit engines/resources, full-plan compilation, and shared
Python/CLI execution. Generate its executable example with
`flint init --format v1 --engine spark`; run with an explicit borrowed session or
`flint run --spark-local`. Spark runs without Pandas. The
[Pandas workflow](docs/project-workflow.md) supports local CSV and Delta too.
All v1 writers support explicit atomic `create` only. Remote/cloud execution,
weaker write guarantees, and a scheduler are not implied. Existing APIs remain separate.

For comprehensive guides, testing architecture deep-dives, and complete API references, visit our documentation site:
👉 **[Flint Core Docs](https://flint-core.readthedocs.io/en/latest/)**

---

## ⚖️ License

Distributed under the **MIT License**. Any modification or distribution (including forks) must include the original copyright notice and liability waiver. See `LICENSE` for more information.

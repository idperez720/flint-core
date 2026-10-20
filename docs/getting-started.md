# 🏁 Getting Started

This guide describes legacy initialization. For the versioned Catalog and shared
Python/CLI runner, see the [minimum project workflow](project-workflow.md) and
select `flint init --format v1`.

This guide walks you through installing `flint-core` and setting up a fresh
production-grade data engineering workspace from scratch.

## 1. Installation & Project Initialization

`flint` offers flexible execution boundaries. You can run the initialization
wizard on-the-fly using ephemeral execution tools (`pipx` or `uvx`), install the
CLI utility globally, or declare it as a local project dependency.

### Option A: Standalone Ephemeral Execution (Recommended for Scaffolding)
To bootstrap a brand new data project repository cleanly without polluting your
system environments, leverage `uvx` or `pipx` to trigger the interactive wizard
instantly inside a temporary sandbox:

```bash
# Using uvx (Blazing fast alternative via astral's uv)
uvx --from flint-core flint init

# Using pipx
pipx run flint-core init

```

Alternatively, install the binary permanently into an isolated global environment toolpath:

```bash
# Via uv
uv tool install flint-core

# Via pipx
pipx install flint-core

# Invoke your global tool binary anywhere
flint init

```

### Option B: Project-Level Dependency Installation

If you are developing custom transformation nodes or integrating the framework API
into an existing pipeline ecosystem, install it via your standard package manager.

Using uv:

```bash
uv add flint-core

```

Using Poetry (Recommended for pipeline environment isolation):

```bash
poetry add flint-core

```

Using pip:

```bash
pip install flint-core

```

## 2. Workspace Layout Architecture

### Initialization safety

`flint init` checks its planned targets before generating files or starting package
managers. Existing project manifests, virtual environments, generated samples, and
configuration files are never overwritten. `requirements.txt` is reserved for the
`venv` and `uv` strategies; `poetry.lock` is reserved for Poetry. Existing unrelated
files and ordinary directories may remain in the destination.

Environment and domain names must be single path components. Paths inside the
resolved project root cannot traverse symlinks, including dangling output links.
Files use exclusive creation so a collision after the initial check also fails
without replacing another creator's output.

If a step fails, cleanup includes that step's partial output. An in-memory journal
records created resources and checks file identity and contents before deletion.
Changed or replaced files, untracked environment files, and resources whose
ownership cannot be verified are preserved with a warning. Directories are removed
only when empty; cleanup never recursively deletes a directory by its name alone.

The virtual environment is reserved before running external tools. `uv` installation
explicitly selects its interpreter. The Poetry strategy creates a local environment
using the running Python interpreter, then invokes Poetry with project-local settings
and without inherited active-environment variables. It does not change global Poetry
configuration or install into a caller's active environment.

Custom scaffold steps should use `context.make_directory()` and `context.write_text()`
for tracked creation and preserve ownership in their own rollback callbacks. Appending
a path to the legacy `created_paths` list alone is not proof of ownership. Custom
callbacks and environment providers remain trusted code.

Use one initializer at a time for a destination. Provider output inside the newly
reserved environment is treated as part of that initialization; unrelated concurrent
writes during provider commands are not supported. The journal is not persistent,
and abrupt process termination, tool caches, or other external effects are outside
its recovery guarantees. Review any preserved resources reported after a failure.

### Layout selection

The wizard prompts you for project metadata with smart fallbacks and displays
an interactive menu to choose your architectural layout paradigm using simplified
numeric selections (1-7):

```text
Select a structural architecture layout pattern:
  1 - default    (Flat directory layout configuration)
  2 - medallion  (Bronze, Silver, Gold layer split)
  3 - kimball    (Staging, Dimensions, Facts split)
  4 - datamart   (Domain isolated business marts)
  5 - datamesh   (Decentralized domain data products)
  6 - inmon      (Corporate data warehouse model)
  7 - datavault  (Agile raw and business hub vaults)

```

Selecting the **Medallion (2)** pattern builds the following convention-driven layout:

```text
your-project/
├── conf/
│   ├── catalog/
│   │   └── bronze/
│   │       └── sample_dataset.yaml  # Layer declarative dataset contracts
│   │   └── silver/
│   │   └── gold/
│   ├── envs/
│   │   ├── dev/
│   │   │   ├── variables.yml        # Isolated sandbox token configurations
│   │   │   └── spark.yml            # Context dynamic runtime configurations
│   │   ├── qa/
│   │   └── prod/
│   └── spark.yml                    # Global immutable environment settings
├── data/
│   └── sample_table.csv             # Physical boilerplate seed data asset
├── src/
│   └── notebooks/                   # Standardized zone for Jupyter research
└── pyproject.toml               # Root environment anchor of the workspace

```

## 3. Declaring Datasets in the Catalog

Open your catalog file (e.g., `conf/catalog/bronze/sample_dataset.yaml`). Datasets are
declared as semantic lookup keys. Specify the serialization format, processing engine,
and storage location pathways relative to the root anchor:

```yaml
sample_table:
  description: 'Boilerplate example dataset created by flint'
  format: 'csv'
  engine: 'pandas'
  storage_path: 'data/sample_table.csv'
  columns:
    - name: 'id'
      type: 'integer'
    - name: 'name'
      type: 'string'

```

## 4. Reading and Writing Data Safely

Create a notebook under `src/notebooks/`. You do not need hardcoded path traversals
or custom config parsers. Ingest and persist data cleanly using decoupled I/O boundaries.

### Loading Data

```python
from flint_core.core.io import DataLoader

# 1. Instantiate the loader (automatically locates pyproject.toml)
loader = DataLoader()

# 2. Fetch the dataset into memory using its logical identifier
df = loader.load("sample_table")

# 3. Inspect your data matrix instantly
print(df.head())

```

### Saving Data (Schema Enforced)

`flint` implements strict library-grade schema validation. If you attempt to save an
incomplete dataframe missing expected catalog columns, the operation aborts defensively
to protect your storage layer from structural data corruption.

```python
from flint_core.core.io import DataSaver

# Instantiate the persistence manager
saver = DataSaver()

# Persist data cleanly back to storage (verifies schema before execution)
saver.save(df, "sample_table", mode="overwrite")

```

## 5. Seamless Big Data Engine Scaling

If your dataset scales up and requires distributed computing cluster workloads, you do
not need to rewrite your code logic. Simply configure a matching entry in your catalog
specifying `engine: 'spark'`.

`flint` dynamically introspects and binds active global `SparkSession` configurations
and performs multi-environment metadata cascade joins under the hood automatically.

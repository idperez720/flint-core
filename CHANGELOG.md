# CHANGELOG

<!-- version list -->

## v0.4.1 (2026-07-09)

### Bug Fixes

- Update environment variable name for PyPI token in CI/CD workflow
  ([`4f16de6`](https://github.com/idperez720/flint-core/commit/4f16de6d8dc18bcab382094f2625e9f70a9eb681))


## v0.4.0 (2026-07-09)

### Bug Fixes

- Allow zero version for package uploads in pyproject.toml
  ([`d6c447e`](https://github.com/idperez720/flint-core/commit/d6c447ea013e4299e3841a7311045cdad5c0e0eb))

- Baseline manual version 0.3.3
  ([`3f27346`](https://github.com/idperez720/flint-core/commit/3f27346b8563078018b25b67e5e94a3b187f9446))

- Enhance project initializer tests with subprocess mocking and virtual environment validation
  ([`8fbd3db`](https://github.com/idperez720/flint-core/commit/8fbd3dbc8b4bc1ca27d00528c5a88f4ed5fd2973))

- Update semantic release step to execute versioning and publishing separately
  ([`d568443`](https://github.com/idperez720/flint-core/commit/d56844315a7f04694dd065be87cdd3a9543b3e02))

### Features

- Add support for selecting Python environment package manager during project initialization
  ([`4029062`](https://github.com/idperez720/flint-core/commit/402906271a32d63e81d871afa962c73ac3289c9f))

- Implement pipeline execution and circular dependency handling; add unit tests for orchestration
  engine
  ([`c3ff358`](https://github.com/idperez720/flint-core/commit/c3ff358a6291ba2f13e7f0059d06fc902a948d0f))

### Refactoring

- Remove unused imports from test_pipeline.py
  ([`19c0ffd`](https://github.com/idperez720/flint-core/commit/19c0ffdb35c1fff06a8de82ccf1042a43e6ee936))

- Update CI/CD pipeline for semantic release and streamline deployment process
  ([`adcad49`](https://github.com/idperez720/flint-core/commit/adcad49013e7a2a491477efec0cf66959fd01066))


## v0.3.3 (2026-07-06)

### Features

- Enhance validation in models and data handling; raise CatalogParseError for invalid configurations
  ([`2061fe8`](https://github.com/idperez720/flint-core/commit/2061fe880c11ee6295d009bef57f4afea4a98a42))


## v0.3.2 (2026-07-06)

### Features

- Enhance project initialization with environment and pattern options; add isolated environment
  templates
  ([`4722d7b`](https://github.com/idperez720/flint-core/commit/4722d7be4bff356ebfeba93c695e544c15b4b377))


## v0.3.1 (2026-07-06)

### Features

- Enhance CLI with pluggable command registry and improved project initialization
  ([`0603051`](https://github.com/idperez720/flint-core/commit/060305181ad1c322afd51140dd8a05e26031b1a2))


## v0.3.0 (2026-07-06)

### Features

- Add optional dependencies for data processing engines and cloud data warehouses
  ([`59fe12d`](https://github.com/idperez720/flint-core/commit/59fe12d24e4f8987444238c04224a5a34c463262))

- Enhance database connectivity with JDBC and ODBC support; add unit tests for connector factories
  ([`dafaf94`](https://github.com/idperez720/flint-core/commit/dafaf947cffe74e3cd06a4bd5cf28001d077e4b0))

- Enhance DataCatalog and DatasetConfiguration, add support for connector attribute in data models
  ([`e8748fd`](https://github.com/idperez720/flint-core/commit/e8748fd9b0d35e6c9a36ed3a8a03a5104d5d3b7a))

- Implement multi-environment support in DataCatalog; add tests for variable isolation and
  interpolation
  ([`6a635a9`](https://github.com/idperez720/flint-core/commit/6a635a96c66d0322cf65315d6de5806e6c5927e0))

- Update dependencies in pyproject.toml for improved compatibility and performance
  ([`986fc00`](https://github.com/idperez720/flint-core/commit/986fc00685c705c0553034f6b4501c741fd6ef34))


## v0.2.1 (2026-07-03)

### Bug Fixes

- Add missing newline at end of file in exceptions.py
  ([`9db20f3`](https://github.com/idperez720/flint-core/commit/9db20f3a2e63ccbc6a48e7c93f3d55443d7afb3a))


## v0.2.0 (2026-07-03)

### Chores

- Add dev_docs directory to .gitignore
  ([`7c536c2`](https://github.com/idperez720/flint-core/commit/7c536c2b3786451e7b16eb5826233fe84b562c96))

- Update extras to add cloud services support
  ([`2b85a52`](https://github.com/idperez720/flint-core/commit/2b85a527cabb87e1257ede8e392af694ad892b3f))

### Features

- Add cloud storage dependencies and update Spark configuration for enhanced compatibility
  ([`8ec52a5`](https://github.com/idperez720/flint-core/commit/8ec52a598bd1e7e7a7accae91910867dfde80b88))

- Add Delta and Iceberg format handlers for distributed data processing; enhance time-travel options
  in DataCatalog
  ([`8b87ce4`](https://github.com/idperez720/flint-core/commit/8b87ce4da20f13378a4a6414d336b8d60b09aea5))

- Add test for pandas engine cloud infrastructure injection with fsspec storage options
  ([`3f3304b`](https://github.com/idperez720/flint-core/commit/3f3304b6b883834b103f31094a4ca170c8eed6e7))

- Enhance data loading and saving with cloud path handling and infrastructure injection
  ([`8798152`](https://github.com/idperez720/flint-core/commit/8798152cc4a98ddfd1427cd9c736391d36a21d3e))

- Enhance DataCatalog and exceptions with improved error handling and new Spark configuration method
  ([`b51289f`](https://github.com/idperez720/flint-core/commit/b51289f42d36bf1043de700d509e17abcc117153))

- Enhance DataSaver and engines to enforce schema during data saving
  ([`3507866`](https://github.com/idperez720/flint-core/commit/3507866610142ec13132cc8a882fea21e37da986))

- Enhance path resolution for cloud URIs and improve infrastructure injection in SparkEngine
  ([`621c51a`](https://github.com/idperez720/flint-core/commit/621c51ae02c542d504023b0be7dc43b1cba2db09))

- Implement format handler architecture for Spark engine with support for multiple data formats
  ([`8f6e789`](https://github.com/idperez720/flint-core/commit/8f6e7891cbafb904e99f7cf3ff50eb7a08f11aef))

- Improve SparkEngine session handling and schema enforcement in save method
  ([`201c159`](https://github.com/idperez720/flint-core/commit/201c15935772dbd80e05a4603775e0c78c00ac78))

### Refactoring

- Improve docstrings and formatting in Pandas engine and format handlers
  ([`8398480`](https://github.com/idperez720/flint-core/commit/83984805125e841f518bca944990bf0c3f83f8a0))


## v0.1.2 (2026-06-27)

### Chores

- Update test coverage threshold to 60% and remove requirements.txt
  ([`1906ed8`](https://github.com/idperez720/flint-core/commit/1906ed83a4ea0c5071aa05bc659dc15544871e79))

### Features

- Enhance DataLoader and engines with multi-format support and advanced metadata handling
  ([`b0c7747`](https://github.com/idperez720/flint-core/commit/b0c7747ae115bd52b8684c8402e869e1c0803e11))

- Implement Pandas and Spark engines for data loading with schema enforcement
  ([`0b2526f`](https://github.com/idperez720/flint-core/commit/0b2526f1863fba1232b81ca29937a8bfb65dbf27))

- Implement save methods for DataLoader, DataSaver, PandasEngine, and SparkEngine to enable data
  persistence
  ([`569c28d`](https://github.com/idperez720/flint-core/commit/569c28deed5ace0730e350b3e4892a9c58384242))


## v0.1.1 (2026-06-27)

- Initial Release

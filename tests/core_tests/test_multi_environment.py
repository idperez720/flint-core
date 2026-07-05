"""Unit tests verifying high-fidelity multi-environment interpolation and variable isolation loops."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import yaml

from flint_core.core.catalog import DataCatalog


def _scaffold_test_environment_hub(tmp_path: Path, env_configs: dict, catalog_str: str) -> Path:
    """Helper method to map centralized layouts into isolated multi-file environment structures."""
    conf_dir = tmp_path / "conf"
    catalog_dir = conf_dir / "catalog"
    envs_base_dir = conf_dir / "envs"

    catalog_dir.mkdir(parents=True, exist_ok=True)

    with open(tmp_path / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write('[project]\\nname = "flint-multi-env-testing"\\n')

    # Create separate folders per environment and dump variables.yml and spark.yml independently
    for env, config in env_configs.items():
        env_dir = envs_base_dir / env
        env_dir.mkdir(parents=True, exist_ok=True)

        with open(env_dir / "variables.yml", "w", encoding="utf-8") as f:
            yaml.dump(config["variables"], f)

        with open(env_dir / "spark.yml", "w", encoding="utf-8") as f:
            yaml.dump(config["spark"], f)

    with open(catalog_dir / "pipelines.yaml", "w", encoding="utf-8") as f:
        f.write(catalog_str)

    return catalog_dir


def test_hierarchical_interpolation_and_spark_isolation(tmp_path: Path) -> None:
    """Asserts that the parsing engine isolates variables and spark tuning configurations per profile."""
    env_configs = {
        "dev": {
            "variables": {"bucket": "dev-raw", "schema": "sandbox"},
            "spark": {"spark.app.name": "Flint-DEV", "spark.sql.shuffle.partitions": "8"},
        },
        "prod": {
            "variables": {"bucket": "prod-analytics", "schema": "gold"},
            "spark": {"spark.app.name": "Flint-PROD", "spark.sql.shuffle.partitions": "128"},
        },
    }

    catalog_str = """
.base: &base
  engine: "spark"
  format: "delta"
  columns:
    - name: "id"
      type: "integer"

target_dataset:
  <<: *base
  storage_path: "s3a://${bucket}/${schema}/users"
"""

    catalog_dir = _scaffold_test_environment_hub(tmp_path, env_configs, catalog_str)

    # 1. Verify Development profile isolation (Variables & Spark Tuning)
    catalog_dev = DataCatalog(catalog_path=catalog_dir, env="dev")
    assert catalog_dev.active_env == "dev"
    assert catalog_dev.target_dataset.storage_path == "s3a://dev-raw/sandbox/users"

    spark_dev_conf = catalog_dev.get_spark_configuration()
    assert spark_dev_conf["spark.app.name"] == "Flint-DEV"
    assert spark_dev_conf["spark.sql.shuffle.partitions"] == "8"

    # 2. Verify Production profile isolation (Variables & Spark Tuning)
    catalog_prod = DataCatalog(catalog_path=catalog_dir, env="prod")
    assert catalog_prod.active_env == "prod"
    assert catalog_prod.target_dataset.storage_path == "s3a://prod-analytics/gold/users"

    spark_prod_conf = catalog_prod.get_spark_configuration()
    assert spark_prod_conf["spark.app.name"] == "Flint-PROD"
    assert spark_prod_conf["spark.sql.shuffle.partitions"] == "128"


def test_system_os_environment_variable_fallback(tmp_path: Path) -> None:
    """Asserts that unresolved local variables fall back natively onto system OS environment keys."""
    env_configs = {"dev": {"variables": {"host": "localhost"}, "spark": {}}}
    catalog_str = """
dataset:
  engine: "pandas"
  format: "postgres"
  storage_path: "users"
  options:
    url: "jdbc:postgresql://${host}:5432/db"
    password: "${ENV_TEST_SECRET}"
"""
    catalog_dir = _scaffold_test_environment_hub(tmp_path, env_configs, catalog_str)

    with patch.dict(os.environ, {"ENV_TEST_SECRET": "highly_secure_token"}):
        catalog = DataCatalog(catalog_path=catalog_dir, env="dev")
        assert catalog.dataset.metadata["options"]["password"] == "highly_secure_token"
        assert catalog.dataset.metadata["options"]["url"] == "jdbc:postgresql://localhost:5432/db"

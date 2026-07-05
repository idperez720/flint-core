"""Command Line Interface (CLI) implementation governing scaffolding and setup for flint-core."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import click
import yaml


@click.group()
def entry_point() -> None:
    """Flint CLI - A minimalist data engineering framework control center."""
    pass


@entry_point.command()
@click.option(
    "--envs",
    default=None,
    help="Comma-separated matrix of deployment environment profiles to scaffold physically.",
)
def init(envs: Optional[str]) -> None:
    """Initializes a new unified flint-core project infrastructure layout."""
    click.echo("🚀 Executing enterprise flint-core project environment layout setup...")

    # Interactive prompt fallback if the user omits the flag
    if envs is None:
        envs = click.prompt(
            "📝 Introduce los entornos para tu proyecto (separados por comas)",
            default="dev,qa,prod",
            type=str,
        )

    project_root = Path.cwd()
    conf_dir = project_root / "conf"
    catalog_dir = conf_dir / "catalog"
    envs_base_dir = conf_dir / "envs"

    # Parse and normalize environment list
    env_list = [e.strip().lower() for e in envs.split(",") if e.strip()]

    # Scaffold core physical directories structure safely
    catalog_dir.mkdir(parents=True, exist_ok=True)
    click.echo(f"📁 Scaffolding structural directory configuration root: {conf_dir}")
    click.echo(f"📁 Scaffolding decoupled data catalog tracking spaces: {catalog_dir}")

    # Step 1: Seed global analytical engine baseline settings (The immutable shared core config)
    global_spark_template = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }
    global_spark_path = conf_dir / "spark.yml"
    with open(global_spark_path, "w", encoding="utf-8") as stream:
        yaml.dump(global_spark_template, stream, default_flow_style=False, sort_keys=False)
    click.echo(f"📝 Seeded global baseline engine configurations (shared core): {global_spark_path}")

    # Step 2: Scaffold isolated environment folders and their strictly specific overrides
    for env in env_list:
        env_folder = envs_base_dir / env
        env_folder.mkdir(parents=True, exist_ok=True)

        # Build environment-isolated variables (No hardcoding)
        env_variables = {
            "lake_bucket": f"company-analytics-lakehouse-{env}",
            "db_host": "localhost" if env == "dev" else f"rds-cluster-{env}.internal.company.com",
            "db_name": f"analytics_{env}_db",
            "db_schema": "development" if env == "dev" else env,
            "db_user": "postgres" if env == "dev" else f"runtime_agent_{env}",
        }

        var_path = env_folder / "variables.yml"
        with open(var_path, "w", encoding="utf-8") as stream:
            yaml.dump(env_variables, stream, default_flow_style=False, sort_keys=False)
        click.echo(f"  📝 Seeded variables manifest for [{env}]: {var_path}")

        # Build STRICTLY environment-specific Spark parameters tuning overrides
        env_spark_template = {
            "spark.app.name": f"FlintPipeline-{env.upper()}",
            "spark.sql.shuffle.partitions": "8" if env == "dev" else "128",
        }

        if env == "dev":
            env_spark_template["# spark.remote"] = "sc://localhost:15002"

        env_spark_path = env_folder / "spark.yml"
        with open(env_spark_path, "w", encoding="utf-8") as stream:
            yaml.dump(env_spark_template, stream, default_flow_style=False, sort_keys=False)
        click.echo(f"  ⚡ Seeded isolated Spark configuration overrides tuning for [{env}]: {env_spark_path}")

    # Step 3: Construct and write a pure snippet template into the unified catalog space
    sample_pipeline_str = """# --- SECTION 1: REUSABLE CONTRACT SCHEMAS (YAML ANCHORS) ---
.relational_contract: &relational_contract
  engine: "pandas"
  format: "postgres"
  connector: "jdbc"
  columns:
    - name: "id"
      type: "integer"
      description: "Unique structural primary identifier ledger key."
    - name: "name"
      type: "string"
      description: "Business token label classification text name."

# --- SECTION 2: DATASET TOPOLOGIES (INTERPOLATED ENVIRONMENTS) ---
telemetry_ingestion:
  <<: *relational_contract
  storage_path: "${db_schema}.users_telemetry"
  options:
    url: "jdbc:postgresql://${db_host}:5432/${db_name}"
    user: "${db_user}"
    password: "${ENV_DATABASE_SECRET}"
"""
    sample_pipeline_path = catalog_dir / "formula1_telemetry.yaml"
    with open(sample_pipeline_path, "w", encoding="utf-8") as stream:
        stream.write(sample_pipeline_str)

    click.echo(f"📝 Seeding sample contract anchor interpolated configuration: {sample_pipeline_path}")
    click.echo("✨ Flint infrastructure architecture initialized with successful environment scaffolding maps!")

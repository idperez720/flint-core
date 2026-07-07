"""Command Line Interface (CLI) entry point for flint utilizing pluggable architectures."""

import sys
from pathlib import Path
from typing import ClassVar, Dict, List, Optional

import click

from flint_core.core.initializer import ProjectInitializationError, ProjectInitializer

# =============================================================================
# METAPROGRAMMING REGISTRY FOR CLI EXTENSIBILITY
# =============================================================================


class FlintCommandRegistry:
    """Thread-safe and pluggable registry to inject dynamic commands into the main CLI."""

    _custom_commands: ClassVar[Dict[str, click.Command]] = {}

    @classmethod
    def register_command(cls, command: click.Command) -> None:
        """Allows external core extensions or plug-ins to register custom subcommands.

        Args:
            command: A native Click Command object instance.
        """
        if command.name and command.name not in cls._custom_commands:
            cls._custom_commands[command.name] = command


class ExtensibleFlintGroup(click.Group):
    """Custom Click Group subclass leveraging dynamic metaprogramming lookup extensions."""

    def list_commands(self, ctx: click.Context) -> List[str]:
        """Amalgamates built-in baseline commands with externally registered plugins."""
        base_commands = super().list_commands(ctx)
        plugin_commands = list(FlintCommandRegistry._custom_commands.keys())
        return sorted(base_commands + plugin_commands)

    def get_command(self, ctx: click.Context, cmd_name: str) -> Optional[click.Command]:
        """Resolves target commands searching iteratively through standard registries."""
        base_cmd = super().get_command(ctx, cmd_name)
        if base_cmd is not None:
            return base_cmd

        return FlintCommandRegistry._custom_commands.get(cmd_name)


# =============================================================================
# ARCHITECTURAL PARADIGM LOOKUP MAP (UX SIMPLIFICATION)
# =============================================================================

PATTERN_MAP: Dict[str, str] = {
    "1": "default",
    "2": "medallion",
    "3": "kimball",
    "4": "datamart",
    "5": "datamesh",
    "6": "inmon",
    "7": "datavault",
}


# =============================================================================
# CORE CLI GROUP INTERFACE
# =============================================================================


@click.group(cls=ExtensibleFlintGroup)
def entry_point() -> None:
    """flint - Highly versatile and extensible Data Engineering Experience CLI utilities."""
    pass


# =============================================================================
# ENTERPRISE TRANSACTIONAL INITIALIZATION COMMAND
# =============================================================================


@entry_point.command(name="init")
@click.option(
    "--path",
    "-p",
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    default=".",
    help="Root destination directory where scaffolding conventions will be built.",
)
@click.option(
    "--name",
    "-n",
    type=str,
    help="Explicit project name designation. Disables prompt if supplied.",
)
@click.option(
    "--version",
    "-v",
    type=str,
    help="Initial semantic configuration version string placeholder parameters.",
)
@click.option(
    "--description",
    "-d",
    type=str,
    help="Short purpose statement describing the pipeline repository target.",
)
@click.option(
    "--author",
    "-a",
    type=str,
    help="Identity token alias marking structural ownership parameters fields.",
)
@click.option(
    "--envs",
    type=str,
    help="Target environments to configure as a comma-separated string list.",
)
@click.option(
    "--pattern",
    type=click.Choice(["1", "2", "3", "4", "5", "6", "7"]),
    help="Target structural architecture layout paradigm index selection (1-7).",
)
@click.option(
    "--no-input",
    is_flag=True,
    default=False,
    help="Deactivates interactive prompts. Forces headless automation.",
)
def init(
    path: str,
    name: Optional[str],
    version: Optional[str],
    description: Optional[str],
    author: Optional[str],
    envs: Optional[str],
    pattern: Optional[str],
    no_input: bool,
) -> None:
    """Scaffold a new production-ready data engineering project layout transactionally.

    Supports both highly intuitive interactive prompts setups and headless automation
    pipelines via direct flags execution.
    """
    click.secho(
        "🚀 Welcome to the elite flint project initialization wizard!\n",
        fg="cyan",
        bold=True,
    )

    resolved_path = Path(path).resolve()
    default_name = resolved_path.name if resolved_path.name not in (".", "") else "my-flint-project"

    if no_input:
        final_name = name if name else default_name
        final_version = version if version else "0.1.0"
        final_description = description if description else "Data engineering project scaffolded by flint"
        final_author = author if author else "Anonymous"
        final_envs_str = envs if envs else "dev,qa,prod"
        final_pattern_choice = pattern if pattern else "1"
        final_pattern = PATTERN_MAP[final_pattern_choice]
        final_domains_str = (
            "core_operations,intelligence_reporting" if final_pattern in ("datamart", "datamesh") else ""
        )
    else:
        final_name = (
            name
            if name
            else click.prompt(
                click.style("? Project name", fg="green", bold=True),
                default=default_name,
                type=str,
            )
        )
        final_version = (
            version
            if version
            else click.prompt(
                click.style("? Version", fg="green", bold=True),
                default="0.1.0",
                type=str,
            )
        )
        final_description = (
            description
            if description
            else click.prompt(
                click.style("? Description", fg="green", bold=True),
                default="Data engineering project scaffolded by flint",
                type=str,
            )
        )
        final_author = (
            author
            if author
            else click.prompt(
                click.style("? Author", fg="green", bold=True),
                default="Anonymous",
                type=str,
            )
        )
        final_envs_str = (
            envs
            if envs
            else click.prompt(
                click.style("? Target environments", fg="green", bold=True),
                default="dev,qa,prod",
                type=str,
            )
        )

        if not pattern:
            click.echo("\nSelect a structural architecture layout pattern:")
            click.echo("  1 - default    (Flat directory layout configuration)")
            click.echo("  2 - medallion  (Bronze, Silver, Gold layer split)")
            click.echo("  3 - kimball    (Staging, Dimensions, Facts split)")
            click.echo("  4 - datamart   (Domain isolated business marts)")
            click.echo("  5 - datamesh   (Decentralized domain data products)")
            click.echo("  6 - inmon      (Corporate data warehouse model)")
            click.echo("  7 - datavault  (Agile raw and business hub vaults)\n")
            final_pattern_choice = click.prompt(
                click.style("? Architecture pattern", fg="green", bold=True),
                type=click.Choice(["1", "2", "3", "4", "5", "6", "7"]),
                default="1",
            )
        else:
            final_pattern_choice = pattern

        final_pattern = PATTERN_MAP[final_pattern_choice]

        if final_pattern in ("datamart", "datamesh"):
            final_domains_str = click.prompt(
                click.style("? Target business domains", fg="green", bold=True),
                default="core_operations,intelligence_reporting",
                type=str,
            )
        else:
            final_domains_str = ""

    parsed_envs = [e.strip().lower() for e in final_envs_str.split(",") if e.strip()]
    parsed_domains = [d.strip().lower() for d in final_domains_str.split(",") if d.strip()]

    try:
        initializer = ProjectInitializer(base_path=resolved_path)
        initializer.init_project(
            name=final_name,
            version=final_version,
            description=final_description,
            author=final_author,
            envs=parsed_envs,
            pattern=final_pattern,
            domains=parsed_domains,
        )
        click.secho(
            f"\n✨ Project successfully initialized at '{resolved_path}'!",
            fg="green",
            bold=True,
        )
    except FileExistsError as error:
        click.secho(
            f"\n🚨 Architectural Collision Abort: {error}",
            fg="yellow",
            bold=True,
            err=True,
        )
        sys.exit(1)
    except ProjectInitializationError as error:
        click.secho(
            f"\n💥 Critical Scaffolding Transaction Failure: {error}",
            fg="red",
            bold=True,
            err=True,
        )
        sys.exit(1)
    except Exception as unexpected_error:
        click.secho(
            f"\n☣️ Unhandled System Exception Intercepted: {unexpected_error}",
            fg="red",
            err=True,
        )
        sys.exit(2)

"""dcx load - Load files into Snowflake."""

from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console

from dcx.core.loader import FileLoader, SchemaNotFoundError
from dcx.core.settings import get_connection, get_profile

console = Console()


class Strategy(str, Enum):
    overwrite = "overwrite"  # Delete matching tags, then insert
    append = "append"  # Just insert (keep history)
    replace = "replace"  # Truncate table, then insert


class Format(str, Enum):
    auto = "auto"  # Auto-detect from extension
    single_column = "single-column"  # One column per line (UCOP style)
    csv = "csv"
    tsv = "tsv"


def load(
    source: Annotated[
        Path,
        typer.Argument(
            help="File, folder, or zip to load",
            exists=True,
            resolve_path=True,
        ),
    ],
    dest: Annotated[
        Optional[str],
        typer.Option("--dest", "-d", help="Destination table (schema.table or just table)"),
    ] = None,
    profile: Annotated[
        Optional[str],
        typer.Option("--profile", "-p", help="Load profile name (from dcx config profile)"),
    ] = None,
    tag: Annotated[
        Optional[list[str]],
        typer.Option(
            "--tag", "-t",
            help="Metadata tag as key=value (can specify multiple)",
        ),
    ] = None,
    strategy: Annotated[
        Strategy,
        typer.Option("--strategy", "-s", help="Load strategy"),
    ] = Strategy.overwrite,
    format: Annotated[
        Format,
        typer.Option("--format", "-f", help="File format"),
    ] = Format.auto,
    skip_header: Annotated[
        int,
        typer.Option("--skip-header", help="Number of header lines to skip"),
    ] = 0,
    connection: Annotated[
        Optional[str],
        typer.Option("--connection", "-c", help="Connection name from config"),
    ] = None,
    create_table: Annotated[
        bool,
        typer.Option("--create-table", help="Create table if it doesn't exist"),
    ] = True,
    create_schema: Annotated[
        bool,
        typer.Option("--create-schema", help="Create schema if it doesn't exist (skips prompt)"),
    ] = False,
    grant: Annotated[
        Optional[list[str]],
        typer.Option(
            "--grant", "-g",
            help="Grant SELECT to role (can specify multiple)",
        ),
    ] = None,
    most_recent: Annotated[
        bool,
        typer.Option("--most-recent", help="Track most recent load with boolean column"),
    ] = False,
    audit: Annotated[
        bool,
        typer.Option("--audit", help="Log load to _dcx_load_history table"),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show what would be done without executing"),
    ] = False,
):
    """
    Load files into Snowflake.

    Examples:

        dcx load ./data.zip --dest my_table

        dcx load ./CENSUS_2258.zip --dest ucop_file_loads \\
            --tag extract_type=CENSUS --tag term_code=2258

        dcx load ./data.csv --dest raw.imports --strategy append

        dcx load ./data.zip --profile ucop-census --tag term_code=2258
    """
    # Load profile and merge settings (CLI takes precedence)
    profile_config = {}
    if profile:
        profile_config = get_profile(profile)
        if not profile_config:
            console.print(f"[red]Profile not found: {profile}[/red]")
            raise typer.Exit(1)
        console.print(f"[dim]Using profile: {profile}[/dim]")

    # Merge dest from profile (CLI takes precedence)
    final_dest = dest or profile_config.get("dest")
    if not final_dest:
        console.print("[red]No destination specified. Use --dest or --profile[/red]")
        raise typer.Exit(1)

    # Merge connection from profile
    final_connection = connection or profile_config.get("connection")

    # Merge strategy from profile
    final_strategy = strategy
    if profile_config.get("strategy") and strategy == Strategy.overwrite:  # default value
        final_strategy = Strategy(profile_config["strategy"])

    # Merge most_recent from profile
    final_most_recent = most_recent or profile_config.get("most_recent", False)

    # Merge grants from profile
    final_grants = grant or profile_config.get("grants")

    # Parse tags - start with profile tags, then override with CLI tags
    tags = dict(profile_config.get("tags", {}))
    if tag:
        for t in tag:
            if "=" not in t:
                console.print(f"[red]Invalid tag format: {t} (expected key=value)[/red]")
                raise typer.Exit(1)
            key, value = t.split("=", 1)
            tags[key] = value

    # Get connection
    conn_config = get_connection(final_connection)
    if not conn_config:
        console.print("[red]No connection configured. Run: dcx config add[/red]")
        raise typer.Exit(1)

    # Show plan
    console.print(f"\n[bold]Source:[/bold] {source}")
    console.print(f"[bold]Destination:[/bold] {final_dest}")
    console.print(f"[bold]Connection:[/bold] {conn_config.get('account', 'default')}")
    console.print(f"[bold]Strategy:[/bold] {final_strategy.value}")
    if tags:
        console.print(f"[bold]Tags:[/bold] {tags}")
    console.print()

    if dry_run:
        console.print("[yellow]Dry run - no changes made[/yellow]")
        return

    # Execute load
    loader = FileLoader(
        connection=conn_config,
        dest_table=final_dest,
        tags=tags,
        strategy=final_strategy.value,
        file_format=format.value,
        create_table=create_table,
        create_schema=create_schema,
        grants=final_grants,
        track_most_recent=final_most_recent,
        skip_header=skip_header,
        audit=audit,
    )

    try:
        result = loader.load(source)
        console.print(f"\n[green]Loaded {result['rows']:,} rows from {result['files']} file(s)[/green]")
        if result.get("deleted"):
            console.print(f"[dim](deleted {result['deleted']:,} existing rows)[/dim]")
    except SchemaNotFoundError as e:
        console.print(f"\n[yellow]Schema '{e.schema}' does not exist.[/yellow]")
        if typer.confirm(f"Create schema '{e.schema}'?", default=True):
            # Retry with create_schema=True
            loader = FileLoader(
                connection=conn_config,
                dest_table=final_dest,
                tags=tags,
                strategy=final_strategy.value,
                file_format=format.value,
                create_table=create_table,
                create_schema=True,
                grants=final_grants,
                track_most_recent=final_most_recent,
                skip_header=skip_header,
                audit=audit,
            )
            result = loader.load(source)
            console.print(f"\n[green]Loaded {result['rows']:,} rows from {result['files']} file(s)[/green]")
            if result.get("deleted"):
                console.print(f"[dim](deleted {result['deleted']:,} existing rows)[/dim]")
        else:
            raise typer.Exit(1)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        raise typer.Exit(1)

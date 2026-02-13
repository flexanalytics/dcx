"""dcx load - Load files into Snowflake."""

from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console

from dcx.commands.config import get_dbt_project_profile
from dcx.core.loader import FileLoader, SchemaNotFoundError
from dcx.core.settings import get_connection, get_profile

console = Console()


class Strategy(str, Enum):
    overwrite = "overwrite"  # Delete matching tags, then insert
    append = "append"  # Just insert (keep history)
    truncate = "truncate"  # Truncate table, then insert (fast, keeps structure)
    replace = "replace"  # Drop and recreate table (allows schema changes)


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
        typer.Option("--dest", "-d", help="Destination table name (database/schema come from connection)"),
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
    single_column: Annotated[
        bool,
        typer.Option("--single-column", help="Store CSV as single JSON column instead of expanding"),
    ] = False,
    sanitize: Annotated[
        bool,
        typer.Option("--sanitize", help="Sanitize column names (spaces→underscores, uppercase)"),
    ] = False,
    audit: Annotated[
        bool,
        typer.Option("--audit", help="Log load to _dcx_load_history table"),
    ] = False,
    include: Annotated[
        Optional[list[str]],
        typer.Option("--include", "-i", help="Only include files with these extensions (e.g., --include txt)"),
    ] = None,
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

    # Get connection - prioritize dbt_project.yml if no explicit --connection
    conn_config = None
    conn_display = "default"

    if final_connection:
        # Explicit --connection provided
        conn_config = get_connection(final_connection)
        conn_display = final_connection
        if not conn_config:
            console.print(f"[red]Connection '{final_connection}' not found. Run: dcx config list[/red]")
            raise typer.Exit(1)
    else:
        # No explicit connection - check for dbt_project.yml first
        dbt_project = get_dbt_project_profile()
        if dbt_project:
            profile_name, target_name, dbt_config = dbt_project
            console.print(f"\n[cyan]Found dbt_project.yml using profile '{profile_name}' (target: {target_name})[/cyan]")
            console.print(f"[dim]  account: {dbt_config.get('account')}[/dim]")
            console.print(f"[dim]  database: {dbt_config.get('database')}[/dim]")
            console.print(f"[dim]  warehouse: {dbt_config.get('warehouse')}[/dim]")
            if not typer.confirm("\nUse this connection?", default=True):
                # Offer to use a different connection
                from dcx.core.settings import load_config
                config = load_config()
                connections = list(config.get("connections", {}).keys())

                console.print("\n[bold]Available connections:[/bold]")
                connections_config = config.get("connections", {})
                for i, name in enumerate(connections, 1):
                    conn = connections_config.get(name, {})
                    account = conn.get("account", "?")
                    database = conn.get("database", "?")
                    schema = conn.get("schema", "?")
                    console.print(f"  {i}. [cyan]{name}[/cyan]")
                    console.print(f"     {account} → {database}.{schema}")
                console.print(f"  {len(connections) + 1}. [green]Create new connection[/green]")
                console.print(f"  {len(connections) + 2}. [yellow]Cancel[/yellow]")

                choice = typer.prompt("\nSelect", type=int, default=len(connections) + 2)

                if choice == len(connections) + 1:
                    # Create new connection inline
                    from dcx.commands.config import add as config_add
                    conn_name = typer.prompt("\nConnection name")
                    config_add(name=conn_name)
                    conn_config = get_connection(conn_name)
                    conn_display = conn_name
                elif choice < 1 or choice > len(connections):
                    console.print("[yellow]Aborted.[/yellow]")
                    raise typer.Exit(1)
                else:
                    conn_name = connections[choice - 1]
                    conn_config = get_connection(conn_name)
                    conn_display = conn_name
            else:
                conn_config = dbt_config
                conn_display = f"dbt:{profile_name}.{target_name}"

        if not conn_config:
            # Fall back to default connection
            conn_config = get_connection(None)
            if not conn_config:
                console.print("[red]No connection configured. Run: dcx config add[/red]")
                raise typer.Exit(1)

    # Parse destination - connection provides defaults, but dest can override
    conn_db = conn_config.get("database", "")
    conn_schema = conn_config.get("schema", "")

    dest_parts = final_dest.split(".")
    if len(dest_parts) == 1:
        # Just table name - use connection's db/schema
        table_name = final_dest
        db = conn_db
        schema = conn_schema
    elif len(dest_parts) == 2:
        # schema.table - confirm override
        dest_schema, table_name = dest_parts
        if dest_schema.upper() != conn_schema.upper():
            console.print(f"\n[yellow]Destination specifies schema '{dest_schema}' but connection uses '{conn_schema}'[/yellow]")
            if typer.confirm(f"Use schema '{dest_schema}' instead?", default=True):
                schema = dest_schema
            else:
                schema = conn_schema
        else:
            schema = dest_schema
        db = conn_db
    else:
        # db.schema.table - confirm override
        dest_db = dest_parts[0]
        dest_schema = dest_parts[-2]
        table_name = dest_parts[-1]

        overrides = []
        if dest_db.upper() != conn_db.upper():
            overrides.append(f"database '{dest_db}' (connection: '{conn_db}')")
        if dest_schema.upper() != conn_schema.upper():
            overrides.append(f"schema '{dest_schema}' (connection: '{conn_schema}')")

        if overrides:
            console.print(f"\n[yellow]Destination specifies: {', '.join(overrides)}[/yellow]")
            if typer.confirm("Use destination's database/schema?", default=True):
                db = dest_db
                schema = dest_schema
            else:
                db = conn_db
                schema = conn_schema
        else:
            db = dest_db
            schema = dest_schema

    # Build full path for display
    full_dest = ".".join(filter(None, [db, schema, table_name]))

    # Update connection config with resolved db/schema (may have been overridden by dest)
    conn_config = {**conn_config, "database": db, "schema": schema}

    # Show plan
    console.print(f"\n[bold]Source:[/bold] {source}")
    console.print(f"[bold]Destination:[/bold] {full_dest}")
    console.print(f"[bold]Connection:[/bold] {conn_display} ({conn_config.get('account')})")
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
        dest_table=table_name,
        tags=tags,
        strategy=final_strategy.value,
        file_format=format.value,
        create_table=create_table,
        create_schema=create_schema,
        grants=final_grants,
        track_most_recent=final_most_recent,
        skip_header=skip_header,
        expand_columns=not single_column,
        audit=audit,
        sanitize_columns=sanitize,
        include_extensions=include,
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
                dest_table=table_name,
                tags=tags,
                strategy=final_strategy.value,
                file_format=format.value,
                create_table=create_table,
                create_schema=True,
                grants=final_grants,
                track_most_recent=final_most_recent,
                skip_header=skip_header,
                expand_columns=not single_column,
                audit=audit,
                sanitize_columns=sanitize,
                include_extensions=include,
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

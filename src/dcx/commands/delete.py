"""dcx delete - Delete data by tags."""

from typing import Annotated, Optional

import typer
from rich.console import Console

from dcx.core.settings import get_connection
from dcx.core.snowflake import get_snowflake_connection

console = Console()


def delete_data(
    table: Annotated[
        str,
        typer.Argument(help="Table to delete from"),
    ],
    tag: Annotated[
        Optional[list[str]],
        typer.Option(
            "--tag", "-t",
            help="Filter by tag as key=value (can specify multiple)",
        ),
    ] = None,
    connection: Annotated[
        Optional[str],
        typer.Option("--connection", "-c", help="Connection name from config"),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip confirmation prompt"),
    ] = False,
    all_data: Annotated[
        bool,
        typer.Option("--all", help="Delete all data (requires --force)"),
    ] = False,
):
    """
    Delete data from a table by tags.

    Examples:
        dcx delete ucop_file_loads --tag extract_type=CENSUS --tag term_code=2258
        dcx delete ucop_file_loads --all --force
    """
    # Parse tags
    tags = {}
    if tag:
        for t in tag:
            if "=" not in t:
                console.print(f"[red]Invalid tag format: {t} (expected key=value)[/red]")
                raise typer.Exit(1)
            key, value = t.split("=", 1)
            tags[key] = value

    if not tags and not all_data:
        console.print("[red]Must specify --tag or --all[/red]")
        raise typer.Exit(1)

    if all_data and not force:
        console.print("[red]--all requires --force to prevent accidental deletion[/red]")
        raise typer.Exit(1)

    conn_config = get_connection(connection)
    if not conn_config:
        console.print("[red]No connection configured. Run: dcx config add[/red]")
        raise typer.Exit(1)

    conn = get_snowflake_connection(conn_config)

    try:
        cursor = conn.cursor()

        # Build WHERE clause
        if all_data:
            where_clause = "1=1"
            params = ()
        else:
            conditions = " AND ".join(f"{k} = %s" for k in tags.keys())
            where_clause = conditions
            params = tuple(tags.values())

        # Get count
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_clause}", params)
        count = cursor.fetchone()[0]

        if count == 0:
            console.print("[yellow]No matching rows found[/yellow]")
            return

        # Confirm
        console.print(f"\n[bold]Will delete {count:,} rows from {table}[/bold]")
        if tags:
            console.print(f"[dim]Where: {tags}[/dim]")

        if not force:
            if not typer.confirm("Proceed?", default=False):
                console.print("[yellow]Cancelled[/yellow]")
                raise typer.Exit(0)

        # Delete
        cursor.execute(f"DELETE FROM {table} WHERE {where_clause}", params)
        console.print(f"[green]Deleted {count:,} rows[/green]")

    finally:
        conn.close()

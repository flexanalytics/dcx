"""dcx list - Show loaded data summary."""

from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from dcx.core.settings import get_connection
from dcx.core.snowflake import get_snowflake_connection

console = Console()


def list_data(
    table: Annotated[
        str,
        typer.Argument(help="Table to inspect"),
    ],
    connection: Annotated[
        Optional[str],
        typer.Option("--connection", "-c", help="Connection name from config"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-n", help="Max rows to show"),
    ] = 50,
):
    """
    Show summary of loaded data grouped by tags.

    Example:
        dcx list ucop_file_loads
    """
    conn_config = get_connection(connection)
    if not conn_config:
        console.print("[red]No connection configured. Run: dcx config add[/red]")
        raise typer.Exit(1)

    conn = get_snowflake_connection(conn_config)

    try:
        cursor = conn.cursor()

        # First, get column names to find tag columns
        cursor.execute(f"DESCRIBE TABLE {table}")
        columns = [row[0] for row in cursor.fetchall()]

        # Identify tag columns (exclude system columns)
        system_cols = {"_SOURCE_FILE", "_LOAD_TIMESTAMP", "IS_MOST_RECENT", "DATA"}
        tag_cols = [c for c in columns if c.upper() not in system_cols]

        if not tag_cols:
            # No tag columns - just show basic stats
            cursor.execute(f"""
                SELECT
                    COUNT(*) as row_count,
                    COUNT(DISTINCT _source_file) as file_count,
                    MIN(_load_timestamp) as first_load,
                    MAX(_load_timestamp) as last_load
                FROM {table}
            """)
            row = cursor.fetchone()
            console.print(f"\n[bold]{table}[/bold]")
            console.print(f"  Rows: {row[0]:,}")
            console.print(f"  Files: {row[1]:,}")
            console.print(f"  First load: {row[2]}")
            console.print(f"  Last load: {row[3]}")
            return

        # Build query to group by tag columns
        tag_list = ", ".join(tag_cols)

        # Check if is_most_recent column exists
        has_most_recent = "IS_MOST_RECENT" in [c.upper() for c in columns]

        most_recent_col = ", MAX(is_most_recent::INT) as is_current" if has_most_recent else ""

        query = f"""
            SELECT
                {tag_list},
                COUNT(*) as row_count,
                COUNT(DISTINCT _source_file) as file_count,
                MIN(_load_timestamp) as first_load,
                MAX(_load_timestamp) as last_load
                {most_recent_col}
            FROM {table}
            GROUP BY {tag_list}
            ORDER BY last_load DESC
            LIMIT {limit}
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            console.print(f"[yellow]No data in {table}[/yellow]")
            return

        # Build table
        result_table = Table(title=f"Data in {table}")
        for col in tag_cols:
            result_table.add_column(col, style="cyan")
        result_table.add_column("Rows", justify="right")
        result_table.add_column("Files", justify="right")
        result_table.add_column("Last Load")
        if has_most_recent:
            result_table.add_column("Current", justify="center")

        for row in rows:
            values = list(row)
            tag_values = [str(v) for v in values[:len(tag_cols)]]
            row_count = f"{values[len(tag_cols)]:,}"
            file_count = str(values[len(tag_cols) + 1])
            last_load = str(values[len(tag_cols) + 3])[:19]  # Trim microseconds

            if has_most_recent:
                is_current = "✓" if values[-1] else ""
                result_table.add_row(*tag_values, row_count, file_count, last_load, is_current)
            else:
                result_table.add_row(*tag_values, row_count, file_count, last_load)

        console.print(result_table)

    finally:
        conn.close()

"""dcx info - Show table information."""

from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from dcx.core.settings import get_connection
from dcx.core.snowflake import get_snowflake_connection

console = Console()


def info(
    table: Annotated[
        str,
        typer.Argument(help="Table to inspect"),
    ],
    connection: Annotated[
        Optional[str],
        typer.Option("--connection", "-c", help="Connection name from config"),
    ] = None,
):
    """
    Show table schema and statistics.

    Example:
        dcx info ucop_file_loads
    """
    conn_config = get_connection(connection)
    if not conn_config:
        console.print("[red]No connection configured. Run: dcx config add[/red]")
        raise typer.Exit(1)

    conn = get_snowflake_connection(conn_config)

    try:
        cursor = conn.cursor()

        # Get schema
        console.print(f"\n[bold]Schema: {table}[/bold]\n")
        cursor.execute(f"DESCRIBE TABLE {table}")
        columns = cursor.fetchall()

        schema_table = Table()
        schema_table.add_column("Column", style="cyan")
        schema_table.add_column("Type")
        schema_table.add_column("Nullable")
        schema_table.add_column("Default")

        for col in columns:
            name, dtype, kind, nullable, default, *_ = col
            schema_table.add_row(
                name,
                dtype,
                "✓" if nullable == "Y" else "",
                str(default) if default else "",
            )

        console.print(schema_table)

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cursor.fetchone()[0]
        console.print(f"\n[bold]Total rows:[/bold] {total_rows:,}")

        # Get distinct files
        try:
            cursor.execute(f"SELECT COUNT(DISTINCT _source_file) FROM {table}")
            file_count = cursor.fetchone()[0]
            console.print(f"[bold]Distinct files:[/bold] {file_count:,}")
        except Exception:
            pass

        # Get load timestamp range
        try:
            cursor.execute(f"""
                SELECT MIN(_load_timestamp), MAX(_load_timestamp)
                FROM {table}
            """)
            min_ts, max_ts = cursor.fetchone()
            if min_ts:
                console.print(f"[bold]First load:[/bold] {str(min_ts)[:19]}")
                console.print(f"[bold]Last load:[/bold] {str(max_ts)[:19]}")
        except Exception:
            pass

        # Get most recent stats if column exists
        col_names = [c[0].upper() for c in columns]
        if "IS_MOST_RECENT" in col_names:
            cursor.execute(f"""
                SELECT is_most_recent, COUNT(*)
                FROM {table}
                GROUP BY is_most_recent
            """)
            rows = cursor.fetchall()
            console.print("\n[bold]Most recent tracking:[/bold]")
            for is_recent, count in rows:
                label = "Current" if is_recent else "Historical"
                console.print(f"  {label}: {count:,} rows")

        # Get sample of tag values
        system_cols = {"_SOURCE_FILE", "_LOAD_TIMESTAMP", "IS_MOST_RECENT", "DATA"}
        tag_cols = [c[0] for c in columns if c[0].upper() not in system_cols]

        if tag_cols:
            console.print("\n[bold]Tag columns:[/bold]")
            for col in tag_cols:
                cursor.execute(f"SELECT DISTINCT {col} FROM {table} LIMIT 10")
                values = [str(row[0]) for row in cursor.fetchall()]
                console.print(f"  {col}: {', '.join(values)}")

    finally:
        conn.close()

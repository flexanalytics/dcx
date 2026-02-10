"""dcx validate - Validate files before loading."""

import zipfile
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

console = Console()

# Snowflake VARCHAR limit
MAX_LINE_LENGTH = 16_777_216  # 16MB


def validate(
    source: Annotated[
        Path,
        typer.Argument(
            help="File, folder, or zip to validate",
            exists=True,
            resolve_path=True,
        ),
    ],
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Show detailed file info"),
    ] = False,
):
    """
    Validate files before loading.

    Checks:
    - File can be read
    - No lines exceed Snowflake VARCHAR limit (16MB)
    - Encoding is valid UTF-8

    Example:
        dcx validate ./CENSUS_2258.zip
    """
    files = list(_iter_files(source))

    if not files:
        console.print("[yellow]No files found[/yellow]")
        raise typer.Exit(1)

    console.print(f"\nValidating {len(files)} file(s)...\n")

    results = []
    all_valid = True

    for file_path, file_name in files:
        result = _validate_file(file_path, file_name)
        results.append(result)
        if not result["valid"]:
            all_valid = False

    # Display results
    table = Table(title="Validation Results")
    table.add_column("File", style="cyan")
    table.add_column("Lines", justify="right")
    table.add_column("Max Line", justify="right")
    table.add_column("Status")

    for r in results:
        status = "[green]✓ Valid[/green]" if r["valid"] else f"[red]✗ {r['error']}[/red]"
        max_line = f"{r['max_line_length']:,}" if r["max_line_length"] else "-"

        table.add_row(
            r["file_name"],
            f"{r['line_count']:,}" if r["line_count"] else "-",
            max_line,
            status,
        )

    console.print(table)

    if verbose:
        console.print("\n[bold]Details:[/bold]")
        for r in results:
            console.print(f"\n  {r['file_name']}:")
            console.print(f"    Size: {r['size']:,} bytes")
            if r["line_count"]:
                console.print(f"    Lines: {r['line_count']:,}")
                console.print(f"    Avg line length: {r['avg_line_length']:,.0f}")
                console.print(f"    Max line length: {r['max_line_length']:,}")
            if r["error"]:
                console.print(f"    [red]Error: {r['error']}[/red]")

    if all_valid:
        console.print("\n[green]All files valid[/green]")
    else:
        console.print("\n[red]Validation failed[/red]")
        raise typer.Exit(1)


def _iter_files(source: Path) -> list[tuple[Path, str]]:
    """Get list of files to validate."""
    import tempfile

    files = []

    if source.is_file():
        if source.suffix.lower() == ".zip":
            temp_dir = tempfile.mkdtemp()
            temp_path = Path(temp_dir)
            with zipfile.ZipFile(source) as zf:
                zf.extractall(temp_path)

            for file_path in sorted(temp_path.rglob("*")):
                if file_path.is_file() and not file_path.name.startswith("."):
                    files.append((file_path, file_path.name))
        else:
            files.append((source, source.name))

    elif source.is_dir():
        for file_path in sorted(source.rglob("*")):
            if file_path.is_file() and not file_path.name.startswith("."):
                files.append((file_path, file_path.name))

    return files


def _validate_file(file_path: Path, file_name: str) -> dict:
    """Validate a single file."""
    result = {
        "file_name": file_name,
        "valid": True,
        "error": None,
        "size": file_path.stat().st_size,
        "line_count": 0,
        "max_line_length": 0,
        "avg_line_length": 0,
    }

    try:
        total_length = 0
        line_count = 0
        max_length = 0

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line_count += 1
                length = len(line.encode("utf-8"))
                total_length += length
                max_length = max(max_length, length)

                if length > MAX_LINE_LENGTH:
                    result["valid"] = False
                    result["error"] = f"Line {line_count} exceeds 16MB ({length:,} bytes)"
                    break

        result["line_count"] = line_count
        result["max_line_length"] = max_length
        result["avg_line_length"] = total_length / line_count if line_count else 0

    except UnicodeDecodeError as e:
        result["valid"] = False
        result["error"] = f"Encoding error: {e}"
    except Exception as e:
        result["valid"] = False
        result["error"] = str(e)

    return result

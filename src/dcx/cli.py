"""DCX CLI entrypoint."""

import typer

from dcx.commands import config
from dcx.commands.load import load
from dcx.commands.list import list_data
from dcx.commands.delete import delete_data
from dcx.commands.info import info
from dcx.commands.validate import validate

app = typer.Typer(
    name="dcx",
    help="DataCampus CLI - Data loading and authentication tools",
    no_args_is_help=True,
)

# Register commands
app.command("load")(load)
app.command("list")(list_data)
app.command("delete")(delete_data)
app.command("info")(info)
app.command("validate")(validate)

# Register config as a command group
app.add_typer(config.app, name="config", help="Manage connections and settings")


@app.command()
def version():
    """Show dcx version."""
    from dcx import __version__

    typer.echo(f"dcx {__version__}")


if __name__ == "__main__":
    app()

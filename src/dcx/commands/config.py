"""dcx config - Manage connections and settings."""

import os
from pathlib import Path
from typing import Annotated, Optional

import typer
import yaml
from rich.console import Console
from rich.table import Table

from dcx.core.settings import (
    add_connection,
    add_profile,
    get_config_path,
    get_profile,
    list_connections,
    list_profiles,
    remove_connection,
    remove_profile,
    set_default_connection,
    test_connection,
)

app = typer.Typer(no_args_is_help=True)
console = Console()


def _get_dbt_profiles_path() -> Path | None:
    """Get path to dbt profiles.yml."""
    # Check DBT_PROFILES_DIR env var first
    profiles_dir = os.environ.get("DBT_PROFILES_DIR")
    if profiles_dir:
        path = Path(profiles_dir) / "profiles.yml"
        if path.exists():
            return path

    # Default location
    default_path = Path.home() / ".dbt" / "profiles.yml"
    if default_path.exists():
        return default_path

    return None


def _load_dbt_profiles() -> dict | None:
    """Load and parse dbt profiles.yml."""
    path = _get_dbt_profiles_path()
    if not path:
        return None

    with open(path) as f:
        return yaml.safe_load(f)


def _get_dbt_snowflake_targets(profiles: dict) -> list[tuple[str, str, dict]]:
    """
    Extract Snowflake targets from dbt profiles.
    Returns list of (profile_name, target_name, config).
    """
    targets = []
    for profile_name, profile in profiles.items():
        if not isinstance(profile, dict) or "outputs" not in profile:
            continue
        for target_name, target_config in profile.get("outputs", {}).items():
            if isinstance(target_config, dict) and target_config.get("type") == "snowflake":
                targets.append((profile_name, target_name, target_config))
    return targets


def _dbt_config_to_dcx(dbt_config: dict) -> dict:
    """Convert dbt Snowflake config to dcx format."""
    config = {
        "account": dbt_config.get("account", ""),
        "database": dbt_config.get("database", ""),
        "warehouse": dbt_config.get("warehouse", ""),
    }

    if dbt_config.get("user"):
        config["user"] = dbt_config["user"]
    if dbt_config.get("role"):
        config["role"] = dbt_config["role"]
    if dbt_config.get("schema"):
        config["schema"] = dbt_config["schema"]

    # Handle authentication
    if dbt_config.get("authenticator"):
        config["authenticator"] = dbt_config["authenticator"]
    elif dbt_config.get("private_key_path"):
        config["authenticator"] = "snowflake_jwt"
        config["private_key_path"] = dbt_config["private_key_path"]
    elif dbt_config.get("password"):
        config["authenticator"] = "snowflake"
        # Note: we don't store passwords - user will need to re-auth
    else:
        config["authenticator"] = "externalbrowser"

    return config


def _prompt_for_edits(config: dict) -> dict:
    """Prompt user to edit config values. Press enter to keep current value."""
    console.print("\n[dim]Press Enter to keep current value, or type new value:[/dim]\n")

    editable_fields = [
        ("account", "Account"),
        ("user", "User"),
        ("database", "Database"),
        ("warehouse", "Warehouse"),
        ("schema", "Schema"),
        ("role", "Role"),
        ("authenticator", "Authenticator"),
        ("private_key_path", "Private key path"),
    ]

    updated = {}
    for key, label in editable_fields:
        current = config.get(key, "")
        if current or key in ("account", "database", "warehouse"):
            # Show prompt with current value
            new_value = typer.prompt(
                f"  {label}",
                default=current,
                show_default=True,
            )
            if new_value:  # Only include non-empty values
                updated[key] = new_value

    return updated


@app.command("add")
def add(
    name: Annotated[str, typer.Argument(help="Connection name")],
    account: Annotated[str, typer.Option("--account", "-a", help="Snowflake account")] = None,
    database: Annotated[str, typer.Option("--database", "-d", help="Default database")] = None,
    warehouse: Annotated[str, typer.Option("--warehouse", "-w", help="Default warehouse")] = None,
    role: Annotated[str, typer.Option("--role", "-r", help="Default role")] = None,
    schema: Annotated[str, typer.Option("--schema", "-s", help="Default schema")] = None,
    authenticator: Annotated[
        str,
        typer.Option("--authenticator", help="Auth method (externalbrowser, snowflake, etc.)"),
    ] = None,
    from_dbt: Annotated[
        bool,
        typer.Option("--from-dbt", help="Import from dbt profiles.yml"),
    ] = False,
    set_default: Annotated[
        bool,
        typer.Option("--default", help="Set as default connection"),
    ] = False,
):
    """Add a new Snowflake connection."""
    config = None

    # Check for dbt profiles if --from-dbt or no options provided
    if from_dbt or (not account and not database and not warehouse):
        profiles = _load_dbt_profiles()
        if profiles:
            targets = _get_dbt_snowflake_targets(profiles)
            if targets:
                # Show available targets
                console.print("\n[bold]Available dbt Snowflake profiles:[/bold]")
                for i, (profile, target, _) in enumerate(targets, 1):
                    console.print(f"  {i}. {profile}.{target}")
                console.print(f"  {len(targets) + 1}. [dim]Enter manually[/dim]")
                console.print()

                choice = typer.prompt(
                    "Select profile",
                    default="1" if len(targets) == 1 else None,
                )

                try:
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(targets):
                        profile_name, target_name, dbt_config = targets[choice_idx]
                        config = _dbt_config_to_dcx(dbt_config)

                        # Show config for confirmation
                        console.print(f"\n[bold]Importing from {profile_name}.{target_name}:[/bold]")
                        for key, value in config.items():
                            console.print(f"  {key}: {value}")
                        console.print()

                        # Offer to modify
                        if typer.confirm("Would you like to make changes to any of these?", default=False):
                            config = _prompt_for_edits(config)
                            console.print()

                        if not typer.confirm("Save this configuration?", default=True):
                            config = None
                except (ValueError, IndexError):
                    pass

    # Manual entry if no dbt config used
    if config is None:
        if not account:
            account = typer.prompt("Snowflake account (e.g., abc12345.us-east-1)")
        if not database:
            database = typer.prompt("Default database")
        if not warehouse:
            warehouse = typer.prompt("Default warehouse")

        config = {
            "account": account,
            "database": database,
            "warehouse": warehouse,
            "authenticator": authenticator or "externalbrowser",
        }
        if role:
            config["role"] = role
        if schema:
            config["schema"] = schema

    add_connection(name, config, set_as_default=set_default)
    console.print(f"\n[green]Added connection: {name}[/green]")

    if set_default:
        console.print("[dim]Set as default connection[/dim]")


@app.command("list")
def list_cmd():
    """List configured connections."""
    connections = list_connections()

    if not connections["connections"]:
        console.print("[yellow]No connections configured. Run: dcx config add <name>[/yellow]")
        return

    table = Table(title="Connections")
    table.add_column("Name", style="cyan")
    table.add_column("Account")
    table.add_column("Database")
    table.add_column("Warehouse")
    table.add_column("Default", style="green")

    default = connections.get("default")
    for name, cfg in connections["connections"].items():
        table.add_row(
            name,
            cfg.get("account", ""),
            cfg.get("database", ""),
            cfg.get("warehouse", ""),
            "" if name == default else "",
        )

    console.print(table)


@app.command("remove")
def remove(
    name: Annotated[str, typer.Argument(help="Connection name to remove")],
):
    """Remove a connection."""
    if remove_connection(name):
        console.print(f"[green]Removed connection: {name}[/green]")
    else:
        console.print(f"[red]Connection not found: {name}[/red]")
        raise typer.Exit(1)


@app.command("default")
def set_default(
    name: Annotated[str, typer.Argument(help="Connection name to set as default")],
):
    """Set the default connection."""
    if set_default_connection(name):
        console.print(f"[green]Default connection set to: {name}[/green]")
    else:
        console.print(f"[red]Connection not found: {name}[/red]")
        raise typer.Exit(1)


@app.command("test")
def test(
    name: Annotated[
        Optional[str],
        typer.Argument(help="Connection name (uses default if not specified)"),
    ] = None,
):
    """Test a connection."""
    console.print(f"Testing connection: {name or 'default'}...")

    success, message = test_connection(name)
    if success:
        console.print(f"[green]{message}[/green]")
    else:
        console.print(f"[red]{message}[/red]")
        raise typer.Exit(1)


@app.command("path")
def path():
    """Show config file location."""
    console.print(get_config_path())


# ============================================================================
# Profile commands
# ============================================================================

profile_app = typer.Typer(no_args_is_help=True)
app.add_typer(profile_app, name="profile", help="Manage load profiles")


@profile_app.command("add")
def profile_add(
    name: Annotated[str, typer.Argument(help="Profile name")],
    dest: Annotated[str, typer.Option("--dest", "-d", help="Default destination table")] = None,
    tag: Annotated[
        Optional[list[str]],
        typer.Option("--tag", "-t", help="Tag as key=value (can specify multiple)"),
    ] = None,
    conn: Annotated[str, typer.Option("--connection", "-c", help="Default connection")] = None,
    strategy: Annotated[str, typer.Option("--strategy", "-s", help="Default strategy")] = None,
    grant: Annotated[
        Optional[list[str]],
        typer.Option("--grant", "-g", help="Default grants (can specify multiple)"),
    ] = None,
    most_recent: Annotated[
        bool,
        typer.Option("--most-recent", help="Enable most_recent tracking by default"),
    ] = False,
):
    """
    Create a load profile.

    Example:
        dcx config profile add ucop-census --dest ucop_file_loads \\
            --tag extract_type=CENSUS --strategy append --most-recent
    """
    profile = {}

    if dest:
        profile["dest"] = dest
    if conn:
        profile["connection"] = conn
    if strategy:
        profile["strategy"] = strategy
    if most_recent:
        profile["most_recent"] = True
    if grant:
        profile["grants"] = grant

    # Parse tags
    if tag:
        tags = {}
        for t in tag:
            if "=" in t:
                key, value = t.split("=", 1)
                tags[key] = value
        if tags:
            profile["tags"] = tags

    if not profile:
        console.print("[red]Must specify at least one option[/red]")
        raise typer.Exit(1)

    add_profile(name, profile)
    console.print(f"[green]Added profile: {name}[/green]")

    # Show what was saved
    for key, value in profile.items():
        console.print(f"  {key}: {value}")


@profile_app.command("list")
def profile_list():
    """List all profiles."""
    profiles = list_profiles()

    if not profiles:
        console.print("[yellow]No profiles configured. Run: dcx config profile add <name>[/yellow]")
        return

    table = Table(title="Profiles")
    table.add_column("Name", style="cyan")
    table.add_column("Dest")
    table.add_column("Tags")
    table.add_column("Strategy")
    table.add_column("Options")

    for name, cfg in profiles.items():
        tags_str = ", ".join(f"{k}={v}" for k, v in cfg.get("tags", {}).items())
        options = []
        if cfg.get("most_recent"):
            options.append("most_recent")
        if cfg.get("grants"):
            options.append(f"grants: {cfg['grants']}")

        table.add_row(
            name,
            cfg.get("dest", ""),
            tags_str,
            cfg.get("strategy", ""),
            ", ".join(options) if options else "",
        )

    console.print(table)


@profile_app.command("show")
def profile_show(
    name: Annotated[str, typer.Argument(help="Profile name")],
):
    """Show profile details."""
    profile = get_profile(name)

    if not profile:
        console.print(f"[red]Profile not found: {name}[/red]")
        raise typer.Exit(1)

    console.print(f"\n[bold]Profile: {name}[/bold]\n")
    for key, value in profile.items():
        console.print(f"  {key}: {value}")


@profile_app.command("remove")
def profile_remove(
    name: Annotated[str, typer.Argument(help="Profile name to remove")],
):
    """Remove a profile."""
    if remove_profile(name):
        console.print(f"[green]Removed profile: {name}[/green]")
    else:
        console.print(f"[red]Profile not found: {name}[/red]")
        raise typer.Exit(1)

"""DCX configuration management."""

import sys
from pathlib import Path
from typing import Any, Optional

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import tomli_w

CONFIG_DIR = Path.home() / ".dcx"
CONFIG_FILE = CONFIG_DIR / "config.toml"


def get_config_path() -> Path:
    """Get the config file path."""
    return CONFIG_FILE


def load_config() -> dict[str, Any]:
    """Load config from file."""
    if not CONFIG_FILE.exists():
        return {"default": None, "connections": {}}

    with open(CONFIG_FILE, "rb") as f:
        return tomllib.load(f)


def save_config(config: dict[str, Any]) -> None:
    """Save config to file."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "wb") as f:
        tomli_w.dump(config, f)


def add_connection(name: str, connection: dict[str, Any], set_as_default: bool = False) -> None:
    """Add or update a connection."""
    config = load_config()

    if "connections" not in config:
        config["connections"] = {}

    config["connections"][name] = connection

    if set_as_default or config.get("default") is None:
        config["default"] = name

    save_config(config)


def remove_connection(name: str) -> bool:
    """Remove a connection. Returns True if found and removed."""
    config = load_config()

    if name not in config.get("connections", {}):
        return False

    del config["connections"][name]

    if config.get("default") == name:
        # Set new default to first remaining connection, or None
        remaining = list(config["connections"].keys())
        config["default"] = remaining[0] if remaining else None

    save_config(config)
    return True


def set_default_connection(name: str) -> bool:
    """Set the default connection. Returns True if connection exists."""
    config = load_config()

    if name not in config.get("connections", {}):
        return False

    config["default"] = name
    save_config(config)
    return True


def list_connections() -> dict[str, Any]:
    """List all connections with default indicator."""
    return load_config()


def get_connection(name: Optional[str] = None) -> Optional[dict[str, Any]]:
    """Get a connection by name, or the default connection."""
    config = load_config()

    if name is None:
        name = config.get("default")

    if name is None:
        return None

    return config.get("connections", {}).get(name)


# ============================================================================
# Profiles
# ============================================================================


def add_profile(name: str, profile: dict[str, Any]) -> None:
    """Add or update a load profile."""
    config = load_config()

    if "profiles" not in config:
        config["profiles"] = {}

    config["profiles"][name] = profile
    save_config(config)


def remove_profile(name: str) -> bool:
    """Remove a profile. Returns True if found and removed."""
    config = load_config()

    if name not in config.get("profiles", {}):
        return False

    del config["profiles"][name]
    save_config(config)
    return True


def get_profile(name: str) -> Optional[dict[str, Any]]:
    """Get a profile by name."""
    config = load_config()
    return config.get("profiles", {}).get(name)


def list_profiles() -> dict[str, Any]:
    """List all profiles."""
    config = load_config()
    return config.get("profiles", {})


def test_connection(name: Optional[str] = None) -> tuple[bool, str]:
    """Test a connection. Returns (success, message)."""
    conn_config = get_connection(name)

    if not conn_config:
        return False, f"Connection not found: {name or 'default'}"

    try:
        import snowflake.connector

        connect_args = {
            "account": conn_config["account"],
            "database": conn_config.get("database"),
            "warehouse": conn_config.get("warehouse"),
            "role": conn_config.get("role"),
        }

        # Add user if present
        if conn_config.get("user"):
            connect_args["user"] = conn_config["user"]

        # Handle authentication
        authenticator = conn_config.get("authenticator", "externalbrowser")

        if authenticator == "snowflake_jwt" and conn_config.get("private_key_path"):
            # JWT auth with private key file
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            key_path = Path(conn_config["private_key_path"]).expanduser()
            with open(key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                    backend=default_backend(),
                )
            connect_args["private_key"] = private_key
        else:
            connect_args["authenticator"] = authenticator

        conn = snowflake.connector.connect(**connect_args)

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        user, role, warehouse = cursor.fetchone()
        conn.close()

        return True, f"Connected as {user} (role: {role}, warehouse: {warehouse})"

    except Exception as e:
        return False, str(e)

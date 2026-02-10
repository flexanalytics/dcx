"""Shared Snowflake connection utilities."""

import snowflake.connector


def get_snowflake_connection(config: dict):
    """Create a Snowflake connection from config dict."""
    connect_args = {
        "account": config["account"],
        "database": config.get("database"),
        "warehouse": config.get("warehouse"),
        "role": config.get("role"),
        "schema": config.get("schema"),
    }

    # Add user if present
    if config.get("user"):
        connect_args["user"] = config["user"]

    # Handle authentication
    authenticator = config.get("authenticator", "externalbrowser")

    if authenticator == "snowflake_jwt" and config.get("private_key_path"):
        # JWT auth with private key file
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        with open(config["private_key_path"], "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend(),
            )
        connect_args["private_key"] = private_key
    else:
        connect_args["authenticator"] = authenticator

    conn = snowflake.connector.connect(**connect_args)

    # Explicitly set schema if provided
    if config.get("schema"):
        conn.cursor().execute(f"USE SCHEMA {config['schema']}")

    return conn

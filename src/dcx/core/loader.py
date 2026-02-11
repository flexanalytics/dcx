"""DCX file loader - Load files into Snowflake."""

import csv
import json
import re
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import snowflake.connector
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


class SchemaNotFoundError(Exception):
    """Raised when schema doesn't exist and create_schema is False."""

    def __init__(self, schema: str):
        self.schema = schema
        super().__init__(f"Schema '{schema}' does not exist")


class FileLoader:
    """Load files into Snowflake using native COPY INTO."""

    def __init__(
        self,
        connection: dict[str, Any],
        dest_table: str,
        tags: dict[str, str] | None = None,
        strategy: str = "overwrite",
        file_format: str = "auto",
        create_table: bool = True,
        create_schema: bool = True,
        grants: list[str] | None = None,
        track_most_recent: bool = False,
        skip_header: int = 0,
        expand_columns: bool = False,
        audit: bool = False,
    ):
        self.connection = connection
        self.dest_table = dest_table
        self.tags = tags or {}
        self.strategy = strategy
        self.file_format = file_format
        self.create_table = create_table
        self.create_schema = create_schema
        self.grants = grants or []
        self.track_most_recent = track_most_recent
        self.skip_header = skip_header
        self.expand_columns = expand_columns
        self.audit = audit
        self._conn = None
        self._temp_dir = None
        self._stage_name = f"dcx_stage_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        self._csv_columns: list[str] | None = None  # Cached CSV column names

    def _get_conn(self):
        """Get or create Snowflake connection."""
        if self._conn is None:
            connect_args = {
                "account": self.connection["account"],
                "database": self.connection.get("database"),
                "warehouse": self.connection.get("warehouse"),
                "role": self.connection.get("role"),
                "schema": self.connection.get("schema"),
            }

            # Add user if present
            if self.connection.get("user"):
                connect_args["user"] = self.connection["user"]

            # Handle authentication
            authenticator = self.connection.get("authenticator", "externalbrowser")

            if authenticator == "snowflake_jwt" and self.connection.get("private_key_path"):
                # JWT auth with private key file
                from cryptography.hazmat.backends import default_backend
                from cryptography.hazmat.primitives import serialization

                key_path = Path(self.connection["private_key_path"]).expanduser()
                with open(key_path, "rb") as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=None,
                        backend=default_backend(),
                    )
                connect_args["private_key"] = private_key
            else:
                connect_args["authenticator"] = authenticator

            self._conn = snowflake.connector.connect(**connect_args)

            # Create and/or use schema if provided
            if self.connection.get("schema"):
                schema = self.connection["schema"]
                if self.create_schema:
                    self._conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                try:
                    self._conn.cursor().execute(f"USE SCHEMA {schema}")
                except snowflake.connector.errors.ProgrammingError as e:
                    if "does not exist" in str(e) or "002043" in str(e):
                        raise SchemaNotFoundError(schema) from e
                    raise

        return self._conn

    def _execute(self, sql: str, params: tuple = None) -> Any:
        """Execute SQL and return cursor."""
        cursor = self._get_conn().cursor()
        cursor.execute(sql, params)
        return cursor

    def load(self, source: Path) -> dict[str, int]:
        """
        Load files from source path into Snowflake.

        Returns dict with 'rows', 'files', and optionally 'deleted' counts.
        """
        load_id = str(uuid.uuid4())

        try:
            # Resolve files to load
            files = list(self._iter_files(source))
            if not files:
                raise ValueError(f"No files found in {source}")

            console.print(f"Found {len(files)} file(s) to load")

            # If expand_columns, detect columns from first file
            if self.expand_columns and files:
                first_file, _ = files[0]
                detected_format = self._detect_format(first_file)
                if detected_format in ("csv", "tsv"):
                    delimiter = "," if detected_format == "csv" else "\t"
                    self._csv_columns = self._get_csv_headers(first_file, delimiter)
                    if self._csv_columns:
                        console.print(f"[dim]Detected {len(self._csv_columns)} columns from CSV header[/dim]")
                    else:
                        console.print("[yellow]Warning: No columns detected, falling back to single data column[/yellow]")
                        self.expand_columns = False
                else:
                    console.print("[yellow]Warning: --expand-columns only works with CSV/TSV files[/yellow]")
                    self.expand_columns = False

            # Create table if needed (outside transaction - DDL auto-commits)
            if self.create_table:
                self._ensure_table_exists()

            # Create audit table if auditing enabled
            if self.audit:
                self._ensure_audit_table()

            # Create temporary stage (outside transaction)
            self._execute(f"CREATE TEMPORARY STAGE {self._stage_name}")

            # Begin transaction for data operations
            self._execute("BEGIN TRANSACTION")

            try:
                # Handle strategy
                deleted = 0
                if self.strategy == "replace":
                    deleted = self._truncate_table()
                elif self.strategy == "overwrite" and self.tags:
                    deleted = self._delete_matching_tags()

                # Mark existing rows as not most recent (before loading new ones)
                if self.track_most_recent and self.tags:
                    self._mark_existing_not_recent()

                total_rows = 0
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console,
                ) as progress:
                    for file_path, file_name in files:
                        task = progress.add_task(f"Loading {file_name}...", total=None)

                        rows = self._load_file(file_path, file_name)
                        total_rows += rows

                        progress.update(task, description=f"Loaded {file_name} ({rows:,} rows)")
                        progress.remove_task(task)

                # Commit transaction
                self._execute("COMMIT")
                console.print("[dim]Transaction committed[/dim]")

            except Exception as e:
                # Rollback on any error
                self._execute("ROLLBACK")
                console.print("[red]Transaction rolled back[/red]")

                # Log failed load to audit
                if self.audit:
                    self._log_audit(
                        load_id=load_id,
                        file_count=len(files),
                        status="failed",
                        error_message=str(e)[:1000],
                    )
                raise

            # Apply grants (outside transaction - DCL)
            if self.grants:
                self._apply_grants()

            # Log successful load to audit
            if self.audit:
                self._log_audit(
                    load_id=load_id,
                    row_count=total_rows,
                    file_count=len(files),
                    deleted_count=deleted,
                    status="success",
                )
                console.print(f"[dim]Audit logged: {load_id}[/dim]")

            return {
                "rows": total_rows,
                "files": len(files),
                "deleted": deleted if deleted else None,
                "grants": self.grants if self.grants else None,
                "load_id": load_id if self.audit else None,
            }

        finally:
            # Cleanup temp directory if created
            if self._temp_dir:
                import shutil
                shutil.rmtree(self._temp_dir, ignore_errors=True)
                self._temp_dir = None

            if self._conn:
                self._conn.close()
                self._conn = None

    def _iter_files(self, source: Path) -> list[tuple[Path, str]]:
        """Get list of files to load. Returns [(file_path, display_name), ...]."""
        files = []

        if source.is_file():
            if source.suffix.lower() == ".zip":
                # Extract zip to temp dir (caller must handle cleanup)
                self._temp_dir = tempfile.mkdtemp()
                temp_path = Path(self._temp_dir)
                with zipfile.ZipFile(source) as zf:
                    zf.extractall(temp_path)

                # Collect extracted files
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

    def _ensure_table_exists(self) -> None:
        """Create destination table if it doesn't exist."""
        # Build column definitions
        columns = [
            "_source_file VARCHAR",
            "_load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()",
        ]

        # Add most_recent column if tracking
        if self.track_most_recent:
            columns.append("is_most_recent BOOLEAN DEFAULT TRUE")

        # Add tag columns
        for tag_name in self.tags.keys():
            columns.append(f"{tag_name} VARCHAR")

        # Add data columns - either expanded CSV columns or single VARIANT
        if self.expand_columns and self._csv_columns:
            # Create one VARCHAR column per CSV column (quote identifiers for special chars)
            for col_name in self._csv_columns:
                columns.append(f'"{col_name}" VARCHAR')
        else:
            # Single VARIANT column for flexible storage
            columns.append("data VARIANT")

        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.dest_table} (
            {', '.join(columns)}
        )
        """
        self._execute(sql)

        # Add is_most_recent column if it doesn't exist (for existing tables)
        if self.track_most_recent:
            try:
                self._execute(f"""
                    ALTER TABLE {self.dest_table}
                    ADD COLUMN IF NOT EXISTS is_most_recent BOOLEAN DEFAULT TRUE
                """)
            except Exception:
                pass  # Column might already exist

    def _truncate_table(self) -> int:
        """Truncate table. Returns count of deleted rows."""
        cursor = self._execute(f"SELECT COUNT(*) FROM {self.dest_table}")
        count = cursor.fetchone()[0]

        if count > 0:
            self._execute(f"TRUNCATE TABLE {self.dest_table}")
            console.print(f"[dim]Truncated table ({count:,} rows)[/dim]")

        return count

    def _delete_matching_tags(self) -> int:
        """Delete rows matching current tags. Returns count of deleted rows."""
        if not self.tags:
            return 0

        conditions = " AND ".join(f"{k} = %s" for k in self.tags.keys())
        values = tuple(self.tags.values())

        # Get count first
        cursor = self._execute(
            f"SELECT COUNT(*) FROM {self.dest_table} WHERE {conditions}",
            values,
        )
        count = cursor.fetchone()[0]

        if count > 0:
            self._execute(
                f"DELETE FROM {self.dest_table} WHERE {conditions}",
                values,
            )
            console.print(f"[dim]Deleted {count:,} existing rows matching tags[/dim]")

        return count

    def _mark_existing_not_recent(self) -> int:
        """Mark existing rows matching tags as not most recent. Returns count updated."""
        if not self.tags:
            return 0

        conditions = " AND ".join(f"{k} = %s" for k in self.tags.keys())
        values = tuple(self.tags.values())

        # Update existing rows
        self._execute(
            f"UPDATE {self.dest_table} SET is_most_recent = FALSE WHERE {conditions} AND is_most_recent = TRUE",
            values,
        )

        # Get count of updated rows
        cursor = self._get_conn().cursor()
        count = cursor.rowcount or 0

        if count > 0:
            console.print(f"[dim]Marked {count:,} existing rows as not most recent[/dim]")

        return count

    def _detect_format(self, file_path: Path) -> str:
        """Detect file format from extension if auto mode."""
        if self.file_format != "auto":
            return self.file_format

        ext = file_path.suffix.lower()
        if ext == ".csv":
            return "csv"
        elif ext == ".tsv":
            return "tsv"
        else:
            return "single-column"

    def _get_csv_headers(self, file_path: Path, delimiter: str) -> list[str]:
        """Read CSV header row to get column names."""
        with open(file_path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
            if not first_line:
                return []
            # Parse header respecting quotes
            reader = csv.reader([first_line], delimiter=delimiter)
            headers = next(reader)
            # Sanitize column names for Snowflake
            return [self._sanitize_column_name(h) for h in headers]

    def _sanitize_column_name(self, name: str) -> str:
        """Sanitize column name for Snowflake."""
        # Replace non-alphanumeric with underscore
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
        # Ensure starts with letter or underscore
        if sanitized and sanitized[0].isdigit():
            sanitized = "_" + sanitized
        return sanitized.upper() or "COL"

    def _load_file(self, file_path: Path, file_name: str) -> int:
        """Load a single file into Snowflake. Returns row count."""
        # Upload file to stage (use as_posix() for Windows compatibility)
        self._execute(
            f"PUT 'file://{file_path.as_posix()}' @{self._stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )

        # Detect format
        detected_format = self._detect_format(file_path)

        # Build column list for INSERT
        columns = ["_source_file"]
        values = [f"'{file_name}'"]

        # Add most_recent flag if tracking
        if self.track_most_recent:
            columns.append("is_most_recent")
            values.append("TRUE")

        for tag_name, tag_value in self.tags.items():
            columns.append(tag_name)
            values.append(f"'{tag_value}'")

        # COPY INTO from stage
        staged_file = f"@{self._stage_name}/{file_path.name}"

        if detected_format == "single-column":
            # Single column mode: each line is one value (wrap in TO_VARIANT to store as string)
            columns.append("data")
            values.append("TO_VARIANT($1)")
            file_format = f"""
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = NONE
                SKIP_HEADER = {self.skip_header}
                FIELD_OPTIONALLY_ENCLOSED_BY = NONE
                ESCAPE_UNENCLOSED_FIELD = NONE
            )
            """
        elif self.expand_columns and self._csv_columns:
            # Expanded columns mode: one column per CSV field
            delimiter = "," if detected_format == "csv" else "\\t"
            skip_header = max(1, self.skip_header)

            for i, col_name in enumerate(self._csv_columns, 1):
                columns.append(f'"{col_name}"')
                values.append(f"${i}")

            file_format = f"""
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = '{delimiter}'
                SKIP_HEADER = {skip_header}
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE_UNENCLOSED_FIELD = NONE
            )
            """
        else:
            # CSV/TSV mode: parse into JSON object in single VARIANT column
            columns.append("data")
            delimiter = "," if detected_format == "csv" else "\\t"
            skip_header = max(1, self.skip_header)  # Always skip at least the header row

            # Get column names from header
            header_delimiter = "," if detected_format == "csv" else "\t"
            headers = self._get_csv_headers(file_path, header_delimiter)

            if headers:
                # Build OBJECT_CONSTRUCT with column names
                obj_parts = []
                for i, header in enumerate(headers, 1):
                    obj_parts.append(f"'{header}', ${i}")
                values.append(f"OBJECT_CONSTRUCT_KEEP_NULL({', '.join(obj_parts)})")
            else:
                # No headers, store as simple value
                values.append("$1")
                skip_header = self.skip_header

            file_format = f"""
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_DELIMITER = '{delimiter}'
                SKIP_HEADER = {skip_header}
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE_UNENCLOSED_FIELD = NONE
            )
            """

        sql = f"""
        COPY INTO {self.dest_table} ({', '.join(columns)})
        FROM (
            SELECT {', '.join(values)}
            FROM {staged_file}
        )
        {file_format}
        ON_ERROR = ABORT_STATEMENT
        """

        cursor = self._execute(sql)
        result = cursor.fetchone()

        # Result format: (file, status, rows_parsed, rows_loaded, ...)
        rows_loaded = result[3] if result else 0
        return rows_loaded

    def _apply_grants(self) -> None:
        """Apply grants to the destination table."""
        schema = self.connection.get("schema")

        for role in self.grants:
            role = role.upper()
            # Grant usage on schema
            if schema:
                self._execute(f"GRANT USAGE ON SCHEMA {schema} TO ROLE {role}")
            # Grant select on table
            self._execute(f"GRANT SELECT ON TABLE {self.dest_table} TO ROLE {role}")
            console.print(f"[dim]Granted SELECT to {role}[/dim]")

    def _ensure_audit_table(self) -> None:
        """Create the audit table if it doesn't exist."""
        self._execute("""
            CREATE TABLE IF NOT EXISTS _dcx_load_history (
                load_id VARCHAR,
                table_name VARCHAR,
                tags VARIANT,
                strategy VARCHAR,
                row_count INTEGER,
                file_count INTEGER,
                deleted_count INTEGER,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                status VARCHAR,
                error_message VARCHAR,
                user_name VARCHAR DEFAULT CURRENT_USER()
            )
        """)

    def _log_audit(
        self,
        load_id: str,
        row_count: int = 0,
        file_count: int = 0,
        deleted_count: int = 0,
        status: str = "success",
        error_message: str | None = None,
    ) -> None:
        """Log a load operation to the audit table."""
        tags_json = json.dumps(self.tags) if self.tags else "{}"

        self._execute(
            """
            INSERT INTO _dcx_load_history (
                load_id, table_name, tags, strategy, row_count, file_count,
                deleted_count, status, error_message
            ) VALUES (%s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s, %s)
            """,
            (
                load_id,
                self.dest_table,
                tags_json,
                self.strategy,
                row_count,
                file_count,
                deleted_count,
                status,
                error_message,
            ),
        )

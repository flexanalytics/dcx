# dcx - DataCampus CLI

A command-line tool for loading files into Snowflake with metadata tagging, format detection, and audit logging.

## Features

- **Flexible file loading**: Load single files, folders, or zip archives
- **Format auto-detection**: Automatically handles single-column, CSV, and TSV files
- **Metadata tagging**: Tag loads with key-value pairs for easy filtering
- **Load strategies**: Overwrite by tags, append for history, or replace entire table
- **Most-recent tracking**: Boolean column to identify the latest load
- **Audit logging**: Track all load operations in a history table
- **Profiles**: Save reusable load configurations
- **dbt integration**: Import connection settings from dbt profiles.yml
- **Pre-load validation**: Check files before loading

## Installation

**Option 1: pip (recommended)**
```bash
pip install git+https://github.com/flexanalytics/dcx.git
```

**Option 2: pipx (isolated environment)**
```bash
# Install pipx first if needed: pip install pipx
pipx install git+https://github.com/flexanalytics/dcx.git
```

**Option 3: Development install**
```bash
git clone https://github.com/flexanalytics/dcx.git
cd dcx
pip install -e .
```

## Quick Start

### 1. Configure a connection

```bash
# Auto-detects dbt profiles.yml and offers to import
dcx config add prod

# Or specify details manually
dcx config add prod --account abc12345.us-east-1 --database ANALYTICS --warehouse WH
```

### 2. Load files

```bash
# Load a zip file with tags
dcx load ./CENSUS_2258.zip --dest ucop_file_loads \
  --tag extract_type=CENSUS \
  --tag term_code=2258

# Load a CSV file (format auto-detected)
dcx load ./data.csv --dest imports

# Validate before loading
dcx validate ./data.zip
```

### 3. Query loaded data

```sql
-- Single-column files: data is a string
SELECT data FROM my_table WHERE _source_file = 'STUDENT.txt';

-- CSV files: data is a JSON object, use dot notation
SELECT data:NAME, data:EMAIL FROM my_table;

-- Filter by tags
SELECT * FROM ucop_file_loads
WHERE extract_type = 'CENSUS' AND term_code = '2258';
```

---

## Commands

### `dcx load`

Load files into Snowflake.

```bash
dcx load <source> [options]
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `source` | File, folder, or zip to load |

**Options:**
| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--dest` | `-d` | Destination table (schema.table or table) | Required* |
| `--profile` | `-p` | Load profile name | - |
| `--tag` | `-t` | Metadata tag as key=value (repeatable) | - |
| `--strategy` | `-s` | Load strategy: overwrite, append, replace | overwrite |
| `--format` | `-f` | File format: auto, single-column, csv, tsv | auto |
| `--skip-header` | | Number of header lines to skip | 0 |
| `--connection` | `-c` | Connection name | default |
| `--create-table` | | Create table if not exists | true |
| `--create-schema` | | Create schema if not exists (skips prompt) | false |
| `--grant` | `-g` | Grant SELECT to role (repeatable) | - |
| `--most-recent` | | Track most recent load with boolean column | false |
| `--single-column` | | Store CSV as single JSON column instead of expanding | false |
| `--audit` | | Log load to _dcx_load_history table | false |
| `--dry-run` | | Show what would be done without executing | false |

*Required unless using a profile with `dest` configured.

**Load Strategies:**

| Strategy | Behavior |
|----------|----------|
| `overwrite` | Delete rows matching tags, then insert new rows |
| `append` | Insert without deleting (preserves history) |
| `replace` | Truncate entire table, then insert |

**File Formats:**

| Format | Behavior |
|--------|----------|
| `auto` | Detect from extension (.csv, .tsv, or single-column) |
| `single-column` | Each line stored as a single VARIANT value |
| `csv` | Comma-delimited, creates one column per CSV field |
| `tsv` | Tab-delimited, creates one column per TSV field |

**CSV Column Handling:**

By default, CSV/TSV files create separate table columns from the header row. Use `--single-column` to store as a JSON object in a single VARIANT column instead.

**Examples:**

```bash
# Basic load with tags
dcx load ./data.zip --dest my_table --tag env=prod --tag version=1.0

# CSV file with grants
dcx load ./users.csv --dest analytics.users --grant ANALYST --grant REPORTER

# Track most recent load
dcx load ./extract.zip --dest loads --tag type=daily --most-recent

# Use a profile with runtime tag
dcx load ./CENSUS_2258.zip --profile ucop-census --tag term_code=2258

# Audit the load
dcx load ./data.zip --dest my_table --audit

# Dry run to preview
dcx load ./data.zip --dest my_table --dry-run
```

---

### `dcx list`

List loaded data with optional filtering.

```bash
dcx list <table> [options]
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--tag` | `-t` | Filter by tag as key=value (repeatable) |
| `--connection` | `-c` | Connection name |

**Example:**

```bash
dcx list ucop_file_loads --tag extract_type=CENSUS --tag term_code=2258
```

---

### `dcx delete`

Delete loaded data by tags.

```bash
dcx delete <table> [options]
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--tag` | `-t` | Filter by tag as key=value (repeatable, required) |
| `--connection` | `-c` | Connection name |
| `--yes` | `-y` | Skip confirmation prompt |

**Example:**

```bash
# Delete with confirmation
dcx delete ucop_file_loads --tag extract_type=CENSUS --tag term_code=2258

# Skip confirmation
dcx delete ucop_file_loads --tag term_code=2252 --yes
```

---

### `dcx info`

Show table information including schema and row counts.

```bash
dcx info <table> [options]
```

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--connection` | `-c` | Connection name |

**Example:**

```bash
dcx info ucop_file_loads
```

---

### `dcx validate`

Validate files before loading.

```bash
dcx validate <source> [options]
```

Checks:
- Files can be read
- UTF-8 encoding is valid
- No lines exceed Snowflake VARCHAR limit (16MB)

**Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--verbose` | `-v` | Show detailed file info |

**Example:**

```bash
dcx validate ./CENSUS_2258.zip --verbose
```

---

### `dcx config`

Manage Snowflake connections.

```bash
dcx config add <name> [options]    # Add a connection
dcx config list                     # List connections
dcx config remove <name>            # Remove a connection
dcx config default <name>           # Set default connection
dcx config test [name]              # Test a connection
dcx config path                     # Show config file location
```

**Add Options:**
| Option | Description |
|--------|-------------|
| `--account`, `-a` | Snowflake account |
| `--database`, `-d` | Default database |
| `--warehouse`, `-w` | Default warehouse |
| `--role`, `-r` | Default role |
| `--schema`, `-s` | Default schema |
| `--authenticator` | Auth method (externalbrowser, snowflake_jwt, etc.) |
| `--from-dbt` | Force import from dbt profiles.yml |
| `--default` | Set as default connection |

**dbt Profile Import:**

When adding a connection, dcx automatically checks for `~/.dbt/profiles.yml` (or `$DBT_PROFILES_DIR`). If Snowflake profiles exist, you can select one to import:

```
$ dcx config add prod

Available dbt Snowflake profiles:
  1. my_project.dev
  2. my_project.prod
  3. Enter manually

Select profile: 2

Importing from my_project.prod:
  account: abc12345.us-east-1
  database: ANALYTICS
  warehouse: TRANSFORM_WH
  role: TRANSFORMER
  authenticator: externalbrowser

Would you like to make changes? [y/N]: n
Save this configuration? [Y/n]: y

Added connection: prod
```

**dbt Project Auto-Detection:**

When running `dcx load` without a configured connection, dcx checks for `dbt_project.yml` in the current directory. If found, it reads the profile name and looks up the matching connection from `~/.dbt/profiles.yml`:

```
$ dcx load ./data.zip --dest my_table

Found dbt_project.yml using profile 'my_project' (target: dev)
  account: abc12345.us-east-1
  database: ANALYTICS
  warehouse: TRANSFORM_WH

Use this connection? [Y/n]: y

Source: ./data.zip
Destination: ANALYTICS.RAW.my_table
Connection: dbt:my_project.dev (abc12345.us-east-1)
...
```

---

### `dcx config profile`

Manage load profiles for reusable configurations.

```bash
dcx config profile add <name> [options]    # Create a profile
dcx config profile list                     # List profiles
dcx config profile show <name>              # Show profile details
dcx config profile remove <name>            # Remove a profile
```

**Profile Add Options:**
| Option | Short | Description |
|--------|-------|-------------|
| `--dest` | `-d` | Default destination table |
| `--tag` | `-t` | Default tag as key=value (repeatable) |
| `--connection` | `-c` | Default connection |
| `--strategy` | `-s` | Default strategy |
| `--grant` | `-g` | Default grants (repeatable) |
| `--most-recent` | | Enable most_recent tracking |

**Example:**

```bash
# Create a profile for UCOP Census loads
dcx config profile add ucop-census \
  --dest ucop_file_loads \
  --tag extract_type=CENSUS \
  --strategy overwrite \
  --most-recent \
  --grant ANALYST

# Use the profile (only need to specify runtime tags)
dcx load ./CENSUS_2258.zip --profile ucop-census --tag term_code=2258
```

---

## Table Schema

When using `--create-table`, dcx creates a table based on file format:

**For CSV/TSV files (default):**
```sql
CREATE TABLE IF NOT EXISTS <dest> (
    _source_file     VARCHAR,                                    -- Original filename
    _load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),  -- When loaded
    is_most_recent   BOOLEAN DEFAULT TRUE,                       -- If --most-recent
    <tag_name>       VARCHAR,                                    -- One per --tag
    "<CSV Column 1>" VARCHAR,                                    -- From CSV header
    "<CSV Column 2>" VARCHAR,
    ...
);
```

**For single-column files (or with `--single-column`):**
```sql
CREATE TABLE IF NOT EXISTS <dest> (
    _source_file     VARCHAR,
    _load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    is_most_recent   BOOLEAN DEFAULT TRUE,                       -- If --most-recent
    <tag_name>       VARCHAR,                                    -- One per --tag
    data             VARIANT                                     -- Each line as string/JSON
);
```

**Querying CSV/TSV Data:**

```sql
-- CSV columns are created directly in the table
SELECT
    "STUDENT_ID",
    "FIRST_NAME",
    "EMAIL"
FROM my_table;

-- Column names preserve original casing from CSV header
SELECT * FROM my_table
WHERE "STATUS" = 'ACTIVE';
```

**Querying single-column data (with `--single-column`):**

```sql
-- Access JSON fields with dot notation
SELECT data:STUDENT_ID::VARCHAR AS student_id
FROM my_table;
```

---

## Audit Table

When using `--audit`, dcx logs to `_dcx_load_history`:

```sql
CREATE TABLE _dcx_load_history (
    load_id         VARCHAR,           -- UUID for the load
    table_name      VARCHAR,           -- Destination table
    tags            VARIANT,           -- Tags as JSON
    strategy        VARCHAR,           -- Load strategy used
    row_count       INTEGER,           -- Rows loaded
    file_count      INTEGER,           -- Files processed
    deleted_count   INTEGER,           -- Rows deleted (overwrite/replace)
    load_timestamp  TIMESTAMP_NTZ,     -- When loaded
    status          VARCHAR,           -- 'success' or 'failed'
    error_message   VARCHAR,           -- Error details if failed
    user_name       VARCHAR            -- Snowflake user
);
```

**Query Load History:**

```sql
-- Recent loads
SELECT load_id, table_name, tags, row_count, status, load_timestamp
FROM _dcx_load_history
ORDER BY load_timestamp DESC
LIMIT 10;

-- Failed loads
SELECT * FROM _dcx_load_history WHERE status = 'failed';
```

---

## Config File

Stored at `~/.dcx/config.toml`:

```toml
default = "prod"

[connections.prod]
account = "abc12345.us-east-1"
user = "myuser"
database = "ANALYTICS"
warehouse = "TRANSFORM_WH"
schema = "RAW"
role = "TRANSFORMER"
authenticator = "externalbrowser"

[connections.dev]
account = "abc12345.us-east-1"
database = "DEV"
warehouse = "DEV_WH"
authenticator = "snowflake_jwt"
private_key_path = "~/.ssh/snowflake_key.p8"

[profiles.ucop-census]
dest = "ucop_file_loads"
strategy = "overwrite"
most_recent = true
grants = ["ANALYST"]

[profiles.ucop-census.tags]
extract_type = "CENSUS"
```

---

## Examples

### UCOP File Loading

```bash
# Create a profile for Census loads
dcx config profile add ucop-census \
  --dest ucop_file_loads \
  --tag extract_type=CENSUS \
  --most-recent \
  --grant ANALYST

# Load Census extract (overwrites previous for same term)
dcx load ./CENSUS_PBFILES_3WK_2258.zip \
  --profile ucop-census \
  --tag term_code=2258

# Keep history of all loads
dcx load ./CENSUS_PBFILES_3WK_2258.zip \
  --profile ucop-census \
  --tag term_code=2258 \
  --strategy append

# Validate before loading
dcx validate ./CENSUS_PBFILES_3WK_2258.zip
```

### CSV Data Loading

```bash
# Load CSV with auto-detection
dcx load ./users.csv --dest analytics.users

# Query the JSON data
# SELECT data:EMAIL, data:NAME FROM analytics.users;

# Explicit format
dcx load ./data.txt --dest my_table --format csv --skip-header 2
```

### Query Loaded Data

```sql
-- Get latest Census data for a term
SELECT _source_file, data
FROM ucop_file_loads
WHERE extract_type = 'CENSUS'
  AND term_code = '2258'
  AND is_most_recent = TRUE;

-- Compare file counts across loads
SELECT
    _load_timestamp,
    _source_file,
    COUNT(*) as rows
FROM ucop_file_loads
WHERE extract_type = 'CENSUS'
GROUP BY 1, 2
ORDER BY 1 DESC;

-- Get all unique tags
SELECT DISTINCT extract_type, term_code
FROM ucop_file_loads;
```

---

## Authentication

dcx supports multiple Snowflake authentication methods:

| Method | Config |
|--------|--------|
| Browser SSO | `authenticator = "externalbrowser"` |
| Username/Password | `authenticator = "snowflake"` |
| JWT with Key File | `authenticator = "snowflake_jwt"` + `private_key_path` |
| Okta | `authenticator = "https://myorg.okta.com"` |

---

## Troubleshooting

### Schema does not exist

If the target schema doesn't exist, dcx will prompt to create it:

```
Schema 'RAW' does not exist.
Create schema 'RAW'? [Y/n]: y
```

Use `--create-schema` to skip the prompt.

### Insufficient privileges

Use `--grant` to grant SELECT access after loading:

```bash
dcx load ./data.zip --dest my_table --grant ANALYST --grant REPORTER
```

### Line too long

Snowflake VARCHAR has a 16MB limit. Use `dcx validate` to check files:

```bash
dcx validate ./data.zip --verbose
```

### Connection test

Test your connection configuration:

```bash
dcx config test prod
```

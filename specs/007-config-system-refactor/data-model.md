# Data Model: Configuration System

**Feature**: Configuration System Refactor
**Date**: 2025-10-25

## Overview

This document defines the data model for the refactored configuration system. The configuration is represented as Pydantic models loaded from environment variables, not database-backed entities. The models provide type-safe, validated access to application configuration.

## Configuration Entities

### NoizConfig (Root Configuration)

**Purpose**: Root configuration object that aggregates all application settings

**Source**: Environment variables with `NOIZ_` prefix

**Validation Rules**:
- All nested configurations must validate successfully
- At least one database connection method must be specified (DATABASE_URL or POSTGRES_*)
- PROCESSED_DATA_DIR must be provided

**Fields**:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| database | DatabaseConfig | Yes | - | Database connection configuration |
| processing | ProcessingConfig | Yes | - | Data processing configuration |
| flask_env | Literal["development", "production"] | No | "development" | Flask environment mode |
| loglevel | str | No | "INFO" | Logging level (compatible with loguru) |

**Validation**:
- `flask_env` must be either "development" or "production"
- `loglevel` must be valid loguru level name

**Methods**:
- `to_flask_config() -> dict`: Generate Flask-compatible config dictionary with derived values
- `__repr__()`: Custom repr that masks sensitive fields

**Relationships**:
- Composes DatabaseConfig (nested)
- Composes ProcessingConfig (nested)

---

### DatabaseConfig (Nested Configuration)

**Purpose**: Database connection settings supporting PostgreSQL and SQLite

**Source**: Nested environment variables via `NOIZ_DATABASE__*` or flat `NOIZ_DATABASE_BACKEND`, `NOIZ_DATABASE_URL`, `NOIZ_POSTGRES_*`

**Validation Rules**:
- If backend is "postgresql": either DATABASE_URL or all POSTGRES_* fields required
- If backend is "sqlite": DATABASE_URL required (defaults to "sqlite:///noiz.db" if not provided)
- URI-first precedence: DATABASE_URL takes precedence over POSTGRES_* if both provided
- Generated SQLALCHEMY_DATABASE_URI must be valid connection string

**Fields**:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| backend | Literal["postgresql", "sqlite"] | No | "postgresql" | Database backend selection |
| database_url | SecretStr \| None | Conditional | None | Complete database connection URI (takes precedence) |
| postgres_host | str \| None | Conditional | None | PostgreSQL host |
| postgres_port | int \| None | Conditional | None | PostgreSQL port |
| postgres_user | str \| None | Conditional | None | PostgreSQL username |
| postgres_password | SecretStr \| None | Conditional | None | PostgreSQL password (masked) |
| postgres_db | str \| None | Conditional | None | PostgreSQL database name |

**Computed Fields**:
- `sqlalchemy_database_uri: SecretStr`: Computed property that returns DATABASE_URL if set, otherwise constructs URI from POSTGRES_* fields. For SQLite, returns "sqlite:///noiz.db" if DATABASE_URL not provided.

**Validation**:
- PostgreSQL backend requires DATABASE_URL OR all of (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB)
- SQLite backend uses DATABASE_URL or defaults to "sqlite:///noiz.db"
- Warn if DATABASE_URL and POSTGRES_* both provided (DATABASE_URL takes precedence)
- Port must be in range 1-65535 if provided

**State Transitions**: N/A (immutable after validation)

---

### ProcessingConfig (Nested Configuration)

**Purpose**: Settings for data processing operations

**Source**: Nested environment variables via `NOIZ_PROCESSING__*` or flat `NOIZ_PROCESSED_DATA_DIR`, `NOIZ_MSEEDINDEX_EXECUTABLE`

**Validation Rules**:
- PROCESSED_DATA_DIR is required
- PROCESSED_DATA_DIR validation:
  - If path doesn't exist: auto-create (warn if parent directories also created)
  - If path exists as file: fail with clear error
  - If path exists as non-empty directory: fail with error about naming conflicts
  - If path exists as empty directory: accept silently
- MSEEDINDEX_EXECUTABLE is required
- MSEEDINDEX_EXECUTABLE must be executable file or in PATH

**Fields**:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| processed_data_dir | Path | Yes | - | Directory for processed seismic data output (rigid naming structure) |
| mseedindex_executable | str | Yes | - | Path to mseedindex executable or command name in PATH |

**Validation**:
- `processed_data_dir` directory validation (auto-create, file check, empty check per clarifications)
- `mseedindex_executable` existence check (file or PATH lookup)

**State Transitions**: N/A (immutable after validation)

---

## Configuration Loading Flow

```
1. Environment variables read with NOIZ_ prefix
   ├─> NOIZ_DATABASE_BACKEND
   ├─> NOIZ_DATABASE_URL (masked in logs)
   ├─> NOIZ_POSTGRES_HOST
   ├─> NOIZ_POSTGRES_PORT
   ├─> NOIZ_POSTGRES_USER
   ├─> NOIZ_POSTGRES_PASSWORD (masked in logs)
   ├─> NOIZ_POSTGRES_DB
   ├─> NOIZ_PROCESSED_DATA_DIR
   ├─> NOIZ_MSEEDINDEX_EXECUTABLE
   ├─> NOIZ_FLASK_ENV
   └─> NOIZ_LOGLEVEL

2. Pydantic Settings instantiation
   └─> NoizConfig() created
       ├─> DatabaseConfig validated
       │   ├─> URI precedence check
       │   ├─> Backend-specific requirements check
       │   └─> Generate SQLALCHEMY_DATABASE_URI
       ├─> ProcessingConfig validated
       │   ├─> Directory existence/creation check
       │   └─> Executable availability check
       └─> Root-level field validation

3. Flask integration
   └─> config.to_flask_config() called
       ├─> Generates Flask-specific values (DEBUG, SQLALCHEMY_TRACK_MODIFICATIONS, etc.)
       └─> Returns dict for app.config.from_mapping()

4. Backward compatibility
   └─> Module-level variables in settings.py
       └─> Expose config values for existing imports
```

## Validation Error Handling

**Error Message Format**:
```
Configuration Error: <field_name>
  Problem: <what's wrong>
  Required: <what's expected>
  Provided: <what was given (masked if sensitive)>
  Action: <how to fix>
```

**Examples**:

```
Configuration Error: NOIZ_PROCESSED_DATA_DIR
  Problem: Path exists but is a file, not a directory
  Provided: /path/to/file.txt
  Action: Set NOIZ_PROCESSED_DATA_DIR to a directory path, not a file

Configuration Error: NOIZ_DATABASE_URL and NOIZ_POSTGRES_*
  Problem: Both DATABASE_URL and individual POSTGRES_* variables provided
  Precedence: NOIZ_DATABASE_URL will be used (URI-first rule)
  Action: This is valid but potentially confusing. Consider using only one method.

Configuration Error: NOIZ_POSTGRES_HOST
  Problem: PostgreSQL backend selected but connection information missing
  Required: Either NOIZ_DATABASE_URL OR all of (NOIZ_POSTGRES_HOST, NOIZ_POSTGRES_PORT, NOIZ_POSTGRES_USER, NOIZ_POSTGRES_PASSWORD, NOIZ_POSTGRES_DB)
  Action: Set NOIZ_DATABASE_URL or provide all individual PostgreSQL connection variables
```

## Logging

**Structured Logging Levels**:

- **INFO**: Successful configuration load
  - Example: `"Configuration loaded successfully (database: postgresql, backend: postgresql)"`
- **WARN**: Non-critical issues (parent directory creation, precedence warnings)
  - Example: `"Created parent directories for NOIZ_PROCESSED_DATA_DIR: /path/to/parent"`
  - Example: `"Both NOIZ_DATABASE_URL and NOIZ_POSTGRES_* provided; using DATABASE_URL (URI-first precedence)"`
- **ERROR**: Validation failures (missing required fields, invalid paths, bad values)
  - Example: `"Configuration validation failed: NOIZ_PROCESSED_DATA_DIR points to file, not directory"`

**Sensitive Field Masking**:
- `postgres_password`: Logged as "******"
- `database_url`: Logged with password masked (e.g., "postgresql://user:******@host:5432/db")
- `sqlalchemy_database_uri`: Logged with password masked

## Testing Considerations

**Test Fixtures**:
- Create fixture that returns NoizConfig with test-appropriate defaults
- Use `@pytest.mark.parametrize(indirect=True)` for test-specific overrides
- Reset pattern: save state → apply test config → restore after test

**Test Scenarios**:
- Valid PostgreSQL config (DATABASE_URL)
- Valid PostgreSQL config (individual POSTGRES_*)
- Valid SQLite config
- Precedence: DATABASE_URL over POSTGRES_*
- Directory validation: missing (auto-create), file (fail), non-empty dir (fail), empty dir (pass)
- Missing required fields
- Invalid field values
- Flask integration
- Backward compatibility imports
- Secret masking in logs

## Backward Compatibility

**Module-Level Variables** (in `noiz.settings`):

```python
# After config is loaded, expose as module-level for backward compat:
from noiz.config import get_config

_config = get_config()

# Exposed variables:
PROCESSED_DATA_DIR = str(_config.processing.processed_data_dir)
MSEEDINDEX_EXECUTABLE = _config.processing.mseedindex_executable
SQLALCHEMY_DATABASE_URI = _config.database.sqlalchemy_database_uri.get_secret_value()
DATABASE_BACKEND = _config.database.backend
FLASK_ENV = _config.flask_env
DEBUG = _config.flask_env == "development"
# ... etc
```

**Import Compatibility**:
```python
# Old code continues to work:
from noiz.settings import PROCESSED_DATA_DIR
from noiz.settings import SQLALCHEMY_DATABASE_URI
```

## Non-Database Configuration

**Important Note**: This data model describes application-level configuration loaded from environment variables. It does NOT cover:
- Processing parameters (DatachunkParams, CrosscorrelationParams, etc.) - these remain as database entities per Constitution Principle II
- User-defined processing configurations - these remain in TOML → database workflow
- Runtime processing state - this is managed by processing workflows

The configuration system refactor targets only the application bootstrap configuration (database connection, data directories, logging), not the scientific processing parameters which are correctly stored in the database.

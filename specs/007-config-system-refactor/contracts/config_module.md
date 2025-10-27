# Contract: Configuration Module API

**Module**: `noiz.config`
**Purpose**: Provide type-safe, validated access to application configuration
**Version**: 1.0.0

## Public API

### Classes

#### `NoizConfig`

**Base Class**: `pydantic_settings.BaseSettings`

**Purpose**: Root configuration object aggregating all application settings

**Constructor**:
```python
def __init__(self, **kwargs) -> None:
    """
    Initialize configuration from environment variables.

    Environment variables are read with NOIZ_ prefix.
    Validation occurs during initialization.

    :raises pydantic.ValidationError: If configuration is invalid
    :raises ValueError: If directory validation fails or executable not found
    """
```

**Public Attributes**:
```python
database: DatabaseConfig           # Database connection configuration
processing: ProcessingConfig       # Data processing configuration
flask_env: Literal["development", "production"]  # Flask environment
loglevel: str                      # Logging level (loguru-compatible)
```

**Public Methods**:
```python
def to_flask_config(self) -> dict[str, Any]:
    """
    Generate Flask-compatible configuration dictionary.

    Includes derived values like DEBUG, SQLALCHEMY_TRACK_MODIFICATIONS.
    Masks sensitive values (passwords displayed as ******).

    :return: Dictionary suitable for app.config.from_mapping()
    :rtype: dict[str, Any]
    """

def __repr__(self) -> str:
    """
    String representation with sensitive values masked.

    :return: Masked string representation
    :rtype: str
    """
```

**Validation Contracts**:
- MUST validate all nested configurations
- MUST emit INFO log on successful validation
- MUST emit ERROR log with actionable message on validation failure
- MUST mask sensitive fields in all log output

---

#### `DatabaseConfig`

**Base Class**: `pydantic.BaseModel` (not BaseSettings - nested)

**Purpose**: Database connection configuration supporting PostgreSQL and SQLite

**Public Attributes**:
```python
backend: Literal["postgresql", "sqlite"]  # Default: "postgresql"
database_url: SecretStr | None            # Complete URI (takes precedence)
postgres_host: str | None                 # PostgreSQL host
postgres_port: int | None                 # PostgreSQL port
postgres_user: str | None                 # PostgreSQL username
postgres_password: SecretStr | None       # PostgreSQL password (masked)
postgres_db: str | None                   # PostgreSQL database name
```

**Computed Properties**:
```python
@property
def sqlalchemy_database_uri(self) -> SecretStr:
    """
    Compute SQLAlchemy database URI.

    Precedence:
    1. database_url if provided
    2. Constructed from postgres_* fields for PostgreSQL
    3. "sqlite:///noiz.db" default for SQLite if database_url not provided

    :return: Database URI as SecretStr (masked in logs)
    :rtype: SecretStr
    :raises ValueError: If PostgreSQL backend missing required fields
    """
```

**Validation Contracts**:
- PostgreSQL: MUST require database_url OR all postgres_* fields
- SQLite: MUST default to "sqlite:///noiz.db" if database_url not provided
- MUST warn if both database_url and postgres_* provided (URI takes precedence)
- MUST validate postgres_port in range 1-65535 if provided

---

#### `ProcessingConfig`

**Base Class**: `pydantic.BaseModel` (not BaseSettings - nested)

**Purpose**: Data processing configuration and validation

**Public Attributes**:
```python
processed_data_dir: Path          # Directory for processed data output
mseedindex_executable: str        # Path to mseedindex or command name
```

**Validation Contracts**:

`processed_data_dir`:
- If path doesn't exist: MUST auto-create (WARN if parent directories created)
- If path exists as file: MUST fail with clear error
- If path exists as non-empty directory: MUST fail with error about naming conflicts
- If path exists as empty directory: MUST accept silently
- Created directories MUST have appropriate permissions (0o755)

`mseedindex_executable`:
- MUST validate executable exists as file or in PATH
- MUST fail with clear error if not found

---

### Functions

#### `get_config()`

```python
def get_config(reload: bool = False) -> NoizConfig:
    """
    Get application configuration singleton.

    On first call, loads configuration from environment.
    Subsequent calls return cached instance unless reload=True.

    This supports Flask's create_app() pattern where config may be
    reloaded if environment variables change between calls.

    :param reload: Force reload from environment (for testing/notebooks)
    :type reload: bool
    :return: Application configuration
    :rtype: NoizConfig
    :raises pydantic.ValidationError: If configuration invalid
    :raises ValueError: If directory/executable validation fails

    Example:
        # First call - loads from env
        config = get_config()

        # Subsequent calls - returns cached
        config = get_config()

        # Force reload (notebooks)
        config = get_config(reload=True)
    """
```

---

## Environment Variable Contracts

### Required Variables

| Variable | Type | Purpose | Validation |
|----------|------|---------|------------|
| `NOIZ_PROCESSED_DATA_DIR` | Path | Processed data output directory | Must be creatable/writable; directory rules apply |
| `NOIZ_MSEEDINDEX_EXECUTABLE` | String | Path to mseedindex | Must be executable file or in PATH |

**Database Connection** (choose one method):

**Method 1: Complete URI**
| Variable | Type | Purpose | Validation |
|----------|------|---------|------------|
| `NOIZ_DATABASE_URL` | String (URI) | Complete database connection URI | Must be valid PostgreSQL or SQLite URI |

**Method 2: Individual Parameters** (PostgreSQL only)
| Variable | Type | Purpose | Validation |
|----------|------|---------|------------|
| `NOIZ_POSTGRES_HOST` | String | Database host | Required for PostgreSQL if no DATABASE_URL |
| `NOIZ_POSTGRES_PORT` | Integer | Database port | Must be 1-65535; required for PostgreSQL |
| `NOIZ_POSTGRES_USER` | String | Database username | Required for PostgreSQL if no DATABASE_URL |
| `NOIZ_POSTGRES_PASSWORD` | String | Database password | Required for PostgreSQL if no DATABASE_URL; masked in logs |
| `NOIZ_POSTGRES_DB` | String | Database name | Required for PostgreSQL if no DATABASE_URL |

### Optional Variables

| Variable | Type | Default | Purpose | Validation |
|----------|------|---------|---------|------------|
| `NOIZ_DATABASE_BACKEND` | String | "postgresql" | Backend selection | Must be "postgresql" or "sqlite" |
| `NOIZ_FLASK_ENV` | String | "development" | Flask environment | Must be "development" or "production" |
| `NOIZ_LOGLEVEL` | String | "INFO" | Logging level | Must be valid loguru level |

### Precedence Rules

1. **Database Connection**: `NOIZ_DATABASE_URL` > `NOIZ_POSTGRES_*` (URI-first)
2. If both provided: WARN user but use DATABASE_URL

---

## Error Contracts

### ValidationError

**Type**: `pydantic.ValidationError`

**Raised When**:
- Missing required environment variables
- Invalid field types (e.g., non-integer port)
- Invalid field values (e.g., invalid backend)
- Cross-field validation failures

**Format**:
```python
try:
    config = NoizConfig()
except ValidationError as e:
    # e.errors() returns list of error dicts:
    # [
    #   {
    #     "loc": ("database", "postgres_host"),
    #     "msg": "field required",
    #     "type": "value_error.missing"
    #   }
    # ]
```

**Log Format**:
```
ERROR: Configuration validation failed
  Field: <field_path>
  Problem: <error_message>
  Action: <how_to_fix>
```

### ValueError

**Raised When**:
- Directory validation fails (file, non-empty)
- Executable not found
- Invalid URI format

**Format**:
```python
raise ValueError(
    "Configuration Error: NOIZ_PROCESSED_DATA_DIR\n"
    "  Problem: Path exists but is a file, not a directory\n"
    "  Provided: /path/to/file.txt\n"
    "  Action: Set NOIZ_PROCESSED_DATA_DIR to a directory path"
)
```

---

## Logging Contracts

All logs emitted via loguru respecting `NOIZ_LOGLEVEL`.

### INFO Level

**When**: Successful configuration load
**Format**: `"Configuration loaded successfully (backend: {backend}, flask_env: {flask_env})"`

### WARN Level

**When**: Non-critical issues
**Examples**:
- `"Created parent directories for NOIZ_PROCESSED_DATA_DIR: {path}"`
- `"Both NOIZ_DATABASE_URL and NOIZ_POSTGRES_* provided; using DATABASE_URL (URI-first precedence)"`

### ERROR Level

**When**: Validation failures
**Format**:
```
Configuration validation failed: <field>
  Problem: <description>
  Action: <how_to_fix>
```

**Sensitive Field Masking**:
- Passwords MUST be masked as "******"
- Database URIs MUST have password component masked

---

## Flask Integration Contract

### `app.config` Population

```python
from flask import Flask
from noiz.config import get_config

app = Flask(__name__)
config = get_config()

# Method 1: Bulk update (recommended)
app.config.from_mapping(config.to_flask_config())

# Method 2: Store config object
app.config['NOIZ_CONFIG'] = config
```

### Required Flask Config Keys

`to_flask_config()` MUST return dict with these keys:

| Key | Type | Derivation |
|-----|------|------------|
| `SQLALCHEMY_DATABASE_URI` | str | From `database.sqlalchemy_database_uri.get_secret_value()` |
| `PROCESSED_DATA_DIR` | str | From `processing.processed_data_dir` as string |
| `MSEEDINDEX_EXECUTABLE` | str | From `processing.mseedindex_executable` |
| `FLASK_ENV` | str | From `flask_env` |
| `DEBUG` | bool | True if `flask_env == "development"`, else False |
| `SQLALCHEMY_TRACK_MODIFICATIONS` | bool | Always False |
| `CACHE_TYPE` | str | Always "simple" |
| `DEBUG_TB_ENABLED` | bool | Same as `DEBUG` |
| `DEBUG_TB_INTERCEPT_REDIRECTS` | bool | Always False |

---

## Backward Compatibility Contract

### Module: `noiz.settings`

**Purpose**: Maintain backward compatibility with existing imports

**Exposed Variables**:
```python
# From config.database
SQLALCHEMY_DATABASE_URI: str
DATABASE_BACKEND: str
POSTGRES_HOST: str | None
POSTGRES_PORT: int | None
POSTGRES_USER: str | None
POSTGRES_PASSWORD: str | None  # Unmasked! (backward compat only)
POSTGRES_DB: str | None

# From config.processing
PROCESSED_DATA_DIR: str
MSEEDINDEX_EXECUTABLE: str

# From config
FLASK_ENV: str
DEBUG: bool

# Flask-specific (derived)
SQLALCHEMY_TRACK_MODIFICATIONS: bool
CACHE_TYPE: str
DEBUG_TB_ENABLED: bool
DEBUG_TB_INTERCEPT_REDIRECTS: bool
```

**Implementation**:
```python
from noiz.config import get_config

_config = get_config()

# Expose individual values
SQLALCHEMY_DATABASE_URI = _config.database.sqlalchemy_database_uri.get_secret_value()
PROCESSED_DATA_DIR = str(_config.processing.processed_data_dir)
# ... etc
```

**Deprecation Note**: These module-level variables are provided for backward compatibility only. New code should use `get_config()` directly.

---

## Testing Contract

### Test Fixtures

```python
import pytest
from noiz.config import NoizConfig

@pytest.fixture
def test_config(tmp_path, monkeypatch):
    """
    Provide test-appropriate configuration.

    Sets up temporary processed data directory.
    Configures SQLite database.
    """
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()

    monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
    monkeypatch.setenv("NOIZ_DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
    monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

    return NoizConfig()
```

### Mock Patterns

**DO NOT** mock environment variables directly (settings load before fixtures).

**DO** use parametrized fixtures:
```python
@pytest.fixture
def config_with_overrides(request, test_config):
    """Apply test-specific overrides."""
    overrides = getattr(request, 'param', {})
    # Apply overrides via object.__setattr__ (frozen models)
    return test_config
```

---

## Version History

- **1.0.0** (2025-10-25): Initial contract definition

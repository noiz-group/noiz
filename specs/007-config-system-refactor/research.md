# Research: Pydantic-Settings v2.x Configuration Management Best Practices

**Date**: 2025-10-25
**Context**: Configuration system refactor for Noiz seismic processing application
**Purpose**: Research production-ready patterns for pydantic-settings v2.x to inform implementation

## Executive Summary

This research evaluates pydantic-settings v2.x patterns for building a production-ready configuration system. The selected approach uses `SettingsConfigDict` for configuration, nested `BaseModel` sub-models for organization, `SecretStr` for password masking, `@field_validator` and `@model_validator` for validation logic, `model_dump()` for Flask integration, and attribute reset fixtures for pytest testing. This approach provides type safety, clear validation, and maintainable configuration management while integrating cleanly with Flask's app factory pattern.

## 1. BaseSettings Configuration

### Decision

Use `SettingsConfigDict` in the `model_config` attribute for all configuration options including `env_prefix`, `case_sensitive`, and `.env` file support.

### Rationale

In pydantic-settings v2, the configuration pattern migrated from an inner `Config` class to a `model_config` attribute using `SettingsConfigDict`. This approach:
- Provides clear, centralized configuration with type hints
- Enables IDE autocomplete for configuration options
- Aligns with Pydantic v2's architectural direction
- Supports all required features (env prefix, env file loading, case sensitivity)
- Is the officially documented v2 pattern

### Alternatives Considered

**Option A: Inner Config class (v1 pattern)**
- Deprecated in v2; migration guides recommend SettingsConfigDict
- Less type-safe than v2 approach
- Would require future migration when v1 compatibility is removed

**Option B: No configuration object**
- Cannot customize environment variable parsing, case sensitivity, or file loading
- Forces hardcoded defaults that don't meet Noiz requirements

### Code Examples

```python
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class NoizConfig(BaseSettings):
    """Central configuration for Noiz application.

    All environment variables must be prefixed with NOIZ_ to avoid
    conflicts with other applications.
    """
    model_config = SettingsConfigDict(
        env_prefix='NOIZ_',           # All vars prefixed with NOIZ_
        case_sensitive=False,          # NOIZ_DEBUG == noiz_debug
        env_file='.env',               # Support .env files for local dev
        env_file_encoding='utf-8',     # Explicit encoding
        env_nested_delimiter='__',     # Support NOIZ_POSTGRES__HOST syntax
        validate_default=True,         # Validate default values (enabled by default)
        extra='ignore',                # Ignore unknown env vars (v1 compat)
    )

    # Database configuration
    database_backend: str = Field(
        default='postgresql',
        description="Database backend: 'postgresql' or 'sqlite'"
    )

    # Processing configuration
    processed_data_dir: str = Field(
        description="Directory for processed data output (required)"
    )

    mseedindex_executable: str = Field(
        description="Path to mseedindex executable (required)"
    )

    # Flask configuration
    flask_env: str = Field(
        default='development',
        description="Flask environment: 'development' or 'production'"
    )

    loglevel: str = Field(
        default='INFO',
        description="Logging level (DEBUG, INFO, WARN, ERROR)"
    )
```

**Key Configuration Options**:
- `env_prefix`: Prepends string to all environment variable names (e.g., `NOIZ_`)
- `case_sensitive`: Controls case-insensitive matching (default: False on Linux/macOS, has no effect on Windows)
- `env_file`: Path(s) to .env files; can be string, tuple, or list (later files override earlier)
- `env_file_encoding`: Character encoding for dotenv files (defaults to OS encoding)
- `env_nested_delimiter`: Enables parsing like `NOIZ_POSTGRES__HOST` into nested structures
- `validate_default`: Validates default field values (enabled by default in BaseSettings, unlike BaseModel)
- `extra='ignore'`: Allows unknown environment variables (useful for backward compatibility)

**Priority Order** (highest to lowest):
1. Explicitly passed init arguments
2. OS Environment variables
3. Secrets directory
4. .env files
5. Default values

## 2. Nested Configuration

### Decision

Use nested `BaseModel` sub-models (not `BaseSettings`) for organizing related configuration with `env_nested_delimiter='__'` to support environment variable parsing.

### Rationale

Pydantic-settings v2 officially recommends that sub-models inherit from `pydantic.BaseModel` rather than `BaseSettings` to avoid unexpected initialization behavior. The `env_nested_delimiter` setting enables parsing environment variables like `NOIZ_POSTGRES__HOST` into nested structures. This approach:
- Provides clear logical grouping (database config, processing config, Flask config)
- Improves code organization and discoverability
- Enables validation at the sub-model level
- Supports both flat environment variables and nested JSON parsing
- Prevents initialization issues from nested BaseSettings

### Alternatives Considered

**Option A: Flat configuration with no nesting**
- Simpler implementation but less organized
- All fields at same level makes it harder to understand relationships
- Still functional but reduces maintainability

**Option B: Nested BaseSettings sub-models**
- Explicitly discouraged by pydantic-settings documentation
- Can cause unexpected initialization behavior
- Sub-models may initialize separately and collect values differently

**Option C: Prefix-based grouping without nested models**
- Fields like `postgres_host`, `postgres_port`, etc. grouped by naming
- Less type-safe than actual nested models
- Loses validation benefits of sub-models

### Code Examples

```python
from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class DatabaseConfig(BaseModel):
    """Database connection configuration.

    Supports both PostgreSQL (with individual params or URI) and SQLite.
    """
    backend: str = Field(
        default='postgresql',
        description="Database backend: 'postgresql' or 'sqlite'"
    )

    # PostgreSQL individual connection parameters
    host: str = Field(default='', description="PostgreSQL host")
    port: str = Field(default='', description="PostgreSQL port")
    user: str = Field(default='', description="PostgreSQL user")
    password: SecretStr = Field(default='', description="PostgreSQL password")
    db: str = Field(default='', description="PostgreSQL database name")

    # Connection URI (takes precedence if set)
    url: Optional[str] = Field(
        default=None,
        description="Database connection URI (takes precedence over individual params)"
    )

    @property
    def sqlalchemy_database_uri(self) -> str:
        """Compute SQLAlchemy database URI with precedence rules."""
        if self.url:
            return self.url

        if self.backend == 'sqlite':
            return 'sqlite:///noiz.db'

        # PostgreSQL from individual params
        if all([self.host, self.port, self.user, self.password, self.db]):
            pwd = self.password.get_secret_value()
            return f"postgresql+psycopg2://{self.user}:{pwd}@{self.host}:{self.port}/{self.db}"

        raise ValueError(
            "Either NOIZ_DATABASE_URL or all of "
            "(NOIZ_POSTGRES__HOST, NOIZ_POSTGRES__PORT, NOIZ_POSTGRES__USER, "
            "NOIZ_POSTGRES__PASSWORD, NOIZ_POSTGRES__DB) must be provided"
        )


class ProcessingConfig(BaseModel):
    """Processing pipeline configuration."""
    data_dir: str = Field(
        description="Directory for processed data output (required)"
    )
    mseedindex_executable: str = Field(
        description="Path to mseedindex executable (required)"
    )


class NoizConfig(BaseSettings):
    """Central configuration for Noiz application."""
    model_config = SettingsConfigDict(
        env_prefix='NOIZ_',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',  # Enables NOIZ_POSTGRES__HOST parsing
        validate_default=True,
        extra='ignore',
    )

    # Nested configuration sub-models
    postgres: DatabaseConfig = Field(
        default_factory=DatabaseConfig,
        description="Database configuration"
    )

    processing: ProcessingConfig = Field(
        description="Processing configuration"
    )

    flask_env: str = Field(default='development')
    loglevel: str = Field(default='INFO')
```

**Environment Variable Examples**:
```bash
# Nested delimiter approach (recommended for clarity)
export NOIZ_POSTGRES__HOST=localhost
export NOIZ_POSTGRES__PORT=5432
export NOIZ_POSTGRES__USER=noiz
export NOIZ_POSTGRES__PASSWORD=secret
export NOIZ_POSTGRES__DB=noiz

# Flat approach (also supported)
export NOIZ_PROCESSING__DATA_DIR=/data/processed
export NOIZ_PROCESSING__MSEEDINDEX_EXECUTABLE=mseedindex

# JSON parsing (alternative for complex nested structures)
export NOIZ_POSTGRES='{"host": "localhost", "port": "5432", "user": "noiz"}'
```

**Important**: Nested environment variables take precedence over top-level JSON environment variables.

## 3. Secret Masking

### Decision

Use `SecretStr` from `pydantic` (not pydantic-settings) for password and secret fields to automatically mask values in logs, repr, and string conversion.

### Rationale

`SecretStr` is a Pydantic built-in type that:
- Automatically masks values when printed or logged (shows `**********`)
- Requires explicit `.get_secret_value()` call to access the actual value
- Serves as a constant reminder to developers they're handling sensitive data
- Works seamlessly with pydantic-settings BaseSettings
- Prevents accidental credential exposure in logs, error messages, or debug output

Note: There is a known issue where validation errors may expose SecretStr values in plaintext. This can be mitigated by using custom error handling that strips sensitive data from validation error messages.

### Alternatives Considered

**Option A: Plain strings with custom repr**
- Requires manual implementation of masking logic
- Easy to forget when adding new secret fields
- More error-prone than built-in solution

**Option B: Custom Secret type**
- Reinvents the wheel; SecretStr is well-tested
- Requires maintenance and testing
- No advantage over built-in solution

**Option C: No masking**
- Unacceptable security risk
- Violates security best practices
- Could expose credentials in logs during troubleshooting

### Code Examples

```python
from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from loguru import logger

class DatabaseConfig(BaseModel):
    """Database connection configuration with secret masking."""

    # Regular fields
    host: str = Field(default='', description="PostgreSQL host")
    port: str = Field(default='', description="PostgreSQL port")
    user: str = Field(default='', description="PostgreSQL user")
    db: str = Field(default='', description="PostgreSQL database name")

    # Secret fields (masked in logs/repr)
    password: SecretStr = Field(
        default=SecretStr(''),
        description="PostgreSQL password (masked in logs)"
    )

    url: Optional[SecretStr] = Field(
        default=None,
        description="Database connection URI (masked in logs, takes precedence)"
    )

    @property
    def sqlalchemy_database_uri(self) -> str:
        """Compute SQLAlchemy database URI with precedence rules."""
        # URI-first precedence
        if self.url is not None:
            # Access secret value explicitly
            return self.url.get_secret_value()

        # Build from individual params
        if self.backend == 'postgresql':
            if all([self.host, self.port, self.user, self.password, self.db]):
                # Access secret value explicitly
                pwd = self.password.get_secret_value()
                return f"postgresql+psycopg2://{self.user}:{pwd}@{self.host}:{self.port}/{self.db}"
            raise ValueError("PostgreSQL backend requires connection parameters")

        return 'sqlite:///noiz.db'

    def log_safe_config(self) -> dict:
        """Return config dict with secrets masked for logging."""
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'db': self.db,
            'password': '******' if self.password else None,
            'url': '******' if self.url else None,
        }


class NoizConfig(BaseSettings):
    """Central configuration with secret masking."""
    model_config = SettingsConfigDict(
        env_prefix='NOIZ_',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
        validate_default=True,
        extra='ignore',
    )

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)

    def log_config(self) -> None:
        """Log configuration with secrets masked."""
        logger.info("Configuration loaded")
        logger.info(f"Database backend: {self.database.backend}")
        logger.info(f"Database config: {self.database.log_safe_config()}")


# Usage example
config = NoizConfig()

# Automatic masking
print(config.database.password)  # Output: **********
print(config.database.url)       # Output: **********

# Explicit access when needed
actual_password = config.database.password.get_secret_value()
actual_url = config.database.url.get_secret_value() if config.database.url else None

# Safe logging
config.log_config()  # Passwords shown as ******
```

**Security Notes**:
- Never log `get_secret_value()` output
- Use `log_safe_config()` methods for debugging
- Be aware validation errors may expose secrets in error messages (handle with custom error formatting)
- Consider using `secrets_dir` for file-based secret loading in production

## 4. Validation

### Decision

Use `@field_validator` for single-field validation (mode='after' by default) and `@model_validator(mode='after')` for cross-field validation and business logic that depends on multiple fields.

### Rationale

Pydantic v2 provides two complementary validation decorators:
- `@field_validator`: Validates individual fields after type coercion (mode='after') or before (mode='before')
- `@model_validator`: Validates the entire model after all fields are validated, ideal for cross-field dependencies

This approach:
- Provides clear separation between field-level and model-level validation
- Enables access to other fields via `ValidationInfo.data` or model instance
- Supports custom validation logic (directory existence, precedence rules, business constraints)
- Integrates with Pydantic's validation error reporting
- Allows validation to run eagerly at configuration load time

### Alternatives Considered

**Option A: Property-based validation**
- Validates lazily on access rather than eagerly at load time
- Doesn't integrate with Pydantic's validation error reporting
- Can't prevent invalid configuration from being created

**Option B: Manual validation in __init__**
- Bypasses Pydantic's validation framework
- Loses automatic error message generation
- More error-prone and harder to test

**Option C: External validation function**
- Separates validation from model definition
- Requires manual invocation (easy to forget)
- Doesn't integrate with Pydantic's ValidationError

### Code Examples

```python
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator, ValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Optional, Self
from loguru import logger
import os

class ProcessingConfig(BaseModel):
    """Processing pipeline configuration with validation."""

    data_dir: str = Field(
        description="Directory for processed data output (required)"
    )

    mseedindex_executable: str = Field(
        description="Path to mseedindex executable (required)"
    )

    @field_validator('data_dir', mode='after')
    @classmethod
    def validate_data_dir(cls, v: str, info: ValidationInfo) -> str:
        """Validate data directory exists or can be created.

        Rules:
        - Auto-create if missing (warn if parent directories created)
        - Fail if path exists as file
        - Fail if path exists as non-empty directory (prevents conflicts)
        - Accept empty directories silently
        """
        path = Path(v)

        # Path exists as file - fail
        if path.exists() and path.is_file():
            raise ValueError(
                f"NOIZ_PROCESSING__DATA_DIR points to a file, not a directory: {v}"
            )

        # Path exists as non-empty directory - fail
        if path.exists() and path.is_dir() and any(path.iterdir()):
            raise ValueError(
                f"NOIZ_PROCESSING__DATA_DIR points to non-empty directory: {v}. "
                f"This could cause naming conflicts. Use an empty or new directory."
            )

        # Path exists as empty directory - accept silently
        if path.exists() and path.is_dir():
            return v

        # Path doesn't exist - create it
        try:
            # Check if we need to create parent directories
            needs_parents = not path.parent.exists()
            path.mkdir(parents=True, exist_ok=True)

            if needs_parents:
                logger.warning(
                    f"Created data directory and parent directories: {v}"
                )
            else:
                logger.info(f"Created data directory: {v}")

            return v
        except OSError as e:
            raise ValueError(
                f"Failed to create NOIZ_PROCESSING__DATA_DIR: {v}. Error: {e}"
            )

    @field_validator('mseedindex_executable', mode='after')
    @classmethod
    def validate_mseedindex_executable(cls, v: str) -> str:
        """Validate mseedindex executable exists and is executable."""
        # Try as absolute path
        if Path(v).is_file() and os.access(v, os.X_OK):
            return v

        # Try finding in PATH
        from shutil import which
        executable_path = which(v)
        if executable_path:
            return executable_path

        raise ValueError(
            f"NOIZ_MSEEDINDEX_EXECUTABLE not found or not executable: {v}. "
            f"Provide absolute path or ensure it's in PATH."
        )


class DatabaseConfig(BaseModel):
    """Database configuration with URI precedence validation."""

    backend: str = Field(default='postgresql')
    host: str = Field(default='')
    port: str = Field(default='')
    user: str = Field(default='')
    password: SecretStr = Field(default=SecretStr(''))
    db: str = Field(default='')
    url: Optional[SecretStr] = Field(default=None)

    @field_validator('backend', mode='after')
    @classmethod
    def validate_backend(cls, v: str) -> str:
        """Validate database backend is supported."""
        valid_backends = {'postgresql', 'sqlite'}
        if v.lower() not in valid_backends:
            raise ValueError(
                f"Invalid database backend: {v}. Must be one of {valid_backends}"
            )
        return v.lower()

    @model_validator(mode='after')
    def validate_connection_params(self) -> Self:
        """Validate connection parameters based on backend and precedence rules.

        Rules:
        - URI (url) takes precedence if set
        - PostgreSQL requires either URI or all individual params
        - SQLite can use default URI if none provided
        """
        # SQLite backend - always valid (has default)
        if self.backend == 'sqlite':
            return self

        # PostgreSQL backend
        if self.backend == 'postgresql':
            # URI provided - valid (URI-first precedence)
            if self.url:
                logger.info("Using NOIZ_DATABASE_URL for connection")
                return self

            # Check individual params
            params = [self.host, self.port, self.user, self.db]
            pwd = self.password.get_secret_value()

            if all(params) and pwd:
                logger.info("Using NOIZ_POSTGRES__* variables for connection")
                return self

            # Missing params
            missing = []
            if not self.host: missing.append('NOIZ_POSTGRES__HOST')
            if not self.port: missing.append('NOIZ_POSTGRES__PORT')
            if not self.user: missing.append('NOIZ_POSTGRES__USER')
            if not pwd: missing.append('NOIZ_POSTGRES__PASSWORD')
            if not self.db: missing.append('NOIZ_POSTGRES__DB')

            raise ValueError(
                f"PostgreSQL backend requires either NOIZ_DATABASE_URL or all of "
                f"{', '.join(missing)}"
            )

        return self


class NoizConfig(BaseSettings):
    """Central configuration with comprehensive validation."""
    model_config = SettingsConfigDict(
        env_prefix='NOIZ_',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
        validate_default=True,
        extra='ignore',
    )

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    processing: ProcessingConfig
    flask_env: str = Field(default='development')
    loglevel: str = Field(default='INFO')

    @field_validator('loglevel', mode='after')
    @classmethod
    def validate_loglevel(cls, v: str) -> str:
        """Validate log level is valid."""
        valid_levels = {'DEBUG', 'INFO', 'WARNING', 'WARN', 'ERROR', 'CRITICAL'}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            logger.warning(
                f"Invalid log level: {v}. Defaulting to INFO. "
                f"Valid levels: {', '.join(valid_levels)}"
            )
            return 'INFO'
        return v_upper

    @field_validator('flask_env', mode='after')
    @classmethod
    def validate_flask_env(cls, v: str) -> str:
        """Validate Flask environment."""
        valid_envs = {'development', 'production'}
        v_lower = v.lower()
        if v_lower not in valid_envs:
            logger.warning(
                f"Invalid Flask environment: {v}. Defaulting to development. "
                f"Valid values: {', '.join(valid_envs)}"
            )
            return 'development'
        return v_lower
```

**Validation Patterns**:
- **Field-level**: Use `@field_validator` for validating individual fields (file existence, enum values, format checking)
- **Cross-field**: Use `@model_validator(mode='after')` for validation requiring multiple fields (URI precedence, mutual exclusivity)
- **Mode='after'**: Default, runs after type coercion, receives typed values
- **Mode='before'**: Runs before type coercion, receives raw input (useful for custom parsing)
- **ValidationInfo**: Access other validated fields via `info.data['field_name']`

## 5. Flask Integration

### Decision

Use `model_dump()` to convert the Pydantic settings model to a dictionary, then populate Flask's `app.config` using `app.config.from_mapping()` or `app.config.update()`.

### Rationale

Flask's config system expects a dictionary-like interface. Pydantic v2's `model_dump()` method:
- Converts the settings model to a Python dictionary
- Can transform field names using aliases (e.g., snake_case to UPPER_CASE)
- Supports exclusion of specific fields
- Integrates cleanly with Flask's config loading patterns
- Allows settings to remain as source of truth while Flask config gets derived values

This approach maintains separation of concerns: Pydantic handles validation and loading, Flask handles application-level config access.

### Alternatives Considered

**Option A: Direct settings object as Flask config**
- Flask config expects dict-like interface; would require custom implementation
- Loses Flask's built-in config patterns and utilities
- More complex to maintain

**Option B: Duplicate configuration in Flask and Pydantic**
- Creates two sources of truth
- Requires manual synchronization
- Error-prone and hard to maintain

**Option C: Use .dict() method (v1 pattern)**
- Deprecated in v2 (replaced by model_dump())
- Would require future migration

### Code Examples

```python
from flask import Flask
from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from loguru import logger

class DatabaseConfig(BaseModel):
    """Database configuration."""
    backend: str = Field(default='postgresql')
    host: str = Field(default='')
    port: str = Field(default='')
    user: str = Field(default='')
    password: SecretStr = Field(default=SecretStr(''))
    db: str = Field(default='')
    url: Optional[SecretStr] = Field(default=None)

    @property
    def sqlalchemy_database_uri(self) -> str:
        """Compute SQLAlchemy database URI."""
        if self.url:
            return self.url.get_secret_value()

        if self.backend == 'sqlite':
            return 'sqlite:///noiz.db'

        if all([self.host, self.port, self.user, self.password, self.db]):
            pwd = self.password.get_secret_value()
            return f"postgresql+psycopg2://{self.user}:{pwd}@{self.host}:{self.port}/{self.db}"

        raise ValueError("Database connection not configured")


class ProcessingConfig(BaseModel):
    """Processing configuration."""
    data_dir: str
    mseedindex_executable: str


class NoizConfig(BaseSettings):
    """Central Noiz configuration."""
    model_config = SettingsConfigDict(
        env_prefix='NOIZ_',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
    )

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    processing: ProcessingConfig
    flask_env: str = Field(default='development')
    loglevel: str = Field(default='INFO')

    def to_flask_config(self) -> dict:
        """Convert settings to Flask config dictionary.

        Generates Flask-specific config values derived from settings.
        """
        # Determine debug mode
        debug = (self.flask_env == 'development')

        flask_config = {
            # Database
            'SQLALCHEMY_DATABASE_URI': self.database.sqlalchemy_database_uri,
            'SQLALCHEMY_TRACK_MODIFICATIONS': False,

            # Flask settings
            'DEBUG': debug,
            'TESTING': False,
            'ENV': self.flask_env,

            # Debug toolbar
            'DEBUG_TB_ENABLED': debug,
            'DEBUG_TB_INTERCEPT_REDIRECTS': False,

            # Cache
            'CACHE_TYPE': 'simple',

            # Custom Noiz settings (for application access)
            'NOIZ_PROCESSED_DATA_DIR': self.processing.data_dir,
            'NOIZ_MSEEDINDEX_EXECUTABLE': self.processing.mseedindex_executable,
            'NOIZ_LOGLEVEL': self.loglevel,
        }

        return flask_config


def create_app() -> Flask:
    """Flask application factory with Pydantic settings integration.

    This pattern allows configuration to be reloaded when create_app()
    is called multiple times (notebook usage pattern).
    """
    # Load and validate configuration (reloads from environment each time)
    try:
        config = NoizConfig()
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise

    # Create Flask app
    app = Flask(__name__)

    # Populate Flask config from Pydantic settings
    flask_config = config.to_flask_config()
    app.config.from_mapping(flask_config)

    # Store settings object for direct access if needed
    app.config['NOIZ_CONFIG'] = config

    # Log configuration (with secrets masked)
    logger.info(f"Flask environment: {app.config['ENV']}")
    logger.info(f"Debug mode: {app.config['DEBUG']}")
    logger.info(f"Database backend: {config.database.backend}")
    logger.info(f"Processed data dir: {config.processing.data_dir}")

    # Initialize extensions (SQLAlchemy, etc.)
    # ... existing initialization code ...

    return app


# Usage examples

# CLI usage (single load at startup)
if __name__ == '__main__':
    app = create_app()
    app.run()


# Notebook usage (can reload between calls)
def notebook_example():
    """Example of notebook usage with config reload."""
    # First call
    app1 = create_app()
    with app1.app_context():
        from noiz.models import Component
        # ... do work ...

    # Change environment variables
    import os
    os.environ['NOIZ_LOGLEVEL'] = 'DEBUG'

    # Second call picks up new config
    app2 = create_app()
    # New loglevel is active
```

**Integration Patterns**:
- **App Factory**: Load config in `create_app()` for each invocation (supports reload)
- **Config Method**: Use `to_flask_config()` to generate Flask config dict with derived values
- **Direct Access**: Store settings object in `app.config['NOIZ_CONFIG']` for complex access patterns
- **Validation First**: Config validation happens before Flask initialization (fail fast)
- **Secrets Masking**: Use log-safe methods when logging config values

**Backward Compatibility**:
```python
# In src/noiz/settings.py (for backward compatibility)
from noiz.config import NoizConfig

# Create global config instance (loaded once per import)
_config = NoizConfig()

# Export individual values for backward compatibility
PROCESSED_DATA_DIR = _config.processing.data_dir
MSEEDINDEX_EXECUTABLE = _config.processing.mseedindex_executable
SQLALCHEMY_DATABASE_URI = _config.database.sqlalchemy_database_uri
DEBUG = (_config.flask_env == 'development')
# ... etc ...
```

## 6. Dynamic Reload

### Decision

Support dynamic reload by instantiating a new `NoizConfig()` object each time configuration is needed, rather than maintaining a singleton. For Flask's `create_app()` pattern, instantiate config on each call.

### Rationale

Pydantic-settings does not have a built-in `reload()` method. The recommended pattern for dynamic reload:
- Instantiate new settings object to re-read environment variables
- For Flask factory pattern: create new config in each `create_app()` call
- For CLI: single instantiation at startup (no reload needed)

This approach:
- Leverages Pydantic's initialization to re-read environment
- Keeps code simple (no custom reload logic)
- Supports notebook usage where environment changes between calls
- Maintains validation on every instantiation

Note: If `reload()` functionality is needed on an existing instance, calling `instance.__init__()` will re-parse environment variables, though this is not officially documented.

### Alternatives Considered

**Option A: Singleton pattern with no reload**
- Simpler but doesn't meet notebook reload requirement
- Would force users to restart kernel to change config

**Option B: Custom reload() method**
- Requires custom implementation and testing
- Pydantic doesn't provide this built-in
- Calling `__init__()` achieves the same result

**Option C: Configuration file watching**
- Overly complex for environment-variable based config
- Not needed for Noiz's usage patterns

### Code Examples

```python
from flask import Flask
from pydantic_settings import BaseSettings, SettingsConfigDict
from loguru import logger
import os

class NoizConfig(BaseSettings):
    """Noiz configuration supporting dynamic reload."""
    model_config = SettingsConfigDict(
        env_prefix='NOIZ_',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
    )

    loglevel: str = 'INFO'
    flask_env: str = 'development'
    # ... other fields ...


# Pattern 1: Flask app factory (creates new config each time)
def create_app() -> Flask:
    """Flask app factory supporting config reload.

    Each call creates a new config instance, picking up any
    environment variable changes since the last call.
    """
    # Create new config instance (reloads from environment)
    config = NoizConfig()

    app = Flask(__name__)
    app.config.from_mapping(config.to_flask_config())

    logger.info(f"App created with loglevel: {config.loglevel}")
    return app


# Pattern 2: CLI usage (single load, no reload needed)
def cli_main():
    """CLI entry point loads config once at startup."""
    config = NoizConfig()

    # Use config throughout CLI command execution
    logger.remove()
    logger.add(sys.stderr, level=config.loglevel)

    # ... CLI logic ...


# Pattern 3: Workaround for reloading existing instance (undocumented)
def reload_example():
    """Example of reloading an existing config instance.

    Note: This uses __init__() which is not officially documented
    for reload purposes. Prefer Pattern 1 (create new instance).
    """
    config = NoizConfig()
    print(f"Initial loglevel: {config.loglevel}")  # INFO

    # Change environment
    os.environ['NOIZ_LOGLEVEL'] = 'DEBUG'

    # Reload by calling __init__() again
    config.__init__()
    print(f"After reload: {config.loglevel}")  # DEBUG


# Pattern 4: Notebook usage demonstrating reload capability
def notebook_example():
    """Notebook usage showing config reload between create_app() calls."""
    # First notebook cell: Initial setup
    app1 = create_app()
    with app1.app_context():
        # Work with initial config
        pass

    # Second notebook cell: Change config
    os.environ['NOIZ_LOGLEVEL'] = 'DEBUG'
    os.environ['NOIZ_FLASK_ENV'] = 'production'

    # Third notebook cell: New config is picked up
    app2 = create_app()
    # This app uses DEBUG loglevel and production environment


# Pattern 5: Conditional reload in long-running process
class ConfigurableService:
    """Example of service that can reload config on demand."""

    def __init__(self):
        self.config = NoizConfig()

    def reload_config(self) -> None:
        """Reload configuration from environment variables.

        Call this after changing environment variables to pick up
        new values without restarting the service.
        """
        self.config = NoizConfig()
        logger.info("Configuration reloaded")
        logger.remove()
        logger.add(sys.stderr, level=self.config.loglevel)

    def process(self) -> None:
        """Process using current config."""
        logger.info(f"Processing with loglevel: {self.config.loglevel}")
```

**Reload Patterns**:
- **Flask Factory**: Create new `NoizConfig()` in each `create_app()` call (recommended for notebook usage)
- **CLI**: Single instantiation at startup (no reload needed)
- **Existing Instance**: Call `instance.__init__()` to re-parse (undocumented workaround)
- **Service Pattern**: Store config as instance variable, replace with new instance on reload

**Important Notes**:
- Reloading doesn't work for `.env` files (only environment variables)
- Validation runs on every instantiation (catches config errors early)
- Thread safety: Creating new instances is thread-safe; reloading existing instance is not

## 7. Testing Patterns

### Decision

Use pytest fixtures that reset settings attributes to defaults and allow parametrized overrides, rather than patching environment variables. The fixture preserves original state, applies test-specific overrides, and restores after test completion.

### Rationale

Environment variable patching fails with pydantic-settings because the `Settings` class initializes before pytest fixtures can intervene. The attribute reset pattern:
- Works reliably with pydantic-settings initialization order
- Provides clean test isolation
- Supports parametrized overrides for different test scenarios
- Validates overrides match expected types
- Restores original state after tests
- Eliminates test flakiness from external environment state

This pattern is recommended by the pydantic-settings testing community and documented in production guides.

### Alternatives Considered

**Option A: Environment variable patching with pytest-env**
- Doesn't work because settings initialize before patches apply
- Common anti-pattern that leads to test flakiness

**Option B: FastAPI dependency_overrides pattern**
- Only works for FastAPI apps (Noiz uses Flask)
- Requires wrapping settings in a dependency function

**Option C: Separate test config files**
- Requires maintaining multiple config definitions
- Doesn't support dynamic overrides per test

### Code Examples

```python
# tests/conftest.py
import pytest
from pytest import FixtureRequest
from typing import Iterator
from noiz.config import NoizConfig, get_config
from pathlib import Path
import tempfile

@pytest.fixture
def test_config(request: FixtureRequest) -> Iterator[NoizConfig]:
    """Fixture providing isolated config for testing.

    Resets all config fields to defaults, applies test-specific overrides,
    and restores original state after test completion.

    Usage:
        # Use defaults
        def test_with_defaults(test_config: NoizConfig):
            assert test_config.loglevel == "INFO"

        # Override specific values
        @pytest.mark.parametrize(
            "test_config",
            [{"loglevel": "DEBUG", "flask_env": "production"}],
            indirect=True
        )
        def test_with_overrides(test_config: NoizConfig):
            assert test_config.loglevel == "DEBUG"
    """
    # Load current config
    config = get_config()

    # Save original state for restoration
    original_state = config.model_dump()

    # Reset all fields to their default values
    for field_name, field_info in config.model_fields.items():
        if field_info.default is not None:
            setattr(config, field_name, field_info.default)
        elif field_info.default_factory is not None:
            setattr(config, field_name, field_info.default_factory())

    # Apply test-specific overrides from parametrize
    overrides = getattr(request, "param", {})
    for key, value in overrides.items():
        if not hasattr(config, key):
            raise ValueError(f"Unknown config field: {key}")
        setattr(config, key, value)

    # Provide config to test
    yield config

    # Restore original state after test
    for key, value in original_state.items():
        setattr(config, key, value)


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Provide temporary data directory for testing."""
    data_dir = tmp_path / "processed_data"
    data_dir.mkdir(parents=True)
    return data_dir


@pytest.fixture
def valid_test_config(test_config: NoizConfig, temp_data_dir: Path) -> NoizConfig:
    """Fixture providing fully valid test configuration.

    Sets all required fields with valid test values.
    """
    test_config.processing.data_dir = str(temp_data_dir)
    test_config.processing.mseedindex_executable = "/usr/bin/mseedindex"
    test_config.database.backend = "sqlite"
    test_config.database.url = "sqlite:///:memory:"
    return test_config


# tests/unit/test_config.py
import pytest
from noiz.config import NoizConfig, DatabaseConfig, ProcessingConfig
from pydantic import ValidationError
from pathlib import Path

class TestConfigDefaults:
    """Test default configuration values."""

    def test_default_flask_env(self, test_config: NoizConfig):
        """Default Flask environment should be development."""
        assert test_config.flask_env == "development"

    def test_default_loglevel(self, test_config: NoizConfig):
        """Default log level should be INFO."""
        assert test_config.loglevel == "INFO"

    def test_default_database_backend(self, test_config: NoizConfig):
        """Default database backend should be postgresql."""
        assert test_config.database.backend == "postgresql"


class TestConfigOverrides:
    """Test configuration overrides."""

    @pytest.mark.parametrize(
        "test_config",
        [{"loglevel": "DEBUG"}],
        indirect=True
    )
    def test_override_loglevel(self, test_config: NoizConfig):
        """Can override log level."""
        assert test_config.loglevel == "DEBUG"

    @pytest.mark.parametrize(
        "test_config",
        [{"flask_env": "production", "loglevel": "ERROR"}],
        indirect=True
    )
    def test_override_multiple_fields(self, test_config: NoizConfig):
        """Can override multiple fields."""
        assert test_config.flask_env == "production"
        assert test_config.loglevel == "ERROR"


class TestConfigValidation:
    """Test configuration validation logic."""

    def test_invalid_database_backend(self):
        """Invalid database backend should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            config = NoizConfig()
            config.database.backend = "mysql"
            config.database.model_validate(config.database.model_dump())

        assert "Invalid database backend" in str(exc_info.value)

    def test_missing_required_field(self):
        """Missing required field should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ProcessingConfig()  # Missing data_dir and mseedindex_executable

        assert "field required" in str(exc_info.value).lower()

    def test_data_dir_creation(self, tmp_path: Path, test_config: NoizConfig):
        """Data directory should be created if missing."""
        data_dir = tmp_path / "new_dir"
        assert not data_dir.exists()

        test_config.processing.data_dir = str(data_dir)
        # Trigger validation
        validated = ProcessingConfig.model_validate(
            {"data_dir": str(data_dir), "mseedindex_executable": "mseedindex"}
        )

        assert Path(validated.data_dir).exists()

    def test_data_dir_as_file_fails(self, tmp_path: Path):
        """Data directory pointing to file should fail validation."""
        file_path = tmp_path / "somefile.txt"
        file_path.write_text("test")

        with pytest.raises(ValidationError) as exc_info:
            ProcessingConfig(
                data_dir=str(file_path),
                mseedindex_executable="mseedindex"
            )

        assert "points to a file" in str(exc_info.value)


class TestFlaskIntegration:
    """Test Flask integration patterns."""

    def test_create_app_with_valid_config(self, valid_test_config: NoizConfig):
        """Create app with valid config should succeed."""
        from noiz.app import create_app

        app = create_app()

        assert app.config['SQLALCHEMY_DATABASE_URI']
        assert app.config['DEBUG'] is not None

    def test_config_reload_between_create_app_calls(
        self, valid_test_config: NoizConfig, monkeypatch
    ):
        """Config changes should be picked up between create_app calls."""
        from noiz.app import create_app
        import os

        # First call
        app1 = create_app()
        assert app1.config['NOIZ_LOGLEVEL'] == 'INFO'

        # Change environment
        monkeypatch.setenv('NOIZ_LOGLEVEL', 'DEBUG')

        # Second call picks up change
        app2 = create_app()
        assert app2.config['NOIZ_LOGLEVEL'] == 'DEBUG'


# tests/integration/test_config_integration.py
import pytest
import os
from noiz.config import NoizConfig

class TestEnvironmentVariableLoading:
    """Test loading from environment variables."""

    def test_load_from_env(self, monkeypatch):
        """Should load config from NOIZ_ prefixed environment variables."""
        monkeypatch.setenv('NOIZ_LOGLEVEL', 'DEBUG')
        monkeypatch.setenv('NOIZ_FLASK_ENV', 'production')

        config = NoizConfig()

        assert config.loglevel == 'DEBUG'
        assert config.flask_env == 'production'

    def test_nested_env_vars(self, monkeypatch, tmp_path):
        """Should load nested config from delimiter-separated env vars."""
        monkeypatch.setenv('NOIZ_POSTGRES__HOST', 'localhost')
        monkeypatch.setenv('NOIZ_POSTGRES__PORT', '5432')
        monkeypatch.setenv('NOIZ_PROCESSING__DATA_DIR', str(tmp_path))

        config = NoizConfig()

        assert config.database.host == 'localhost'
        assert config.database.port == '5432'

    def test_uri_precedence(self, monkeypatch, tmp_path):
        """DATABASE_URL should take precedence over individual params."""
        uri = "postgresql://user:pass@host:5432/db"
        monkeypatch.setenv('NOIZ_DATABASE_URL', uri)
        monkeypatch.setenv('NOIZ_POSTGRES__HOST', 'otherhost')
        monkeypatch.setenv('NOIZ_PROCESSING__DATA_DIR', str(tmp_path))

        config = NoizConfig()

        assert config.database.sqlalchemy_database_uri == uri
```

**Testing Patterns**:
- **Reset Fixture**: Resets config to defaults, applies overrides, restores after test
- **Parametrize**: Use `@pytest.mark.parametrize` with `indirect=True` for test-specific config
- **Isolation**: Each test gets clean config state independent of environment
- **Validation Testing**: Test ValidationError for invalid configurations
- **Integration Testing**: Use `monkeypatch.setenv()` for integration tests that verify environment loading

**Fixture Patterns**:
- `test_config`: Base fixture for config isolation
- `valid_test_config`: Fixture providing fully valid test config
- `temp_data_dir`: Fixture for temporary directories
- Compose fixtures for complex test scenarios

## 8. Handling URI-First Precedence

### Decision

Implement URI-first precedence using `@model_validator(mode='after')` on `DatabaseConfig` to check if `url` is set first, then fall back to individual connection parameters, with clear logging of which source is used.

### Rationale

The requirement specifies that if both `NOIZ_DATABASE_URL` and individual `NOIZ_POSTGRES_*` variables are provided, the URI takes precedence. This is best implemented as:
- A computed property `sqlalchemy_database_uri` that checks precedence
- Model-level validation that ensures required parameters are present
- Informational logging indicating which connection method is active

This approach:
- Makes precedence rules explicit and testable
- Provides clear error messages when configuration is incomplete
- Logs which connection method is being used (aids debugging)
- Supports both connection methods without duplication

### Code Examples

```python
from pydantic import BaseModel, Field, SecretStr, model_validator, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Self
from loguru import logger

class DatabaseConfig(BaseModel):
    """Database configuration with URI-first precedence.

    Precedence rules:
    1. NOIZ_DATABASE_URL (if set, this takes precedence)
    2. Individual NOIZ_POSTGRES__* variables
    3. SQLite default (if backend is 'sqlite')
    """

    backend: str = Field(
        default='postgresql',
        description="Database backend: 'postgresql' or 'sqlite'"
    )

    # Individual PostgreSQL connection parameters
    host: str = Field(default='', description="PostgreSQL host")
    port: str = Field(default='5432', description="PostgreSQL port")
    user: str = Field(default='', description="PostgreSQL username")
    password: SecretStr = Field(
        default=SecretStr(''),
        description="PostgreSQL password"
    )
    db: str = Field(default='', description="PostgreSQL database name")

    # Connection URI (takes precedence if set)
    url: Optional[SecretStr] = Field(
        default=None,
        validation_alias='DATABASE_URL',  # Also accept without nested delimiter
        description="Database connection URI (precedence over individual params)"
    )

    @field_validator('backend', mode='after')
    @classmethod
    def validate_backend(cls, v: str) -> str:
        """Validate database backend."""
        valid = {'postgresql', 'sqlite'}
        if v.lower() not in valid:
            raise ValueError(
                f"Invalid NOIZ_DATABASE_BACKEND: {v}. Must be one of {valid}"
            )
        return v.lower()

    @model_validator(mode='after')
    def validate_connection_params(self) -> Self:
        """Validate connection parameters with URI-first precedence."""
        # SQLite: always valid (has default)
        if self.backend == 'sqlite':
            if self.url:
                logger.info("Using NOIZ_DATABASE_URL for SQLite connection")
            else:
                logger.info("Using default SQLite connection (sqlite:///noiz.db)")
            return self

        # PostgreSQL: check URI first (precedence rule)
        if self.url:
            logger.info(
                "Using NOIZ_DATABASE_URL for PostgreSQL connection "
                "(takes precedence over NOIZ_POSTGRES__* variables)"
            )
            return self

        # PostgreSQL: check individual parameters
        pwd = self.password.get_secret_value() if self.password else ''
        required_params = {
            'NOIZ_POSTGRES__HOST': self.host,
            'NOIZ_POSTGRES__PORT': self.port,
            'NOIZ_POSTGRES__USER': self.user,
            'NOIZ_POSTGRES__PASSWORD': pwd,
            'NOIZ_POSTGRES__DB': self.db,
        }

        missing = [name for name, value in required_params.items() if not value]

        if not missing:
            logger.info("Using NOIZ_POSTGRES__* variables for connection")
            return self

        # Neither URI nor complete parameters provided
        raise ValueError(
            f"PostgreSQL backend requires either:\n"
            f"  1. NOIZ_DATABASE_URL (URI-first precedence), OR\n"
            f"  2. All of: {', '.join(missing)}\n"
            f"Missing: {', '.join(missing)}"
        )

    @property
    def sqlalchemy_database_uri(self) -> str:
        """Compute SQLAlchemy database URI with precedence rules.

        Precedence:
        1. url (NOIZ_DATABASE_URL) if set
        2. Individual parameters if all present
        3. SQLite default if backend is 'sqlite'

        Returns:
            Database connection URI string

        Raises:
            ValueError: If configuration is invalid
        """
        # URI-first precedence
        if self.url:
            return self.url.get_secret_value()

        # SQLite backend
        if self.backend == 'sqlite':
            return 'sqlite:///noiz.db'

        # PostgreSQL from individual parameters
        if all([self.host, self.port, self.user, self.password, self.db]):
            pwd = self.password.get_secret_value()
            return (
                f"postgresql+psycopg2://{self.user}:{pwd}@"
                f"{self.host}:{self.port}/{self.db}"
            )

        # Should never reach here due to model_validator, but explicit for safety
        raise ValueError(
            "Database connection not properly configured. "
            "This should have been caught during validation."
        )


class NoizConfig(BaseSettings):
    """Noiz configuration with URI-first database precedence."""
    model_config = SettingsConfigDict(
        env_prefix='NOIZ_',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
    )

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    # ... other fields ...


# Example usage and tests
def test_uri_precedence():
    """Test that DATABASE_URL takes precedence over individual params."""
    import os

    # Set both URI and individual params
    os.environ['NOIZ_DATABASE_URL'] = 'postgresql://uri_user:uri_pass@uri_host:5433/uri_db'
    os.environ['NOIZ_POSTGRES__HOST'] = 'param_host'
    os.environ['NOIZ_POSTGRES__PORT'] = '5432'
    os.environ['NOIZ_POSTGRES__USER'] = 'param_user'
    os.environ['NOIZ_POSTGRES__PASSWORD'] = 'param_pass'
    os.environ['NOIZ_POSTGRES__DB'] = 'param_db'

    config = NoizConfig()

    # URI should take precedence
    assert 'uri_host' in config.database.sqlalchemy_database_uri
    assert 'param_host' not in config.database.sqlalchemy_database_uri

    # Log message should indicate precedence
    # (check logs for: "Using NOIZ_DATABASE_URL for PostgreSQL connection")


def test_fallback_to_individual_params():
    """Test fallback to individual params when URI not set."""
    import os

    # Set only individual params (no URI)
    os.environ.pop('NOIZ_DATABASE_URL', None)
    os.environ['NOIZ_POSTGRES__HOST'] = 'localhost'
    os.environ['NOIZ_POSTGRES__PORT'] = '5432'
    os.environ['NOIZ_POSTGRES__USER'] = 'noiz'
    os.environ['NOIZ_POSTGRES__PASSWORD'] = 'secret'
    os.environ['NOIZ_POSTGRES__DB'] = 'noiz'

    config = NoizConfig()

    # Should use individual params
    assert 'localhost' in config.database.sqlalchemy_database_uri
    assert 'noiz' in config.database.sqlalchemy_database_uri
```

## 9. Directory Validation with Auto-Creation

### Decision

Use `@field_validator` on the `data_dir` field to implement the specified directory validation rules: auto-create if missing (warn if parents needed), fail if path is file or non-empty directory, accept empty directory silently.

### Rationale

The specification requires specific handling for `NOIZ_PROCESSED_DATA_DIR`:
- Auto-create to reduce setup friction
- Warn if parent directories are created (indicates potential config typo)
- Fail if path is file (prevents destructive behavior)
- Fail if non-empty directory (prevents naming conflicts from reprocessing same data)
- Accept empty directories silently (user explicitly prepared the location)

This logic is best implemented in a field validator that runs during configuration loading, providing immediate feedback about directory issues.

### Code Examples

```python
from pydantic import BaseModel, Field, field_validator, ValidationInfo
from pathlib import Path
from loguru import logger

class ProcessingConfig(BaseModel):
    """Processing configuration with directory validation."""

    data_dir: str = Field(
        description="Directory for processed data output (auto-created if missing)"
    )

    mseedindex_executable: str = Field(
        description="Path to mseedindex executable"
    )

    @field_validator('data_dir', mode='after')
    @classmethod
    def validate_data_dir(cls, v: str, info: ValidationInfo) -> str:
        """Validate data directory with auto-creation rules.

        Rules:
        1. If path exists as file -> FAIL with clear error
        2. If path exists as non-empty directory -> FAIL (prevents conflicts)
        3. If path exists as empty directory -> ACCEPT silently
        4. If path doesn't exist -> CREATE (warn if parents needed)

        Args:
            v: The data directory path from config
            info: Validation context

        Returns:
            Validated directory path

        Raises:
            ValueError: If path is file or non-empty directory
        """
        path = Path(v).resolve()

        # Rule 1: Path exists as file - FAIL
        if path.exists() and path.is_file():
            raise ValueError(
                f"NOIZ_PROCESSED_DATA_DIR points to a file, not a directory: {v}\n"
                f"Please specify a directory path instead."
            )

        # Rule 2: Path exists as non-empty directory - FAIL
        if path.exists() and path.is_dir():
            try:
                # Check if directory is empty
                if any(path.iterdir()):
                    raise ValueError(
                        f"NOIZ_PROCESSED_DATA_DIR points to non-empty directory: {v}\n"
                        f"This could cause naming conflicts when processing data.\n"
                        f"Please use an empty directory or specify a new path."
                    )
            except PermissionError:
                raise ValueError(
                    f"NOIZ_PROCESSED_DATA_DIR exists but cannot be read: {v}\n"
                    f"Check directory permissions."
                )

        # Rule 3: Path exists as empty directory - ACCEPT
        if path.exists() and path.is_dir():
            logger.debug(f"Using existing empty data directory: {v}")
            return str(path)

        # Rule 4: Path doesn't exist - CREATE
        try:
            # Check if parent directories need to be created
            parent_exists = path.parent.exists()

            # Create directory (and parents if needed)
            path.mkdir(parents=True, exist_ok=True)

            # Warn if we created parent directories
            if not parent_exists:
                logger.warning(
                    f"Created data directory with parent directories: {v}\n"
                    f"This may indicate a typo in the configuration. "
                    f"Please verify the path is correct."
                )
            else:
                logger.info(f"Created data directory: {v}")

            return str(path)

        except OSError as e:
            raise ValueError(
                f"Failed to create NOIZ_PROCESSED_DATA_DIR: {v}\n"
                f"Error: {e}\n"
                f"Check parent directory exists and you have write permissions."
            )

    @field_validator('mseedindex_executable', mode='after')
    @classmethod
    def validate_mseedindex_executable(cls, v: str) -> str:
        """Validate mseedindex executable exists and is executable.

        Checks both absolute paths and PATH.
        """
        from shutil import which

        # Try as absolute/relative path
        path = Path(v)
        if path.is_file():
            import os
            if os.access(path, os.X_OK):
                return str(path.resolve())
            else:
                raise ValueError(
                    f"NOIZ_MSEEDINDEX_EXECUTABLE is not executable: {v}\n"
                    f"Check file permissions: chmod +x {v}"
                )

        # Try finding in PATH
        executable_path = which(v)
        if executable_path:
            logger.debug(f"Found mseedindex in PATH: {executable_path}")
            return executable_path

        raise ValueError(
            f"NOIZ_MSEEDINDEX_EXECUTABLE not found: {v}\n"
            f"Options:\n"
            f"  1. Provide absolute path to executable\n"
            f"  2. Ensure '{v}' is in your PATH\n"
            f"  3. Install mseedindex and update configuration"
        )


# Test examples
def test_data_dir_validation():
    """Test data directory validation rules."""
    import tempfile
    from pathlib import Path

    # Test 1: Auto-create missing directory
    with tempfile.TemporaryDirectory() as tmpdir:
        new_dir = Path(tmpdir) / "new_data_dir"
        config = ProcessingConfig(
            data_dir=str(new_dir),
            mseedindex_executable="mseedindex"
        )
        assert Path(config.data_dir).exists()

    # Test 2: Accept empty existing directory
    with tempfile.TemporaryDirectory() as tmpdir:
        empty_dir = Path(tmpdir)
        config = ProcessingConfig(
            data_dir=str(empty_dir),
            mseedindex_executable="mseedindex"
        )
        assert config.data_dir == str(empty_dir)

    # Test 3: Reject non-empty directory
    with tempfile.TemporaryDirectory() as tmpdir:
        non_empty_dir = Path(tmpdir)
        (non_empty_dir / "somefile.txt").write_text("data")

        with pytest.raises(ValueError, match="non-empty directory"):
            ProcessingConfig(
                data_dir=str(non_empty_dir),
                mseedindex_executable="mseedindex"
            )

    # Test 4: Reject file path
    with tempfile.NamedTemporaryFile() as tmpfile:
        with pytest.raises(ValueError, match="points to a file"):
            ProcessingConfig(
                data_dir=tmpfile.name,
                mseedindex_executable="mseedindex"
            )

    # Test 5: Warn when creating parent directories
    with tempfile.TemporaryDirectory() as tmpdir:
        nested_dir = Path(tmpdir) / "level1" / "level2" / "data"
        # Should create and warn
        config = ProcessingConfig(
            data_dir=str(nested_dir),
            mseedindex_executable="mseedindex"
        )
        assert Path(config.data_dir).exists()
```

## 10. Backward Compatibility Patterns

### Decision

Maintain backward compatibility in `src/noiz/settings.py` by importing from the new `config.py` module and exposing individual config values as module-level variables, allowing existing `from noiz.settings import VARIABLE` imports to continue working.

### Rationale

The existing codebase uses `from noiz.settings import PROCESSED_DATA_DIR` and similar imports throughout. To avoid a massive refactoring, we:
- Create new `config.py` with Pydantic-based configuration
- Modify `settings.py` to import from `config.py` and expose individual values
- Maintain same variable names as public API
- Gradually migrate code to use config object directly

This approach:
- Minimizes breaking changes during migration
- Allows incremental refactoring
- Preserves existing code patterns
- Makes migration reversible if issues arise

### Code Examples

```python
# src/noiz/config.py (new module)
"""Pydantic-based configuration system for Noiz.

This module provides type-safe configuration management using pydantic-settings v2.
All configuration is loaded from environment variables with NOIZ_ prefix.

Example:
    >>> from noiz.config import get_config
    >>> config = get_config()
    >>> print(config.processing.data_dir)
    /data/processed
"""

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Self
from loguru import logger
from pathlib import Path
from functools import lru_cache

# ... DatabaseConfig, ProcessingConfig, etc. (as defined above) ...

class NoizConfig(BaseSettings):
    """Central configuration for Noiz application."""
    model_config = SettingsConfigDict(
        env_prefix='NOIZ_',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
        validate_default=True,
        extra='ignore',
    )

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    processing: ProcessingConfig
    flask_env: str = Field(default='development')
    loglevel: str = Field(default='INFO')

    def to_flask_config(self) -> dict:
        """Convert to Flask config dictionary."""
        debug = (self.flask_env == 'development')
        return {
            'SQLALCHEMY_DATABASE_URI': self.database.sqlalchemy_database_uri,
            'SQLALCHEMY_TRACK_MODIFICATIONS': False,
            'DEBUG': debug,
            'DEBUG_TB_ENABLED': debug,
            'DEBUG_TB_INTERCEPT_REDIRECTS': False,
            'CACHE_TYPE': 'simple',
            'NOIZ_PROCESSED_DATA_DIR': self.processing.data_dir,
            'NOIZ_MSEEDINDEX_EXECUTABLE': self.processing.mseedindex_executable,
            'NOIZ_LOGLEVEL': self.loglevel,
        }


@lru_cache(maxsize=1)
def get_config() -> NoizConfig:
    """Get cached config instance.

    Uses LRU cache to return same instance on repeated calls within
    same process. For Flask reload pattern, call create_app() which
    creates new config instance.

    Returns:
        Validated NoizConfig instance
    """
    return NoizConfig()


# src/noiz/settings.py (modified for backward compatibility)
"""Configuration settings for Noiz.

This module maintains backward compatibility with existing code that
imports configuration values directly. New code should import from
noiz.config instead.

Backward compatible usage:
    >>> from noiz.settings import PROCESSED_DATA_DIR
    >>> print(PROCESSED_DATA_DIR)
    /data/processed

Recommended usage:
    >>> from noiz.config import get_config
    >>> config = get_config()
    >>> print(config.processing.data_dir)
    /data/processed
"""

import os
from loguru import logger

# Import new config system
try:
    from noiz.config import get_config
    _config = get_config()

    # Export individual values for backward compatibility
    # Database configuration
    DATABASE_BACKEND = _config.database.backend
    POSTGRES_HOST = _config.database.host
    POSTGRES_PORT = _config.database.port
    POSTGRES_USER = _config.database.user
    # Note: Password not exported (use _config.database.password.get_secret_value())
    POSTGRES_DB = _config.database.db
    SQLALCHEMY_DATABASE_URI = _config.database.sqlalchemy_database_uri

    # Processing configuration
    PROCESSED_DATA_DIR = _config.processing.data_dir
    MSEEDINDEX_EXECUTABLE = _config.processing.mseedindex_executable

    # Flask configuration
    FLASK_ENV = _config.flask_env
    DEBUG = (_config.flask_env == 'development')

    # Logging
    LOGLEVEL = _config.loglevel

    # Flask-specific settings (not from config, kept for compatibility)
    DEBUG_TB_ENABLED = DEBUG
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    CACHE_TYPE = "simple"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    logger.info("Configuration loaded via pydantic-settings")

except Exception as e:
    # Fallback to old implementation if new config fails
    # This provides migration safety
    logger.warning(f"Failed to load pydantic config, using fallback: {e}")

    # Old implementation (from original settings.py)
    from environs import Env
    env = Env()
    env.read_env()

    FLASK_ENV = os.environ.get("FLASK_ENV", "development")
    DATABASE_BACKEND = os.environ.get("DATABASE_BACKEND", "postgresql")
    DEBUG = (FLASK_ENV == "development")

    # ... rest of old implementation ...


# Migration helper for code that needs config object
def get_settings_config():
    """Get config object (for migration to new pattern).

    This function helps migrate from:
        from noiz.settings import PROCESSED_DATA_DIR
    To:
        from noiz.settings import get_settings_config
        config = get_settings_config()
        data_dir = config.processing.data_dir
    """
    return _config


# src/noiz/app.py (modified to use new config)
"""Flask application factory."""

from flask import Flask
from noiz.config import NoizConfig
from loguru import logger

def create_app() -> Flask:
    """Create and configure Flask application.

    Creates new config instance on each call to support notebook
    reload pattern where environment variables may change between calls.
    """
    # Load configuration (reloads from environment each time)
    try:
        config = NoizConfig()
        logger.info("Configuration loaded and validated")
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise

    # Create Flask app
    app = Flask(__name__)

    # Load config into Flask
    flask_config = config.to_flask_config()
    app.config.from_mapping(flask_config)

    # Store config object for direct access
    app.config['NOIZ_CONFIG'] = config

    # Log config (with secrets masked)
    logger.info(f"Flask environment: {config.flask_env}")
    logger.info(f"Debug mode: {app.config['DEBUG']}")
    logger.info(f"Database backend: {config.database.backend}")
    logger.info(f"Processed data dir: {config.processing.data_dir}")
    logger.info(f"Log level: {config.loglevel}")

    # ... rest of app initialization ...

    return app


# Migration examples

# Example 1: Existing code (no changes needed)
from noiz.settings import PROCESSED_DATA_DIR, MSEEDINDEX_EXECUTABLE
def old_code():
    print(f"Data dir: {PROCESSED_DATA_DIR}")

# Example 2: Gradually migrate to config object
from noiz.settings import get_settings_config
def migrating_code():
    config = get_settings_config()
    print(f"Data dir: {config.processing.data_dir}")

# Example 3: New code using config directly
from noiz.config import get_config
def new_code():
    config = get_config()
    print(f"Data dir: {config.processing.data_dir}")
```

## Conclusion

This research identifies production-ready patterns for implementing pydantic-settings v2.x configuration in Noiz. The selected approaches provide:

**Key Benefits**:
- Type safety with IDE autocomplete and mypy support
- Comprehensive validation with clear error messages
- Secret masking to prevent credential leaks
- Clean Flask integration via model_dump()
- Support for dynamic reload (notebook pattern)
- Isolated testing with fixture-based overrides
- Backward compatibility during migration

**Implementation Priorities**:
1. Create `config.py` with nested BaseModel sub-models
2. Implement validation logic for directories, executables, and database params
3. Add `SecretStr` for passwords and URIs
4. Create `to_flask_config()` method for Flask integration
5. Update `settings.py` for backward compatibility
6. Update `app.py` to use new config in create_app()
7. Add pytest fixtures for test isolation
8. Write comprehensive tests for validation logic
9. Update CI configuration to use NOIZ_* variables
10. Create user documentation with .env file examples

**Critical Success Factors**:
- All validators must provide actionable error messages
- Secrets must be masked in all output (logs, repr, errors)
- Flask reload pattern must continue working for notebooks
- Test isolation must be reliable (no environment leakage)
- Backward compatibility must allow gradual migration

This research provides the foundation for implementing a robust, maintainable configuration system that meets all requirements specified in the feature specification.

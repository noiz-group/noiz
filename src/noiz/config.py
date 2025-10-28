# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

"""Pydantic-based configuration system for Noiz.

This module provides type-safe configuration management using pydantic-settings v2.
All configuration is loaded from environment variables with NOIZ_ prefix.

Example:
    >>> from noiz.config import get_config
    >>> config = get_config()
    >>> print(config.processing.processed_data_dir)
    /data/processed
"""

from functools import cached_property, lru_cache
from pathlib import Path
from shutil import which
from typing import Any, Literal, Optional, cast

from loguru import logger
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Self


class NoizBaseSettings(BaseSettings):
    """Base settings class with shared NOIZ_ prefix configuration.

    All Noiz configuration classes inherit from this to ensure uniform
    environment variable handling with NOIZ_ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="NOIZ_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        validate_default=True,
        extra="ignore",
    )


class DatabaseConfig(NoizBaseSettings):
    """Database connection configuration with validation.

    Handles both PostgreSQL and SQLite backends with URI-first precedence.
    All validation happens here independently.
    """

    database_backend: Literal["postgresql", "sqlite"] = Field(
        default="sqlite",
        description="Database backend: 'postgresql' or 'sqlite'",
    )

    # Connection URI (takes precedence if set)
    database_url: Optional[SecretStr] = Field(
        default=None,
        description="Database connection URI (takes precedence over individual params)",
    )

    # PostgreSQL individual connection parameters
    postgres_host: Optional[str] = Field(default=None, description="PostgreSQL host")
    postgres_port: Optional[int] = Field(default=5432, description="PostgreSQL port")
    postgres_user: Optional[str] = Field(default=None, description="PostgreSQL user")
    postgres_password: Optional[SecretStr] = Field(
        default=None,
        description="PostgreSQL password (masked in logs)",
    )
    postgres_db: Optional[str] = Field(default=None, description="PostgreSQL database name")

    @field_validator("database_backend", mode="before")
    @classmethod
    def validate_backend(cls, v: str) -> str:
        """Validate database backend is supported.

        Normalizes to lowercase before Literal type check.

        :param v: Backend value
        :type v: str
        :return: Validated backend (lowercase)
        :rtype: str
        :raises ValueError: If backend is not supported
        """
        if isinstance(v, str):
            v_lower = v.lower()
            valid_backends = {"postgresql", "sqlite"}
            if v_lower not in valid_backends:
                raise ValueError(f"Invalid database backend: {v}. Must be one of {valid_backends}")
            return v_lower
        return v

    @field_validator("postgres_port", mode="after")
    @classmethod
    def validate_port(cls, v: Optional[int]) -> Optional[int]:
        """Validate port is in valid range.

        :param v: Port number
        :type v: Optional[int]
        :return: Validated port
        :rtype: Optional[int]
        :raises ValueError: If port is out of valid range
        """
        if v is not None and not (1 <= v <= 65535):
            raise ValueError(f"Invalid NOIZ_POSTGRES_PORT: {v}. Must be between 1 and 65535")
        return v

    @model_validator(mode="after")
    def validate_database_connection(self) -> Self:
        """Validate connection parameters based on backend and precedence rules.

        Rules:
        - URI (database_url) takes precedence if set
        - PostgreSQL requires either URI or all individual params
        - SQLite can use default URI if none provided

        :return: Validated configuration
        :rtype: Self
        :raises ValueError: If PostgreSQL backend lacks required parameters
        """
        # SQLite backend - always valid (has default)
        if self.database_backend == "sqlite":
            if self.database_url:
                logger.info("Using NOIZ_DATABASE_URL for SQLite connection")
            else:
                logger.info("Using default SQLite connection (sqlite:///noiz.db)")
            return self

        # PostgreSQL backend
        if self.database_backend == "postgresql":
            # URI provided - valid (URI-first precedence)
            if self.database_url:
                # Warn if both URI and individual params provided
                has_params = any(
                    [
                        self.postgres_host,
                        self.postgres_user,
                        self.postgres_password,
                        self.postgres_db,
                    ]
                )
                if has_params:
                    logger.warning(
                        "Both NOIZ_DATABASE_URL and NOIZ_POSTGRES_* variables provided. "
                        "Using DATABASE_URL (URI-first precedence rule)."
                    )
                else:
                    logger.info("Using NOIZ_DATABASE_URL for PostgreSQL connection")
                return self

            # Check individual params
            pwd = self.postgres_password.get_secret_value() if self.postgres_password else ""
            required_params = {
                "NOIZ_POSTGRES_HOST": self.postgres_host,
                "NOIZ_POSTGRES_PORT": self.postgres_port,
                "NOIZ_POSTGRES_USER": self.postgres_user,
                "NOIZ_POSTGRES_PASSWORD": pwd,
                "NOIZ_POSTGRES_DB": self.postgres_db,
            }

            missing = [name for name, value in required_params.items() if not value]

            if not missing:
                logger.info("Using NOIZ_POSTGRES_* variables for connection")
                return self

            # Neither URI nor complete parameters provided
            raise ValueError(
                f"PostgreSQL backend requires either:\n"
                f"  1. NOIZ_DATABASE_URL (URI-first precedence), OR\n"
                f"  2. All of: NOIZ_POSTGRES_HOST, NOIZ_POSTGRES_PORT, "
                f"NOIZ_POSTGRES_USER, NOIZ_POSTGRES_PASSWORD, NOIZ_POSTGRES_DB\n"
                f"Missing: {', '.join(missing)}"
            )

        return self

    @property
    def sqlalchemy_database_uri(self) -> str:
        """Compute SQLAlchemy database URI with precedence rules.

        Precedence:
        1. database_url (NOIZ_DATABASE_URL) if set
        2. Individual parameters if all present
        3. SQLite default if backend is 'sqlite'

        :return: Database connection URI string
        :rtype: str
        :raises ValueError: If configuration is invalid
        """
        # URI-first precedence
        if self.database_url:
            return self.database_url.get_secret_value()

        # SQLite backend
        if self.database_backend == "sqlite":
            return "sqlite:///noiz.db"

        # PostgreSQL from individual parameters
        if all([self.postgres_host, self.postgres_port, self.postgres_user, self.postgres_password, self.postgres_db]):
            pwd = self.postgres_password.get_secret_value() if self.postgres_password else ""
            return (
                f"postgresql+psycopg2://{self.postgres_user}:{pwd}@"
                f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )

        # Should never reach here due to model_validator, but explicit for safety
        raise ValueError(
            "Database connection not properly configured. This should have been caught during validation."
        )

    @property
    def backend(self) -> str:
        """Convenience property for database_backend.

        :return: Database backend type
        :rtype: str
        """
        return self.database_backend


class ProcessingConfig(NoizBaseSettings):
    """Processing pipeline configuration with validation.

    Handles data directory and executable validation with auto-creation rules.
    All validation happens here independently.
    """

    processed_data_dir: Path = Field(description="Directory for processed data output (auto-created if missing)")

    mseedindex_executable: Optional[str] = Field(
        default=None,
        description="Path to mseedindex executable (auto-discovered if not provided)",
    )

    @field_validator("processed_data_dir", mode="after")
    @classmethod
    def validate_data_dir(cls, v: Path) -> Path:
        """Validate data directory with auto-creation rules.

        Rules:
        1. If path exists as file -> FAIL with clear error
        2. If path exists as non-empty directory -> WARN (may cause conflicts)
        3. If path exists as empty directory -> ACCEPT silently
        4. If path doesn't exist -> CREATE (warn if parents needed)

        :param v: Path to validate
        :type v: Path
        :return: Validated path
        :rtype: Path
        :raises ValueError: If validation fails
        """
        path = v.resolve() if isinstance(v, Path) else Path(v).resolve()

        # Rule 1: Path exists as file - FAIL
        if path.exists() and path.is_file():
            raise ValueError(
                f"NOIZ_PROCESSED_DATA_DIR points to a file, not a directory: {path}\n"
                f"Please specify a directory path instead."
            )

        # Rule 2: Path exists as non-empty directory - WARN (don't fail)
        # Dask workers share parent's data directory so we cannot require empty directories
        if path.exists() and path.is_dir():
            try:
                # Check if directory is empty and warn if not
                if any(path.iterdir()):
                    logger.warning(
                        f"NOIZ_PROCESSED_DATA_DIR points to non-empty directory: {path}\n"
                        f"This could cause naming conflicts when processing data. "
                        f"Consider using an empty directory if you encounter issues."
                    )
            except PermissionError as e:
                raise ValueError(
                    f"NOIZ_PROCESSED_DATA_DIR exists but cannot be read: {path}\nCheck directory permissions."
                ) from e

        # Rule 3: Path exists as empty directory - ACCEPT
        if path.exists() and path.is_dir():
            logger.debug(f"Using existing empty data directory: {path}")
            return path

        # Rule 4: Path doesn't exist - CREATE
        try:
            # Check if parent directories need to be created
            parent_exists = path.parent.exists()

            # Create directory (and parents if needed)
            path.mkdir(parents=True, exist_ok=True)

            # Warn if we created parent directories
            if not parent_exists:
                logger.warning(
                    f"Created data directory with parent directories: {path}\n"
                    f"This may indicate a typo in the configuration. "
                    f"Please verify the path is correct."
                )
            else:
                logger.info(f"Created data directory: {path}")

            return path

        except OSError as e:
            raise ValueError(
                f"Failed to create NOIZ_PROCESSED_DATA_DIR: {path}\n"
                f"Error: {e}\n"
                f"Check parent directory exists and you have write permissions."
            ) from e

    @field_validator("mseedindex_executable", mode="after")
    @classmethod
    def validate_mseedindex_executable(cls, v: Optional[str]) -> str:
        """Validate mseedindex executable exists and is executable.

        Auto-discovers mseedindex if not explicitly provided.
        Checks both absolute paths and PATH.

        :param v: Explicit path to mseedindex or None for auto-discovery
        :type v: Optional[str]
        :return: Validated path to mseedindex executable
        :rtype: str
        :raises ValueError: If mseedindex cannot be found or is not executable
        """
        # Auto-discover if not provided
        if v is None:
            executable_path = which("mseedindex")
            if executable_path:
                logger.info(f"Auto-discovered mseedindex in PATH: {executable_path}")
                return executable_path
            else:
                raise ValueError(
                    "NOIZ_MSEEDINDEX_EXECUTABLE not set and auto-discovery failed.\n"
                    "mseedindex is not in PATH. Options:\n"
                    "  1. Install mseedindex: pip install mseedindex\n"
                    "  2. Set NOIZ_MSEEDINDEX_EXECUTABLE to the executable path"
                )

        # Explicit path provided - validate it
        # Try as absolute/relative path
        path = Path(v)
        if path.is_file():
            import os

            if os.access(path, os.X_OK):
                logger.debug(f"Found mseedindex executable: {path.resolve()}")
                return str(path.resolve())
            else:
                raise ValueError(
                    f"NOIZ_MSEEDINDEX_EXECUTABLE is not executable: {v}\nCheck file permissions: chmod +x {v}"
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
            f"  3. Install mseedindex: pip install mseedindex"
        )


class NoizConfig(NoizBaseSettings):
    """Root configuration composing database and processing configs.

    Provides Flask-specific settings and composes DatabaseConfig and
    ProcessingConfig as cached properties.
    """

    # Flask-specific settings
    flask_env: Literal["development", "production"] = Field(
        default="development",
        description="Flask environment: 'development' or 'production'",
    )

    loglevel: str = Field(
        default="INFO",
        description="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )

    def model_post_init(self, __context: Any) -> None:
        """Post-initialization hook to eagerly validate composed configs.

        Accesses database and processing properties to trigger their validation
        immediately rather than lazily.

        :param __context: Pydantic context (unused)
        :type __context: Any
        :return: None
        :rtype: NoneType
        """
        # Trigger validation by accessing the cached properties
        _ = self.database
        _ = self.processing

    @field_validator("loglevel", mode="after")
    @classmethod
    def validate_loglevel(cls, v: str) -> str:
        """Validate log level is recognized by loguru.

        Defaults to INFO if unrecognized value is provided.

        :param v: Log level string
        :type v: str
        :return: Validated log level (uppercase, defaults to INFO if invalid)
        :rtype: str
        """
        v_upper = v.upper()
        valid_levels = {"DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"}
        if v_upper not in valid_levels:
            logger.warning(f"Invalid NOIZ_LOGLEVEL: {v}. Defaulting to INFO. Valid levels: {valid_levels}")
            return "INFO"
        return v_upper

    @field_validator("flask_env", mode="before")
    @classmethod
    def validate_flask_env(cls, v: str) -> str:
        """Validate Flask environment is valid.

        Normalizes to lowercase before Literal type check.

        :param v: Flask environment string
        :type v: str
        :return: Validated environment (lowercase)
        :rtype: str
        :raises ValueError: If environment is invalid
        """
        if isinstance(v, str):
            v_lower = v.lower()
            valid_envs = {"development", "production"}
            if v_lower not in valid_envs:
                raise ValueError(f"Invalid NOIZ_FLASK_ENV: {v}. Must be one of {valid_envs}")
            return v_lower
        return v

    @cached_property
    def database(self) -> DatabaseConfig:
        """Get database configuration.

        Lazily creates and caches DatabaseConfig instance which reads
        its own environment variables and performs validation.

        :return: Database configuration
        :rtype: DatabaseConfig
        """
        return DatabaseConfig()

    @cached_property
    def processing(self) -> ProcessingConfig:
        """Get processing configuration.

        Lazily creates and caches ProcessingConfig instance which reads
        its own environment variables and performs validation.

        :return: Processing configuration
        :rtype: ProcessingConfig
        """
        return ProcessingConfig()

    @property
    def sqlalchemy_database_uri(self) -> str:
        """Convenience property for database URI.

        :return: SQLAlchemy database URI
        :rtype: str
        """
        return self.database.sqlalchemy_database_uri

    @property
    def processed_data_dir(self) -> Path:
        """Convenience property for processed data directory.

        :return: Processed data directory path
        :rtype: Path
        """
        return self.processing.processed_data_dir

    @property
    def mseedindex_executable(self) -> str:
        """Convenience property for mseedindex executable.

        Validator guarantees this is always a string after initialization.

        :return: Path to mseedindex executable
        :rtype: str
        """
        # Validator guarantees this is str after initialization
        return cast(str, self.processing.mseedindex_executable)

    @property
    def database_backend(self) -> str:
        """Convenience property for database backend.

        :return: Database backend name
        :rtype: str
        """
        return self.database.database_backend

    # Backward compatibility properties for individual postgres params
    @property
    def postgres_host(self) -> Optional[str]:
        """Backward compatibility property."""
        return self.database.postgres_host

    @property
    def postgres_port(self) -> Optional[int]:
        """Backward compatibility property."""
        return self.database.postgres_port

    @property
    def postgres_user(self) -> Optional[str]:
        """Backward compatibility property."""
        return self.database.postgres_user

    @property
    def postgres_password(self) -> Optional[SecretStr]:
        """Backward compatibility property."""
        return self.database.postgres_password

    @property
    def postgres_db(self) -> Optional[str]:
        """Backward compatibility property."""
        return self.database.postgres_db

    def to_flask_config(self) -> dict[str, Any]:
        """Convert settings to Flask config dictionary.

        Generates Flask-specific config values derived from settings.
        Masks sensitive values (passwords shown as ******).

        :return: Flask configuration dictionary
        :rtype: dict[str, Any]
        """
        debug = self.flask_env == "development"

        flask_config = {
            # Database
            "SQLALCHEMY_DATABASE_URI": self.sqlalchemy_database_uri,
            "SQLALCHEMY_TRACK_MODIFICATIONS": False,
            # Flask settings
            "DEBUG": debug,
            "TESTING": False,
            "ENV": self.flask_env,
            # Debug toolbar
            "DEBUG_TB_ENABLED": debug,
            "DEBUG_TB_INTERCEPT_REDIRECTS": False,
            # Cache
            "CACHE_TYPE": "simple",
            # Custom Noiz settings (with NOIZ_ prefix)
            "NOIZ_PROCESSED_DATA_DIR": str(self.processed_data_dir),
            "NOIZ_MSEEDINDEX_EXECUTABLE": self.mseedindex_executable,
            "NOIZ_LOGLEVEL": self.loglevel,
            "NOIZ_DATABASE_BACKEND": self.database_backend,
            # Backward compatibility (without NOIZ_ prefix for legacy code)
            "PROCESSED_DATA_DIR": str(self.processed_data_dir),
            "MSEEDINDEX_EXECUTABLE": self.mseedindex_executable,
            "DATABASE_BACKEND": self.database_backend,
            "POSTGRES_HOST": self.postgres_host or "",
            "POSTGRES_PORT": str(self.postgres_port) if self.postgres_port else "",
            "POSTGRES_USER": self.postgres_user or "",
            "POSTGRES_PASSWORD": self.postgres_password.get_secret_value() if self.postgres_password else "",
            "POSTGRES_DB": self.postgres_db or "",
        }

        return flask_config

    def __repr__(self) -> str:
        """String representation with sensitive values masked.

        :return: String representation
        :rtype: str
        """
        return (
            f"NoizConfig(\n"
            f"  database_backend={self.database_backend!r},\n"
            f"  database_url={'******' if self.database.database_url else None},\n"
            f"  processed_data_dir={self.processed_data_dir!r},\n"
            f"  flask_env={self.flask_env!r},\n"
            f"  loglevel={self.loglevel!r}\n"
            f")"
        )


@lru_cache(maxsize=1)
def get_config() -> NoizConfig:
    """Get cached config instance.

    Uses LRU cache to return same instance on repeated calls within
    same process. For Flask reload pattern, clear cache manually with
    get_config.cache_clear().

    :return: Validated NoizConfig instance
    :rtype: NoizConfig
    :raises ValidationError: If configuration is invalid
    :raises ValueError: If directory/executable validation fails
    """
    try:
        config = NoizConfig()
        logger.info(
            f"Configuration loaded successfully (backend: {config.database_backend}, flask_env: {config.flask_env})"
        )
        return config
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise

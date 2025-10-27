# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

"""Unit tests for configuration module."""

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from noiz.config import DatabaseConfig, NoizConfig, ProcessingConfig, get_config


class TestDatabaseConfig:
    """Test DatabaseConfig validation and behavior."""

    def test_valid_postgresql_with_database_url(self, tmp_path, monkeypatch):
        """Valid PostgreSQL config with DATABASE_URL should succeed."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "postgresql")
        monkeypatch.setenv("NOIZ_DATABASE_URL", "postgresql://user:pass@localhost:5432/testdb")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        assert config.database.backend == "postgresql"
        assert config.database.database_url is not None
        assert "postgresql://user:pass@localhost:5432/testdb" in config.database.sqlalchemy_database_uri

    def test_valid_postgresql_with_individual_params(self, tmp_path, monkeypatch):
        """Valid PostgreSQL config with POSTGRES_* vars should succeed."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "postgresql")
        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "localhost")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "5432")
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "testuser")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "testdb")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        assert config.database.backend == "postgresql"
        assert config.database.postgres_host == "localhost"
        assert config.database.postgres_port == 5432
        assert "testuser:testpass@localhost:5432/testdb" in config.database.sqlalchemy_database_uri

    def test_valid_sqlite_config(self, tmp_path, monkeypatch):
        """Valid SQLite config should succeed."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        assert config.database.backend == "sqlite"
        assert config.database.sqlalchemy_database_uri == "sqlite:///noiz.db"

    def test_sqlite_with_custom_url(self, tmp_path, monkeypatch):
        """SQLite with custom DATABASE_URL should use it."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_DATABASE_URL", "sqlite:///custom.db")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        assert config.database.backend == "sqlite"
        assert config.database.sqlalchemy_database_uri == "sqlite:///custom.db"

    def test_uri_precedence(self, tmp_path, monkeypatch):
        """DATABASE_URL takes precedence over POSTGRES_* variables."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        # Set both DATABASE_URL and individual params
        monkeypatch.setenv("NOIZ_DATABASE_URL", "postgresql://uri_user:uri_pass@uri_host:5433/uri_db")
        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "param_host")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "5432")
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "param_user")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "param_pass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "param_db")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        # URI should take precedence
        assert "uri_host" in config.database.sqlalchemy_database_uri
        assert "param_host" not in config.database.sqlalchemy_database_uri

    def test_postgresql_missing_params(self, tmp_path, monkeypatch):
        """PostgreSQL with missing required params should fail."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "postgresql")
        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "localhost")
        # Missing: PORT, USER, PASSWORD, DB
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        error_str = str(exc_info.value)
        assert "PostgreSQL backend requires" in error_str
        assert "NOIZ_POSTGRES_PORT" in error_str or "Missing" in error_str

    def test_invalid_backend(self, tmp_path, monkeypatch):
        """Invalid database backend should fail."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "mysql")
        monkeypatch.setenv("NOIZ_DATABASE_URL", "mysql://localhost/test")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        # Pydantic's Literal validation happens first
        error_str = str(exc_info.value)
        assert "database_backend" in error_str and ("postgresql" in error_str or "sqlite" in error_str)

    def test_invalid_port(self, tmp_path, monkeypatch):
        """Invalid port number should fail."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "localhost")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "99999")  # Invalid port
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "user")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "pass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "db")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        assert "Invalid NOIZ_POSTGRES_PORT" in str(exc_info.value) or "must be between" in str(exc_info.value)

    def test_secret_masking_in_repr(self, tmp_path, monkeypatch):
        """Passwords should be masked in repr."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_URL", "postgresql://user:secret_password@localhost:5432/db")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        repr_str = repr(config)
        assert "******" in repr_str or "SecretStr" in repr_str
        assert "secret_password" not in repr_str


class TestProcessingConfig:
    """Test ProcessingConfig validation and directory handling."""

    def test_auto_create_missing_directory(self, tmp_path, monkeypatch):
        """Directory should be auto-created if missing."""
        new_dir = tmp_path / "new_data_dir"
        assert not new_dir.exists()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(new_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        assert Path(config.processing.processed_data_dir).exists()
        assert Path(config.processing.processed_data_dir).is_dir()

    def test_accept_empty_existing_directory(self, tmp_path, monkeypatch):
        """Empty existing directory should be accepted silently."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(empty_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        assert str(config.processing.processed_data_dir) == str(empty_dir.resolve())

    def test_reject_non_empty_directory(self, tmp_path, monkeypatch):
        """Non-empty directory should fail validation."""
        non_empty_dir = tmp_path / "non_empty"
        non_empty_dir.mkdir()
        (non_empty_dir / "somefile.txt").write_text("data")

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(non_empty_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        error_str = str(exc_info.value)
        assert "non-empty directory" in error_str or "naming conflicts" in error_str

    def test_reject_file_path(self, tmp_path, monkeypatch):
        """File path should fail validation."""
        file_path = tmp_path / "somefile.txt"
        file_path.write_text("test")

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(file_path))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        error_str = str(exc_info.value)
        assert "points to a file" in error_str or "not a directory" in error_str

    def test_warn_parent_directory_creation(self, tmp_path, monkeypatch):
        """Creating parent directories should emit warning (check via side effect)."""
        nested_dir = tmp_path / "level1" / "level2" / "data"

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(nested_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        # Verify directory and parents were created
        assert Path(config.processing.processed_data_dir).exists()
        assert Path(config.processing.processed_data_dir).parent.parent.exists()

    def test_mseedindex_executable_in_path(self, tmp_path, monkeypatch):
        """Executable in PATH should be accepted."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "ls")  # ls should be in PATH

        config = NoizConfig()

        # Should find ls in PATH
        assert config.processing.mseedindex_executable

    def test_mseedindex_executable_not_found(self, tmp_path, monkeypatch):
        """Non-existent executable should fail."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "nonexistent_command_xyz")

        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        error_str = str(exc_info.value)
        assert "not found" in error_str or "NOIZ_MSEEDINDEX_EXECUTABLE" in error_str


class TestNoizConfig:
    """Test NoizConfig root configuration."""

    def test_default_values(self, tmp_path, monkeypatch):
        """Default values should be applied correctly."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        assert config.flask_env == "development"
        assert config.loglevel == "INFO"
        assert config.database.backend == "sqlite"

    def test_flask_env_validation(self, tmp_path, monkeypatch):
        """Invalid Flask environment should fail with Literal validation."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_FLASK_ENV", "invalid_env")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        # Literal validation happens before custom validator, so it should fail
        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        error_str = str(exc_info.value)
        assert "flask_env" in error_str and ("development" in error_str or "production" in error_str)

    def test_loglevel_validation(self, tmp_path, monkeypatch):
        """Invalid log level should default with warning."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_LOGLEVEL", "INVALID_LEVEL")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        # Should default to INFO (validator handles invalid values)
        assert config.loglevel == "INFO"

    def test_to_flask_config(self, tmp_path, monkeypatch):
        """to_flask_config() should generate correct Flask config dict."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_FLASK_ENV", "production")
        monkeypatch.setenv("NOIZ_LOGLEVEL", "ERROR")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        flask_config = config.to_flask_config()

        # Check required keys
        assert "SQLALCHEMY_DATABASE_URI" in flask_config
        assert "DEBUG" in flask_config
        assert "NOIZ_PROCESSED_DATA_DIR" in flask_config
        assert "NOIZ_MSEEDINDEX_EXECUTABLE" in flask_config

        # Check derived values
        assert flask_config["DEBUG"] is False  # production mode
        assert flask_config["ENV"] == "production"
        assert flask_config["SQLALCHEMY_TRACK_MODIFICATIONS"] is False
        assert flask_config["CACHE_TYPE"] == "simple"

    def test_development_mode(self, tmp_path, monkeypatch):
        """Development mode should set DEBUG=True."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_FLASK_ENV", "development")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        flask_config = config.to_flask_config()

        assert flask_config["DEBUG"] is True
        assert flask_config["DEBUG_TB_ENABLED"] is True

    def test_production_mode(self, tmp_path, monkeypatch):
        """Production mode should set DEBUG=False."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_FLASK_ENV", "production")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        flask_config = config.to_flask_config()

        assert flask_config["DEBUG"] is False
        assert flask_config["DEBUG_TB_ENABLED"] is False


class TestGetConfig:
    """Test get_config() singleton function."""

    def test_get_config_returns_instance(self, tmp_path, monkeypatch):
        """get_config() should return valid NoizConfig instance."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        # Clear cache to ensure fresh load
        get_config.cache_clear()

        config = get_config()

        assert isinstance(config, NoizConfig)
        assert config.database.backend == "sqlite"

    def test_get_config_caches_instance(self, tmp_path, monkeypatch):
        """get_config() should return cached instance on subsequent calls."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        # Clear cache
        get_config.cache_clear()

        config1 = get_config()
        config2 = get_config()

        # Should be same instance
        assert config1 is config2


class TestNoizBaseSettings:
    """Test NoizBaseSettings mixin behavior."""

    def test_env_prefix_applied(self, tmp_path, monkeypatch):
        """NoizBaseSettings should apply NOIZ_ prefix to all configs."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        # Set with NOIZ_ prefix
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        # DatabaseConfig should read NOIZ_ prefixed vars
        db_config = DatabaseConfig()
        assert db_config.database_backend == "sqlite"

        # ProcessingConfig should read NOIZ_ prefixed vars
        proc_config = ProcessingConfig()
        assert proc_config.processed_data_dir == processed_dir.resolve()

    def test_case_insensitive(self, tmp_path, monkeypatch):
        """Config should be case insensitive."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        # Use mixed case
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "SQLite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        assert config.database_backend == "sqlite"  # Normalized to lowercase

    def test_dotenv_support(self, tmp_path, monkeypatch):
        """NoizBaseSettings should support .env file loading."""
        # This is implicit via SettingsConfigDict, just verify config includes it
        from noiz.config import NoizBaseSettings

        assert NoizBaseSettings.model_config["env_file"] == ".env"
        assert NoizBaseSettings.model_config["env_prefix"] == "NOIZ_"


class TestDatabaseConfigIndependent:
    """Test DatabaseConfig as independent configuration."""

    def test_instantiate_independently_postgresql(self, monkeypatch):
        """DatabaseConfig can be instantiated independently with PostgreSQL params."""
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "postgresql")
        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "localhost")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "5432")
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "testuser")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "testdb")

        # Create DatabaseConfig without NoizConfig
        db_config = DatabaseConfig()

        assert db_config.backend == "postgresql"
        assert db_config.postgres_host == "localhost"
        assert db_config.postgres_port == 5432
        assert db_config.postgres_user == "testuser"
        assert db_config.postgres_db == "testdb"
        assert "testuser:testpass@localhost:5432/testdb" in db_config.sqlalchemy_database_uri

    def test_instantiate_independently_sqlite(self, monkeypatch):
        """DatabaseConfig can be instantiated independently with SQLite."""
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")

        db_config = DatabaseConfig()

        assert db_config.backend == "sqlite"
        assert db_config.sqlalchemy_database_uri == "sqlite:///noiz.db"

    def test_backend_property_matches_field(self, monkeypatch):
        """backend property should match database_backend field."""
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")

        db_config = DatabaseConfig()

        assert db_config.backend == db_config.database_backend
        assert db_config.backend == "sqlite"

    def test_validation_without_noizconfig(self, monkeypatch):
        """DatabaseConfig validation should work independently."""
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "postgresql")
        # Missing required params

        with pytest.raises(ValidationError) as exc_info:
            DatabaseConfig()

        error_str = str(exc_info.value)
        assert "PostgreSQL backend requires" in error_str


class TestProcessingConfigIndependent:
    """Test ProcessingConfig as independent configuration."""

    def test_instantiate_independently(self, tmp_path, monkeypatch):
        """ProcessingConfig can be instantiated independently."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "ls")  # ls should exist

        proc_config = ProcessingConfig()

        assert proc_config.processed_data_dir == processed_dir.resolve()
        assert proc_config.mseedindex_executable is not None

    def test_auto_discover_mseedindex(self, tmp_path, monkeypatch):
        """ProcessingConfig should auto-discover mseedindex if not set."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        # Don't set NOIZ_MSEEDINDEX_EXECUTABLE

        # Mock which() to return a path
        from unittest.mock import patch

        with patch("noiz.config.which", return_value="/usr/bin/mseedindex"):
            proc_config = ProcessingConfig()
            assert proc_config.mseedindex_executable == "/usr/bin/mseedindex"

    def test_validation_without_noizconfig(self, tmp_path, monkeypatch):
        """ProcessingConfig validation should work independently."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("test")

        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(file_path))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        with pytest.raises(ValidationError) as exc_info:
            ProcessingConfig()

        error_str = str(exc_info.value)
        assert "points to a file" in error_str


class TestNoizConfigComposition:
    """Test NoizConfig composition pattern with cached properties."""

    def test_database_property_returns_database_config(self, tmp_path, monkeypatch):
        """NoizConfig.database should return DatabaseConfig instance."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        # database property should return DatabaseConfig instance
        assert isinstance(config.database, DatabaseConfig)
        assert config.database.backend == "sqlite"

    def test_processing_property_returns_processing_config(self, tmp_path, monkeypatch):
        """NoizConfig.processing should return ProcessingConfig instance."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        # processing property should return ProcessingConfig instance
        assert isinstance(config.processing, ProcessingConfig)
        assert config.processing.processed_data_dir == processed_dir.resolve()

    def test_cached_property_returns_same_instance(self, tmp_path, monkeypatch):
        """Cached properties should return same instance on repeated access."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        # Access properties multiple times
        db1 = config.database
        db2 = config.database
        proc1 = config.processing
        proc2 = config.processing

        # Should be same instances (cached)
        assert db1 is db2
        assert proc1 is proc2

    def test_convenience_properties_delegate_correctly(self, tmp_path, monkeypatch):
        """Convenience properties should delegate to composed configs."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_URL", "postgresql://user:pass@localhost:5432/db")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()

        # Convenience properties should match composed config values
        assert config.sqlalchemy_database_uri == config.database.sqlalchemy_database_uri
        assert config.processed_data_dir == config.processing.processed_data_dir
        assert config.mseedindex_executable == config.processing.mseedindex_executable
        assert config.database_backend == config.database.database_backend

    def test_eager_validation_on_init(self, tmp_path, monkeypatch):
        """NoizConfig should eagerly validate composed configs on init."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "postgresql")
        # Missing postgres params - should fail immediately on NoizConfig()
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        # Should fail during __init__ due to model_post_init
        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        error_str = str(exc_info.value)
        assert "PostgreSQL backend requires" in error_str

    def test_eager_validation_catches_processing_errors(self, tmp_path, monkeypatch):
        """NoizConfig should catch ProcessingConfig validation errors on init."""
        file_path = tmp_path / "file.txt"
        file_path.write_text("test")

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(file_path))  # Invalid - is a file
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        # Should fail during __init__ due to model_post_init
        with pytest.raises(ValidationError) as exc_info:
            NoizConfig()

        error_str = str(exc_info.value)
        assert "points to a file" in error_str


class TestDatabaseConfigValidation:
    """Comprehensive validation tests for DatabaseConfig."""

    def test_database_backend_normalization(self, monkeypatch):
        """Backend should be normalized to lowercase."""
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "POSTGRESQL")
        monkeypatch.setenv("NOIZ_DATABASE_URL", "postgresql://localhost/test")

        db_config = DatabaseConfig()
        assert db_config.database_backend == "postgresql"

    def test_port_validation_lower_bound(self, monkeypatch):
        """Port must be >= 1."""
        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "localhost")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "0")  # Invalid
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "user")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "pass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "db")

        with pytest.raises(ValidationError) as exc_info:
            DatabaseConfig()

        assert "Must be between 1 and 65535" in str(exc_info.value)

    def test_port_validation_upper_bound(self, monkeypatch):
        """Port must be <= 65535."""
        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "localhost")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "65536")  # Invalid
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "user")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "pass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "db")

        with pytest.raises(ValidationError) as exc_info:
            DatabaseConfig()

        assert "Must be between 1 and 65535" in str(exc_info.value)

    def test_uri_precedence_warning(self, monkeypatch):
        """Should use URI when both URI and individual params provided."""
        monkeypatch.setenv("NOIZ_DATABASE_URL", "postgresql://uri_host/db")
        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "param_host")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "5432")
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "user")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "pass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "db")

        db_config = DatabaseConfig()

        # Should use URI (URI-first precedence)
        assert "uri_host" in db_config.sqlalchemy_database_uri
        assert "param_host" not in db_config.sqlalchemy_database_uri

    def test_sqlite_default_uri(self, monkeypatch):
        """SQLite should use default URI if none provided."""
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")

        db_config = DatabaseConfig()
        assert db_config.sqlalchemy_database_uri == "sqlite:///noiz.db"

    def test_postgresql_constructed_uri(self, monkeypatch):
        """PostgreSQL URI should be constructed from individual params."""
        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "postgresql")
        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "myhost")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "5433")
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "myuser")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "mypass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "mydb")

        db_config = DatabaseConfig()

        expected = "postgresql+psycopg2://myuser:mypass@myhost:5433/mydb"
        assert db_config.sqlalchemy_database_uri == expected


class TestProcessingConfigValidation:
    """Comprehensive validation tests for ProcessingConfig."""

    def test_data_dir_auto_create_with_warning(self, tmp_path, monkeypatch):
        """Should create parent directories when needed."""
        nested_dir = tmp_path / "level1" / "level2" / "data"

        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(nested_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        proc_config = ProcessingConfig()

        # Should create directory and all parents
        assert proc_config.processed_data_dir.exists()
        assert proc_config.processed_data_dir.parent.exists()
        assert proc_config.processed_data_dir.parent.parent.exists()

    def test_data_dir_empty_existing_accepted(self, tmp_path, monkeypatch):
        """Empty existing directory should be accepted."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(empty_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        proc_config = ProcessingConfig()

        assert proc_config.processed_data_dir == empty_dir.resolve()

    def test_mseedindex_absolute_path(self, tmp_path, monkeypatch):
        """Absolute path to mseedindex should be accepted."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        # Create a fake executable
        fake_exe = tmp_path / "fake_mseedindex"
        fake_exe.write_text("#!/bin/bash\necho test")
        fake_exe.chmod(0o755)

        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", str(fake_exe))

        proc_config = ProcessingConfig()

        assert proc_config.mseedindex_executable == str(fake_exe.resolve())

    def test_mseedindex_not_executable(self, tmp_path, monkeypatch):
        """Non-executable file should fail validation."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        # Create file without execute permissions
        non_exe = tmp_path / "non_executable"
        non_exe.write_text("test")
        non_exe.chmod(0o644)  # rw-r--r--

        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", str(non_exe))

        with pytest.raises(ValidationError) as exc_info:
            ProcessingConfig()

        error_str = str(exc_info.value)
        assert "not executable" in error_str

    def test_mseedindex_not_found_detailed_error(self, tmp_path, monkeypatch):
        """Missing mseedindex should provide detailed error message."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "nonexistent_command_12345")

        with pytest.raises(ValidationError) as exc_info:
            ProcessingConfig()

        error_str = str(exc_info.value)
        assert "not found" in error_str
        assert "nonexistent_command_12345" in error_str


class TestNoizConfigValidation:
    """Comprehensive validation tests for NoizConfig."""

    def test_flask_env_normalization(self, tmp_path, monkeypatch):
        """Flask env should be normalized to lowercase."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_FLASK_ENV", "PRODUCTION")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        assert config.flask_env == "production"

    def test_loglevel_normalization(self, tmp_path, monkeypatch):
        """Loglevel should be normalized to uppercase."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_LOGLEVEL", "debug")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        assert config.loglevel == "DEBUG"

    def test_loglevel_valid_values(self, tmp_path, monkeypatch):
        """All valid log levels should be accepted."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        valid_levels = ["DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"]

        for level in valid_levels:
            monkeypatch.setenv("NOIZ_LOGLEVEL", level)
            # Clear cached config
            get_config.cache_clear()

            config = NoizConfig()
            assert config.loglevel == level.upper()

    def test_repr_masks_secrets(self, tmp_path, monkeypatch):
        """repr should mask sensitive values."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_URL", "postgresql://user:secret123@localhost/db")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        repr_str = repr(config)

        # Secret should be masked
        assert "******" in repr_str
        assert "secret123" not in repr_str


class TestFlaskConfigGeneration:
    """Test to_flask_config() method."""

    def test_flask_config_has_required_keys(self, tmp_path, monkeypatch):
        """Flask config should contain all required keys."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        flask_config = config.to_flask_config()

        required_keys = [
            "SQLALCHEMY_DATABASE_URI",
            "SQLALCHEMY_TRACK_MODIFICATIONS",
            "DEBUG",
            "TESTING",
            "ENV",
            "DEBUG_TB_ENABLED",
            "DEBUG_TB_INTERCEPT_REDIRECTS",
            "CACHE_TYPE",
            "NOIZ_PROCESSED_DATA_DIR",
            "NOIZ_MSEEDINDEX_EXECUTABLE",
            "NOIZ_LOGLEVEL",
            "NOIZ_DATABASE_BACKEND",
        ]

        for key in required_keys:
            assert key in flask_config, f"Missing required key: {key}"

    def test_flask_config_backward_compatibility_keys(self, tmp_path, monkeypatch):
        """Flask config should include backward compatibility keys."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_POSTGRES_HOST", "localhost")
        monkeypatch.setenv("NOIZ_POSTGRES_PORT", "5432")
        monkeypatch.setenv("NOIZ_POSTGRES_USER", "user")
        monkeypatch.setenv("NOIZ_POSTGRES_PASSWORD", "pass")
        monkeypatch.setenv("NOIZ_POSTGRES_DB", "db")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        config = NoizConfig()
        flask_config = config.to_flask_config()

        # Backward compatibility keys (without NOIZ_ prefix)
        assert "PROCESSED_DATA_DIR" in flask_config
        assert "MSEEDINDEX_EXECUTABLE" in flask_config
        assert "DATABASE_BACKEND" in flask_config
        assert "POSTGRES_HOST" in flask_config
        assert "POSTGRES_PORT" in flask_config
        assert "POSTGRES_USER" in flask_config
        assert "POSTGRES_PASSWORD" in flask_config
        assert "POSTGRES_DB" in flask_config

    def test_flask_config_debug_derived_from_env(self, tmp_path, monkeypatch):
        """DEBUG flag should be derived from flask_env."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()

        monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
        monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
        monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

        # Test development mode
        monkeypatch.setenv("NOIZ_FLASK_ENV", "development")
        config_dev = NoizConfig()
        assert config_dev.to_flask_config()["DEBUG"] is True
        assert config_dev.to_flask_config()["DEBUG_TB_ENABLED"] is True

        # Test production mode
        monkeypatch.setenv("NOIZ_FLASK_ENV", "production")
        get_config.cache_clear()
        config_prod = NoizConfig()
        assert config_prod.to_flask_config()["DEBUG"] is False
        assert config_prod.to_flask_config()["DEBUG_TB_ENABLED"] is False

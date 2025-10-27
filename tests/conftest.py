# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

"""
Test fixtures for Noiz system tests.

This module provides per-module database fixtures that support both SQLite (default)
and PostgreSQL (via DATABASE_URL environment variable) for system testing.

Key fixtures:
    - module_db_session: Primary fixture for most tests (module-scoped)
    - db_session: Optional fixture for tests needing transaction rollback (function-scoped)
    - processed_data_dir: Per-module temporary directory for processed data output

See specs/003-sqlite-default-database/contracts/ for detailed contracts.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Generator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool


@pytest.fixture(scope="module")
def processed_data_dir(tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest) -> Path:
    """
    Generate unique temporary directory for processed data output.

    Creates per-module temporary directory for PROCESSED_DATA_DIR.
    Directory naming pattern: processed_data_{module_name}_{timestamp}

    :param tmp_path_factory: Pytest temporary path factory for automatic cleanup
    :type tmp_path_factory: pytest.TempPathFactory
    :param request: Pytest request fixture for accessing test context
    :type request: pytest.FixtureRequest
    :return: Absolute path to temporary processed data directory
    :rtype: Path
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    module_name = request.module.__name__.split(".")[-1]
    processed_dir = tmp_path_factory.mktemp(f"processed_data_{module_name}_{timestamp}")
    return processed_dir


@pytest.fixture(scope="module")
def module_db_path(tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest) -> Path:
    """
    Generate unique SQLite database file path for test module.

    Creates timestamped filename to prevent conflicts between test runs.
    File naming pattern: noiz-{module_name}-{timestamp}.db

    :param tmp_path_factory: Pytest temporary path factory for automatic cleanup
    :type tmp_path_factory: pytest.TempPathFactory
    :param request: Pytest request fixture for accessing test context
    :type request: pytest.FixtureRequest
    :return: Absolute path to SQLite database file
    :rtype: Path
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    module_name = request.module.__name__.split(".")[-1]
    temp_dir = tmp_path_factory.mktemp(f"noiz_db_{module_name}")
    db_path = temp_dir / f"noiz-{module_name}-{timestamp}.db"
    return db_path


@pytest.fixture(scope="module")
def module_database_url(module_db_path: Path, request: pytest.FixtureRequest) -> str:
    """
    Determine effective database URL based on environment.

    Precedence:
        1. DATABASE_URL environment variable (if set) - create unique database for this module
        2. No DATABASE_URL - use SQLite with module_db_path

    :param module_db_path: Path to SQLite database file for this module
    :type module_db_path: Path
    :param request: Pytest request fixture for accessing test context
    :type request: pytest.FixtureRequest
    :return: SQLAlchemy-compatible database URL
    :rtype: str
    :example:
        SQLite: "sqlite:////tmp/pytest-123/noiz-test_data_ingestion-20251021.db"
        PostgreSQL: "postgresql+psycopg2://user:pass@localhost:5432/test_db_module_20251021"
    """
    database_url = os.environ.get("DATABASE_URL")

    if database_url:
        # For PostgreSQL, create a unique database name per test module
        # This ensures test isolation without needing to drop/recreate
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        module_name = request.module.__name__.split(".")[-1]

        # Replace the database name in the URL with a unique one
        import re

        unique_db_name = f"noiz_test_{module_name}_{timestamp}"
        # Replace the last segment after the final slash
        database_url = re.sub(r"/[^/]+$", f"/{unique_db_name}", database_url)

        return database_url
    else:
        # Default to SQLite with module-specific timestamped file
        return f"sqlite:///{module_db_path}"


@pytest.fixture(scope="module")
def module_db_engine(module_database_url: str):
    """
    Create SQLAlchemy engine for test module.

    Configuration:
        - SQLite: NullPool (no connection pooling due to file locking)
        - PostgreSQL: Default pool (handled automatically)

    :param module_database_url: Database URL for this test module
    :type module_database_url: str
    :yields: Configured database engine
    :ytype: sqlalchemy.Engine
    """
    # Determine if SQLite
    is_sqlite = module_database_url.startswith("sqlite")

    # SQLite requires NullPool to avoid file locking issues
    if is_sqlite:
        engine = create_engine(module_database_url, poolclass=NullPool)
    else:
        engine = create_engine(module_database_url)

    yield engine

    # Cleanup
    engine.dispose()


@pytest.fixture(scope="module")
def module_db_session(module_db_engine, module_database_url, processed_data_dir) -> Generator[Session, None, None]:
    """
    Per-module database session fixture.

    **PRIMARY FIXTURE** - Use this for most system tests.

    Provides SQLAlchemy session configured for either SQLite or PostgreSQL
    based on DATABASE_URL environment variable.
    Runs migrations (db.create_all()) before yielding session to tests.

    Lifecycle:
        1. Created once per test module at module setup
        2. Shared by all test functions in the module
        3. Closed after all tests in module complete
        4. SQLite file automatically cleaned up by pytest

    :param module_db_engine: SQLAlchemy engine for this test module
    :type module_db_engine: sqlalchemy.Engine
    :param module_database_url: Database URL for this test module
    :type module_database_url: str
    :param processed_data_dir: Temporary directory for processed data output
    :type processed_data_dir: Path
    :yields: Database session for tests
    :ytype: sqlalchemy.orm.Session
    :note: Thread Safety: Not thread-safe (system tests run single-threaded)
    """
    from noiz.app import create_app
    from noiz.database import db

    # Set NOIZ_DATABASE_BACKEND environment variable based on database URL
    is_sqlite = module_database_url.startswith("sqlite")
    if is_sqlite:
        os.environ["NOIZ_DATABASE_BACKEND"] = "sqlite"
    else:
        os.environ["NOIZ_DATABASE_BACKEND"] = "postgresql"

    # Set NOIZ_DATABASE_URL to ensure new config system picks it up
    os.environ["NOIZ_DATABASE_URL"] = module_database_url

    # Set NOIZ_PROCESSED_DATA_DIR to the pytest temporary directory for this module
    os.environ["NOIZ_PROCESSED_DATA_DIR"] = str(processed_data_dir)

    # Set NOIZ_MSEEDINDEX_EXECUTABLE if not already set
    if "NOIZ_MSEEDINDEX_EXECUTABLE" not in os.environ:
        os.environ["NOIZ_MSEEDINDEX_EXECUTABLE"] = "mseedindex"

    # Create Flask app with test database
    app = create_app()
    app.config["SQLALCHEMY_DATABASE_URI"] = str(module_db_engine.url)
    app.config["TESTING"] = True

    with app.app_context():
        # For PostgreSQL, create a fresh database for this test module
        # SQLite gets a fresh temp file per module automatically
        if not is_sqlite:
            from sqlalchemy import create_engine, text
            from sqlalchemy.exc import ProgrammingError
            import re

            # Parse database name from URL
            match = re.search(r"/([^/]+)$", module_database_url)
            db_name = match.group(1) if match else None

            if db_name:
                # Create connection to postgres database (not the target database)
                postgres_url = module_database_url.rsplit("/", 1)[0] + "/postgres"
                admin_engine = create_engine(postgres_url, isolation_level="AUTOCOMMIT")

                with admin_engine.connect() as conn:
                    # Create the test database
                    try:
                        conn.execute(text(f"CREATE DATABASE {db_name}"))
                    except ProgrammingError as e:
                        # Database might already exist (e.g., if test was interrupted)
                        if "already exists" not in str(e):
                            raise

                admin_engine.dispose()

        # Run migrations using Flask-Migrate (Alembic)
        # This ensures proper schema creation with autoincrement handling
        from flask_migrate import upgrade as flask_migrate_upgrade
        import pathlib

        # Get the project root directory (parent of tests/)
        migrations_dir = pathlib.Path(__file__).parent.parent / "migrations"

        flask_migrate_upgrade(directory=str(migrations_dir))

        # Create session
        SessionLocal = sessionmaker(bind=module_db_engine)
        session = SessionLocal()

        yield session

        # Cleanup
        session.close()
        # For SQLite: tables remain in database (cleaned up by pytest temp file deletion)
        # For PostgreSQL: tables remain for inspection (dropped at start of next test run)


@pytest.fixture(scope="function")
def db_session(module_db_session: Session) -> Generator[Session, None, None]:
    """
    Function-scoped database session with transaction rollback.

    **OPTIONAL** - Use this fixture when tests need isolated database state
    (changes rolled back after each test function).

    Most tests should use module_db_session (faster, shared state).
    Use this fixture only when test isolation requires rollback.

    :param module_db_session: Module-scoped database session
    :type module_db_session: sqlalchemy.orm.Session
    :yields: Session with automatic rollback after test
    :ytype: sqlalchemy.orm.Session
    :note: Works better with PostgreSQL than SQLite due to nested transaction support
    """
    # Begin nested transaction
    transaction = module_db_session.begin_nested()

    yield module_db_session

    # Rollback after test
    transaction.rollback()


@pytest.fixture(scope="class")
def noiz_app(module_db_session):
    """
    Flask application fixture for system tests.

    This fixture overrides the inline noiz_app fixtures in system test files.
    It depends on module_db_session to ensure environment variables are set
    before Flask app creation.

    :param module_db_session: Module-scoped database session (ensures env vars set)
    :type module_db_session: sqlalchemy.orm.Session
    :return: Flask application instance
    :rtype: Flask
    """
    from noiz.app import create_app

    app = create_app(verbosity=5)
    return app

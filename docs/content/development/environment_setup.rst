.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

===================
Environment Setup
===================

This guide walks you through setting up a complete development environment for Noiz.

.. contents:: Table of Contents
   :local:
   :depth: 2

Prerequisites
=============

System Requirements
-------------------

* **Python**: 3.10 or higher (3.11-3.13 supported)
* **PostgreSQL**: 12 or higher (with PostGIS extension)
* **Git**: For version control
* **uv**: Fast Python package manager (recommended)

Operating Systems
-----------------

Noiz development is supported on:

* **Linux**: Ubuntu 20.04+, Debian 11+, RHEL 8+
* **macOS**: 11 (Big Sur) or higher
* **Windows**: WSL2 with Ubuntu recommended

Installing Prerequisites
========================

Python
------

**Linux (Ubuntu/Debian):**

.. code-block:: bash

    sudo apt-get update
    sudo apt-get install python3.10 python3.10-dev python3-pip

**macOS:**

.. code-block:: bash

    # Using Homebrew
    brew install python@3.10

**Verify installation:**

.. code-block:: bash

    python3 --version  # Should show 3.10 or higher

UV Package Manager
------------------

Install uv (recommended for faster dependency management):

.. code-block:: bash

    # Linux/macOS
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Or with pip
    pip install uv

    # Verify installation
    uv --version

PostgreSQL
----------

**Linux (Ubuntu/Debian):**

.. code-block:: bash

    sudo apt-get update
    sudo apt-get install postgresql postgresql-contrib postgis

**macOS:**

.. code-block:: bash

    # Using Homebrew
    brew install postgresql@14 postgis

    # Start PostgreSQL
    brew services start postgresql@14

**Start PostgreSQL service:**

.. code-block:: bash

    # Linux
    sudo systemctl start postgresql
    sudo systemctl enable postgresql

**Create database and user:**

.. code-block:: bash

    # Switch to postgres user
    sudo -u postgres psql

    # In psql prompt
    CREATE USER noizdev WITH PASSWORD 'noizdev';
    CREATE DATABASE noizdev OWNER noizdev;
    \c noizdev
    CREATE EXTENSION postgis;
    \q

Git
---

**Linux:**

.. code-block:: bash

    sudo apt-get install git

**macOS:**

.. code-block:: bash

    brew install git

**Configure Git:**

.. code-block:: bash

    git config --global user.name "Your Name"
    git config --global user.email "your.email@example.com"

Project Setup
=============

Clone Repository
----------------

.. code-block:: bash

    # Clone the repository
    git clone https://gitlab.com/your-org/noiz.git
    cd noiz

    # Or if using SSH
    git clone git@gitlab.com:your-org/noiz.git
    cd noiz

Install Dependencies
--------------------

**Using uv (recommended):**

.. code-block:: bash

    # Install all dependencies including dev dependencies
    uv sync --all-groups

    # This creates a virtual environment and installs:
    # - Runtime dependencies
    # - Development dependencies (pytest, mypy, ruff, etc.)
    # - Documentation dependencies (sphinx, etc.)

**Using pip (alternative):**

.. code-block:: bash

    # Create virtual environment
    python3 -m venv .venv

    # Activate virtual environment
    source .venv/bin/activate  # Linux/macOS
    # Or on Windows: .venv\Scripts\activate

    # Install dependencies
    pip install -e ".[dev,docs,test]"

Environment Variables
=====================

Required Variables
------------------

Create a ``.env`` file in the project root:

.. code-block:: bash

    # Database configuration
    POSTGRES_HOST=localhost
    POSTGRES_PORT=5432
    POSTGRES_USER=noizdev
    POSTGRES_PASSWORD=noizdev
    POSTGRES_DB=noizdev

    # Or use DATABASE_URL directly
    # DATABASE_URL=postgresql+psycopg2://noizdev:noizdev@localhost:5432/noizdev

    # Data directory (create this directory first)
    PROCESSED_DATA_DIR=/path/to/noiz/data

    # MSEEDINDEX executable (install separately)
    MSEEDINDEX_EXECUTABLE=/usr/local/bin/mseedindex

    # Flask environment
    FLASK_ENV=development

Create Data Directory
---------------------

.. code-block:: bash

    # Create directory for processed data
    mkdir -p /path/to/noiz/data

    # Make sure it's writable
    chmod 755 /path/to/noiz/data

Install MSEEDINDEX
------------------

MSEEDINDEX is required for reading seismic data:

.. code-block:: bash

    # Download from IRIS
    # https://github.com/iris-edu/mseedindex

    # Or install from source
    git clone https://github.com/iris-edu/mseedindex.git
    cd mseedindex
    make
    sudo make install

Database Setup
==============

Initialize Database
-------------------

Run database migrations to set up the schema:

.. code-block:: bash

    # Run migrations
    uv run flask db upgrade

    # Or if using activated venv
    flask db upgrade

Verify Database Setup
---------------------

.. code-block:: bash

    # Check database connectivity
    psql -U noizdev -d noizdev -h localhost

    # List tables
    \dt

    # Should see tables like:
    # - datachunk
    # - component
    # - timespan
    # - etc.

Development Tools Setup
=======================

Pre-commit Hooks
----------------

Install and configure pre-commit hooks for automatic code quality checks:

.. code-block:: bash

    # Install pre-commit
    pip install pre-commit

    # Install hooks
    pre-commit install

    # Run manually on all files
    pre-commit run --all-files

See :doc:`pre_commit_hooks` for detailed configuration.

IDE Configuration
-----------------

**VS Code**

Create ``.vscode/settings.json``:

.. code-block:: json

    {
        "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
        "python.linting.enabled": true,
        "python.linting.mypyEnabled": true,
        "python.linting.ruffEnabled": true,
        "python.formatting.provider": "ruff",
        "python.testing.pytestEnabled": true,
        "python.testing.pytestArgs": [
            "tests"
        ],
        "editor.rulers": [119],
        "editor.formatOnSave": true,
        "[python]": {
            "editor.codeActionsOnSave": {
                "source.organizeImports": true
            }
        }
    }

**PyCharm**

1. Open Settings (Preferences on macOS)
2. Project > Python Interpreter > Add Interpreter
3. Select existing environment: ``.venv/bin/python``
4. Enable mypy: Tools > External Tools > Add mypy
5. Enable ruff: Settings > Tools > External Tools > Add ruff

Verify Installation
===================

Run Tests
---------

.. code-block:: bash

    # Run all tests
    uv run pytest

    # Run with coverage
    uv run pytest --cov=noiz

    # Expected output: All tests should pass

Run Type Checker
----------------

.. code-block:: bash

    # Run mypy
    mypy src/noiz

    # Expected: No type errors (or only known issues)

Run Linter
----------

.. code-block:: bash

    # Check code style
    ruff check .

    # Auto-fix issues
    ruff check --fix .

Build Documentation
-------------------

.. code-block:: bash

    # Build HTML docs
    cd docs
    make html

    # Open in browser
    open _build/html/index.html  # macOS
    # Or: xdg-open _build/html/index.html  # Linux

Command Runner (just)
=====================

Noiz includes a ``justfile`` with common development commands for convenience.

Installing just
---------------

``just`` is a command runner similar to ``make`` but simpler and more intuitive.

**Option 1: Using uv tool (Recommended)**

.. code-block:: bash

    # Install just via Python wrapper
    uv tool install rust-just

    # Verify installation
    just --version

**Option 2: Using cargo (Rust)**

.. code-block:: bash

    # Install just via cargo
    cargo install just

    # Verify installation
    just --version

**Option 3: Using package managers**

.. code-block:: bash

    # macOS
    brew install just

    # Linux (Ubuntu/Debian)
    # Download from https://github.com/casey/just/releases

Available Commands
------------------

List all available commands:

.. code-block:: bash

    just --list

Common commands:

**Development**:

.. code-block:: bash

    # Install dependencies
    just sync

    # Run unit tests
    just unit_tests

    # Run system tests
    just run_system_tests

**Code Quality**:

.. code-block:: bash

    # Run ruff linter
    just ruff_check

    # Run ruff formatter
    just ruff_format

    # Run both ruff checks and formatting
    just ruff

    # Run mypy type checker
    just mypy

**Documentation**:

.. code-block:: bash

    # Build HTML documentation
    just docs

    # Lint documentation
    just lint_docs

**Cleanup**:

.. code-block:: bash

    # Clean up after system tests
    just clean_after_tests

Using Justfile Commands
------------------------

The justfile is located at the project root and contains shortcuts for common tasks:

.. code-block:: bash

    # Install dependencies (equivalent to: uv sync --all-groups)
    just sync

    # Run tests with coverage (equivalent to: uv run pytest --cov=noiz)
    just unit_tests

    # Format and lint code
    just ruff

    # Build documentation
    just docs

For the complete list of available commands and their implementations, see ``justfile``
in the project root.

Docker Setup (Optional)
=======================

Using Docker Compose
--------------------

For a containerized development environment:

.. code-block:: bash

    # Start all services
    docker-compose up -d

    # Run migrations
    docker-compose exec noiz flask db upgrade

    # Run tests in container
    docker-compose exec noiz pytest

    # Stop services
    docker-compose down

See ``docker-compose.yml`` for service configuration.

Troubleshooting
===============

Common Issues
-------------

**Issue: "ModuleNotFoundError: No module named 'noiz'"**

**Solution:**

.. code-block:: bash

    # Make sure you've installed in editable mode
    pip install -e .

    # Or with uv
    uv sync

**Issue: "psycopg2 installation fails"**

**Solution:**

.. code-block:: bash

    # Install PostgreSQL development headers
    # Ubuntu/Debian:
    sudo apt-get install libpq-dev python3-dev

    # macOS:
    brew install postgresql

**Issue: "Permission denied: '/path/to/noiz/data'"**

**Solution:**

.. code-block:: bash

    # Make directory writable
    chmod 755 /path/to/noiz/data

    # Or change ownership
    sudo chown -R $USER:$USER /path/to/noiz/data

**Issue: "Database connection refused"**

**Solution:**

.. code-block:: bash

    # Check PostgreSQL is running
    sudo systemctl status postgresql

    # Start if stopped
    sudo systemctl start postgresql

    # Check connection
    psql -U noizdev -d noizdev -h localhost

**Issue: "Flask-Migrate errors during upgrade"**

**Solution:**

.. code-block:: bash

    # Downgrade and re-upgrade
    flask db downgrade
    flask db upgrade

    # Or start fresh (WARNING: deletes data)
    dropdb noizdev
    createdb noizdev
    flask db upgrade

Getting Help
============

If you encounter issues not covered here:

1. Check the `troubleshooting guide <../miscellaneous/troubleshooting.html>`_
2. Search existing issues on GitLab
3. Ask in the development chat
4. Create a new issue with:
   - Your OS and Python version
   - Error messages
   - Steps to reproduce

Next Steps
==========

After setting up your environment:

1. Read :doc:`coding_standards` to understand code conventions
2. Review :doc:`type_checking` for type hint requirements
3. Check :doc:`design_decisions/index` for architectural decisions
4. Look at open issues to find something to work on
5. Submit your first pull request!

See Also
========

* :doc:`pre_commit_hooks` - Pre-commit hooks configuration
* :doc:`coding_standards` - Code style and conventions
* :doc:`type_checking` - Type checking with mypy
* :doc:`../guides/running_system_tests_locally` - Running system tests

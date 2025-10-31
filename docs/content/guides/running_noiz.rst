.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

============
Running Noiz
============

This guide provides comprehensive instructions for running Noiz with different database backends and
environment configurations.

.. contents:: Table of Contents
   :local:
   :depth: 3

Overview
========

Noiz requires three key components to run:

1. **Database**: Either SQLite (for development/testing) or PostgreSQL (for production)
2. **Processed Data Directory**: Location where Noiz stores processed seismic data
3. **MSeedIndex**: Tool for indexing seismic data files (auto-discovered from PATH)

All configuration is done via environment variables with the ``NOIZ_`` prefix.

Quick Start Guides
==================

SQLite Quick Start
------------------

SQLite is perfect for getting started, development, testing, or single-user scenarios.
No database server required.

**Step 1: Install Dependencies**

.. code-block:: bash

    # Clone repository
    git clone https://gitlab.com/your-org/noiz.git
    cd noiz

    # Install dependencies
    uv sync --all-groups

**Step 2: Create .env File**

Create a file named ``.env`` in the project root:

.. code-block:: bash

    # SQLite Backend Configuration
    NOIZ_DATABASE_BACKEND=sqlite
    NOIZ_DATABASE_URL=sqlite:///noiz.db
    NOIZ_PROCESSED_DATA_DIR=/tmp/noiz-data
    NOIZ_FLASK_ENV=development
    NOIZ_LOGLEVEL=INFO

**Step 3: Load Environment and Run**

.. code-block:: bash

    # Load environment variables
    set -a
    source .env
    set +a

    # Run database migrations
    uv run noiz db upgrade

    # Verify installation
    uv run noiz --help

    # Start using Noiz
    uv run noiz data add_inventory --help

That's it!
You're ready to start using Noiz with SQLite.

PostgreSQL Quick Start
----------------------

PostgreSQL is recommended for production deployments, multi-user environments, and large datasets.

**Step 1: Install PostgreSQL**

.. code-block:: bash

    # Ubuntu/Debian
    sudo apt-get update
    sudo apt-get install postgresql postgresql-contrib postgis

    # macOS
    brew install postgresql@14 postgis
    brew services start postgresql@14

**Step 2: Create Database and User**

.. code-block:: bash

    # Connect to PostgreSQL as superuser
    sudo -u postgres psql

.. code-block:: sql

    -- Create user
    CREATE USER noiz WITH PASSWORD 'your_secure_password';

    -- Create database
    CREATE DATABASE noiz OWNER noiz;

    -- Connect to database
    \c noiz

    -- Enable PostGIS extension (if needed for spatial data)
    CREATE EXTENSION postgis;

    -- Exit
    \q

**Step 3: Create .env File**

Create a file named ``.env`` in the project root:

.. code-block:: bash

    # PostgreSQL Backend Configuration
    NOIZ_DATABASE_BACKEND=postgresql
    NOIZ_DATABASE_URL=postgresql+psycopg2://noiz:your_secure_password@localhost:5432/noiz
    NOIZ_PROCESSED_DATA_DIR=/data/noiz/processed
    NOIZ_FLASK_ENV=production
    NOIZ_LOGLEVEL=INFO

**Step 4: Create Processed Data Directory**

.. code-block:: bash

    # Create directory with appropriate permissions
    sudo mkdir -p /data/noiz/processed
    sudo chown $USER:$USER /data/noiz/processed

**Step 5: Load Environment and Run**

.. code-block:: bash

    # Load environment variables
    set -a
    source .env
    set +a

    # Run database migrations
    uv run noiz db upgrade

    # Verify installation
    uv run noiz --help

    # Start using Noiz
    uv run noiz data add_inventory --help

Complete .env File Examples
============================

Development with SQLite
-----------------------

Perfect for local development, testing, and learning Noiz.

.. code-block:: bash

    # ==================================
    # Noiz Development Configuration
    # Database: SQLite (local file)
    # ==================================

    # Database Backend
    NOIZ_DATABASE_BACKEND=sqlite

    # SQLite database file (relative to working directory)
    # Use 3 slashes for relative paths: sqlite:///filename.db
    # Use 4 slashes for absolute paths: sqlite:////absolute/path/to/db
    NOIZ_DATABASE_URL=sqlite:///noiz_dev.db

    # Processed data directory (will be created if it doesn't exist)
    NOIZ_PROCESSED_DATA_DIR=/tmp/noiz-dev-data

    # MSeedIndex executable (auto-discovered from PATH if not set)
    # Uncomment and set if you need to specify a custom path
    # NOIZ_MSEEDINDEX_EXECUTABLE=/usr/local/bin/mseedindex

    # Flask environment (enables debug mode, debug toolbar)
    NOIZ_FLASK_ENV=development

    # Logging level (DEBUG for verbose output)
    NOIZ_LOGLEVEL=DEBUG

    # Optional: Enable SQLAlchemy warnings for migration to SQLAlchemy 2.0
    SQLALCHEMY_WARN_20=1

Production with PostgreSQL (Connection URI)
--------------------------------------------

Recommended for production deployments.
Uses a single DATABASE_URL for clean configuration.

.. code-block:: bash

    # ==================================
    # Noiz Production Configuration
    # Database: PostgreSQL
    # Method: Connection URI
    # ==================================

    # Database Backend
    NOIZ_DATABASE_BACKEND=postgresql

    # PostgreSQL connection URI
    # Format: postgresql+psycopg2://user:password@host:port/database
    NOIZ_DATABASE_URL=postgresql+psycopg2://noiz:your_secure_password@localhost:5432/noiz_prod

    # Processed data directory (must exist with correct permissions)
    NOIZ_PROCESSED_DATA_DIR=/data/noiz/processed

    # MSeedIndex executable
    NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex

    # Flask environment (disables debug features)
    NOIZ_FLASK_ENV=production

    # Logging level (INFO for production)
    NOIZ_LOGLEVEL=INFO

Production with PostgreSQL (Individual Parameters)
---------------------------------------------------

Alternative method using separate environment variables for each connection parameter.

.. code-block:: bash

    # ==================================
    # Noiz Production Configuration
    # Database: PostgreSQL
    # Method: Individual Parameters
    # ==================================

    # Database Backend
    NOIZ_DATABASE_BACKEND=postgresql

    # PostgreSQL connection parameters
    NOIZ_POSTGRES_HOST=localhost
    NOIZ_POSTGRES_PORT=5432
    NOIZ_POSTGRES_USER=noiz
    NOIZ_POSTGRES_PASSWORD=your_secure_password
    NOIZ_POSTGRES_DB=noiz_prod

    # Processed data directory
    NOIZ_PROCESSED_DATA_DIR=/data/noiz/processed

    # MSeedIndex executable
    NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex

    # Flask environment
    NOIZ_FLASK_ENV=production

    # Logging level
    NOIZ_LOGLEVEL=INFO

Testing Configuration
---------------------

Configuration optimized for running tests.

.. code-block:: bash

    # ==================================
    # Noiz Testing Configuration
    # Database: SQLite (in-memory or temp file)
    # ==================================

    # Database Backend
    NOIZ_DATABASE_BACKEND=sqlite

    # SQLite database in temp directory
    NOIZ_DATABASE_URL=sqlite:////tmp/noiz_test.db

    # Processed data directory in temp
    NOIZ_PROCESSED_DATA_DIR=/tmp/noiz-test-data

    # MSeedIndex executable
    NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex

    # Flask environment
    NOIZ_FLASK_ENV=development

    # Verbose logging for debugging test failures
    NOIZ_LOGLEVEL=DEBUG

    # Enable SQLAlchemy warnings
    SQLALCHEMY_WARN_20=1

Docker/Container Configuration
-------------------------------

Configuration for running Noiz in Docker containers.

.. code-block:: bash

    # ==================================
    # Noiz Docker Configuration
    # Database: PostgreSQL (external service)
    # ==================================

    # Database Backend
    NOIZ_DATABASE_BACKEND=postgresql

    # PostgreSQL connection to docker service
    # 'postgres' is the service name in docker-compose.yml
    NOIZ_DATABASE_URL=postgresql+psycopg2://noiz:noiz_password@postgres:5432/noiz

    # Processed data directory (mount as volume)
    NOIZ_PROCESSED_DATA_DIR=/app/data/processed

    # MSeedIndex executable (installed in container)
    NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex

    # Flask environment
    NOIZ_FLASK_ENV=production

    # Logging level
    NOIZ_LOGLEVEL=INFO

Managing Environment Variables
===============================

Method 1: Manual Export (Basic)
--------------------------------

Export variables manually before running Noiz.

.. code-block:: bash

    # Export all variables
    export NOIZ_DATABASE_BACKEND=sqlite
    export NOIZ_DATABASE_URL=sqlite:///noiz.db
    export NOIZ_PROCESSED_DATA_DIR=/tmp/noiz-data
    export NOIZ_FLASK_ENV=development
    export NOIZ_LOGLEVEL=INFO

    # Run Noiz
    uv run noiz --help

**Pros:**
- Simple and straightforward
- No additional tools required
- Good for CI/CD scripts

**Cons:**
- Variables persist only in current shell session
- Must export every time you start a new terminal
- Easy to forget to set variables

Method 2: Source .env File
--------------------------

Load variables from a ``.env`` file using shell's ``source`` command.

.. code-block:: bash

    # Create .env file (see examples above)
    cat > .env << 'EOF'
    NOIZ_DATABASE_BACKEND=sqlite
    NOIZ_DATABASE_URL=sqlite:///noiz.db
    NOIZ_PROCESSED_DATA_DIR=/tmp/noiz-data
    NOIZ_FLASK_ENV=development
    NOIZ_LOGLEVEL=INFO
    EOF

    # Load environment variables (bash/zsh)
    set -a  # Automatically export all variables
    source .env
    set +a  # Disable automatic export

    # Run Noiz
    uv run noiz --help

**Pros:**
- Single file for all configuration
- Easy to maintain multiple configurations (.env.dev, .env.prod)
- Can be version controlled (without secrets)

**Cons:**
- Must source file in every new shell
- Variables persist in shell after sourcing

**Important Security Note:**

Never commit ``.env`` files containing passwords or secrets to version control.
Add ``.env`` to your ``.gitignore``:

.. code-block:: bash

    echo ".env" >> .gitignore
    echo ".env.*" >> .gitignore

Method 3: Using direnv (Recommended)
-------------------------------------

``direnv`` automatically loads and unloads environment variables when you enter or leave a directory.
This is the recommended approach for development.

**Installation**

.. code-block:: bash

    # macOS
    brew install direnv

    # Ubuntu/Debian
    sudo apt-get install direnv

    # Arch Linux
    sudo pacman -S direnv

**Shell Integration**

Add to your shell configuration file:

.. code-block:: bash

    # For bash: Add to ~/.bashrc
    eval "$(direnv hook bash)"

    # For zsh: Add to ~/.zshrc
    eval "$(direnv hook zsh)"

    # For fish: Add to ~/.config/fish/config.fish
    direnv hook fish | source

**Setup for Noiz**

.. code-block:: bash

    # Navigate to Noiz directory
    cd /path/to/noiz

    # Create .envrc file that loads your .env
    echo 'dotenv' > .envrc

    # Allow direnv to load .envrc
    direnv allow

    # Variables are now automatically loaded!
    # Test it:
    echo $NOIZ_DATABASE_BACKEND

**Create .env file**

.. code-block:: bash

    # Create your .env configuration
    cat > .env << 'EOF'
    NOIZ_DATABASE_BACKEND=sqlite
    NOIZ_DATABASE_URL=sqlite:///noiz.db
    NOIZ_PROCESSED_DATA_DIR=/tmp/noiz-data
    NOIZ_FLASK_ENV=development
    NOIZ_LOGLEVEL=INFO
    EOF

**How It Works**

When you ``cd`` into the Noiz directory:
- ``direnv`` automatically loads variables from ``.env``
- Variables are available to all commands
- Environment is clean (no pollution)

When you ``cd`` out of the directory:
- Variables are automatically unloaded
- No environment pollution in other projects

**Pros:**
- Automatic loading/unloading per directory
- Clean environment management
- Perfect for multiple projects with different configs
- Widely used in development workflows
- Integrates with most shells and editors

**Cons:**
- Requires installation and shell configuration
- Slight learning curve

**Security with direnv:**

.. code-block:: bash

    # Add to .gitignore
    echo ".env" >> .gitignore
    echo ".envrc" >> .gitignore

    # Or commit .envrc but not .env
    # .envrc (can be committed):
    echo 'dotenv' > .envrc

    # .env (NEVER commit with secrets)
    # Contains actual configuration values

Method 4: Using env_file with Commands
---------------------------------------

Pass environment file directly to commands.

.. code-block:: bash

    # Run with docker
    docker run --env-file .env noiz:latest

    # Run with systemd
    # In /etc/systemd/system/noiz.service:
    [Service]
    EnvironmentFile=/path/to/noiz/.env
    ExecStart=/path/to/noiz/.venv/bin/noiz

**Pros:**
- Explicit configuration per command
- Good for containerized deployments
- Variables don't pollute shell

**Cons:**
- Requires tool support (docker, systemd, etc.)
- Less convenient for interactive development

Method 5: Shell Environment Managers
-------------------------------------

Alternative tools for managing environment variables:

**autoenv**

Automatically sources ``.env`` when entering directory:

.. code-block:: bash

    # Install
    git clone git://github.com/kennethreitz/autoenv.git ~/.autoenv
    echo 'source ~/.autoenv/activate.sh' >> ~/.bashrc

    # Use
    echo 'source .env' > .env
    cd /path/to/noiz  # Automatically sources .env

**dotenv CLI**

Python tool for loading .env files:

.. code-block:: bash

    # Install
    pip install python-dotenv

    # Run commands with .env loaded
    dotenv run noiz --help

Recommended Workflow
--------------------

For most developers, we recommend **Method 3: Using direnv**:

1. Install direnv and configure your shell
2. Create ``.env`` file with your configuration
3. Create ``.envrc`` with ``dotenv`` content
4. Run ``direnv allow``
5. Work normally - environment loads automatically

For production deployments, use **Method 1: Manual Export** in deployment scripts or
**Method 4: env_file** with containers.

Database Setup and Migrations
==============================

Initial Database Setup
----------------------

After configuring your environment, initialize the database:

.. code-block:: bash

    # Load your environment (using any method above)
    set -a
    source .env
    set +a

    # Run migrations to create all tables
    uv run noiz db upgrade

    # Verify tables were created
    # For SQLite:
    sqlite3 $NOIZ_DATABASE_URL "SELECT name FROM sqlite_master WHERE type='table';"

    # For PostgreSQL:
    psql $NOIZ_DATABASE_URL -c "\dt"

Expected tables include:
- ``component``
- ``component_pair``
- ``datachunk``
- ``timespan``
- ``crosscorrelation_cartesian``
- And many more...

Creating a New Migration
------------------------

If you've modified database models:

.. code-block:: bash

    # Generate migration
    uv run noiz db migrate -m "description of changes"

    # Review the generated migration file in migrations/versions/

    # Apply migration
    uv run noiz db upgrade

Rolling Back Migrations
------------------------

.. code-block:: bash

    # Downgrade one version
    uv run noiz db downgrade

    # Downgrade to specific version
    uv run noiz db downgrade <revision_id>

    # View migration history
    uv run noiz db history

Switching Between SQLite and PostgreSQL
----------------------------------------

You can switch between databases by changing your configuration:

**From SQLite to PostgreSQL:**

.. code-block:: bash

    # Update .env
    NOIZ_DATABASE_BACKEND=postgresql
    NOIZ_DATABASE_URL=postgresql+psycopg2://noiz:password@localhost:5432/noiz

    # Reload environment
    source .env

    # Run migrations
    uv run noiz db upgrade

**Note:** Data is not automatically migrated.
You'll need to export/import data manually if needed.

**From PostgreSQL to SQLite:**

.. code-block:: bash

    # Update .env
    NOIZ_DATABASE_BACKEND=sqlite
    NOIZ_DATABASE_URL=sqlite:///noiz.db

    # Reload environment
    source .env

    # Run migrations
    uv run noiz db upgrade

Common Workflows
================

Development Workflow
--------------------

Typical day-to-day development workflow:

.. code-block:: bash

    # 1. Start your day - cd into project (direnv loads automatically)
    cd ~/projects/noiz
    # Environment variables automatically loaded by direnv!

    # 2. Pull latest changes
    git pull

    # 3. Update dependencies if needed
    uv sync --all-groups

    # 4. Run any new migrations
    uv run noiz db upgrade

    # 5. Run tests to ensure everything works
    just unit_tests

    # 6. Start development
    # ... make changes ...

    # 7. Run tests again
    just unit_tests

    # 8. Format and lint code
    just ruff

    # 9. Commit changes
    git add .
    git commit -m "feat: add new feature"

Testing Workflow
----------------

Running tests with different configurations:

.. code-block:: bash

    # Unit tests (fast, use SQLite in-memory)
    just unit_tests

    # System tests (slower, use SQLite file)
    just system_tests

    # Run specific test file
    uv run pytest tests/unit/test_config.py

    # Run with verbose output
    uv run pytest -vv tests/

    # Run with coverage report
    uv run pytest --cov=noiz --cov-report=html

Production Deployment Workflow
-------------------------------

Deploying to production:

.. code-block:: bash

    # 1. SSH into production server
    ssh production-server

    # 2. Navigate to application directory
    cd /opt/noiz

    # 3. Pull latest release
    git fetch
    git checkout v1.2.3  # Use specific version tag

    # 4. Update dependencies
    uv sync --all-groups

    # 5. Verify configuration
    cat .env  # Check all required variables are set

    # 6. Backup database (PostgreSQL)
    pg_dump noiz > backup_$(date +%Y%m%d_%H%M%S).sql

    # 7. Run migrations
    source .env
    uv run noiz db upgrade

    # 8. Restart application
    sudo systemctl restart noiz

    # 9. Verify application is running
    sudo systemctl status noiz
    curl localhost:5000/health  # If you have a health endpoint

Data Processing Workflow
-------------------------

Typical workflow for processing seismic data:

.. code-block:: bash

    # 1. Ensure environment is loaded
    cd /path/to/noiz  # direnv loads automatically

    # 2. Import seismic data and inventory
    uv run noiz data add_inventory --path /data/inventory.xml
    uv run noiz data add_seismic_data --path /data/seismic/

    # 3. Generate timespans for processing
    uv run noiz data add_timespans --startdate 2023-01-01 --enddate 2023-12-31

    # 4. Prepare datachunks
    uv run noiz processing prepare_datachunks --startdate 2023-01-01 --enddate 2023-12-31

    # 5. Run quality control
    uv run noiz processing run_qcone --startdate 2023-01-01 --enddate 2023-12-31

    # 6. Process datachunks (filtering, etc.)
    uv run noiz processing process_datachunks --config_id 1

    # 7. Run cross-correlations
    uv run noiz processing run_crosscorrelations_cartesian --config_id 1

    # 8. Stack cross-correlations
    uv run noiz processing run_stacking --config_id 1

Troubleshooting
===============

Environment Variables Not Loading
----------------------------------

**Problem:** Commands fail with "field required" validation errors.

**Solutions:**

1. Check if variables are actually set:

   .. code-block:: bash

       env | grep NOIZ_

2. Verify you've loaded your .env:

   .. code-block:: bash

       set -a
       source .env
       set +a

3. For direnv, check if it's allowed:

   .. code-block:: bash

       direnv status
       direnv allow

4. Verify .envrc contents:

   .. code-block:: bash

       cat .envrc
       # Should contain: dotenv

Database Connection Errors
---------------------------

**Problem:** "could not connect to server" or "connection refused"

**Solutions:**

1. Verify PostgreSQL is running:

   .. code-block:: bash

       # Check status
       sudo systemctl status postgresql

       # Start if stopped
       sudo systemctl start postgresql

2. Test connection manually:

   .. code-block:: bash

       psql -U noiz -d noiz -h localhost -W

3. Check connection parameters:

   .. code-block:: bash

       echo $NOIZ_DATABASE_URL
       # Or
       echo $NOIZ_POSTGRES_HOST
       echo $NOIZ_POSTGRES_PORT
       echo $NOIZ_POSTGRES_USER

4. Verify user has access:

   .. code-block:: sql

       -- As postgres user
       sudo -u postgres psql
       \du  -- List users
       \l   -- List databases

Permission Errors on Processed Data Directory
----------------------------------------------

**Problem:** "Permission denied" when writing to ``NOIZ_PROCESSED_DATA_DIR``

**Solutions:**

1. Check directory permissions:

   .. code-block:: bash

       ls -ld $NOIZ_PROCESSED_DATA_DIR

2. Fix permissions:

   .. code-block:: bash

       # Make directory owned by your user
       sudo chown -R $USER:$USER $NOIZ_PROCESSED_DATA_DIR

       # Ensure it's writable
       chmod 755 $NOIZ_PROCESSED_DATA_DIR

3. Create directory if it doesn't exist:

   .. code-block:: bash

       mkdir -p $NOIZ_PROCESSED_DATA_DIR

Migration Errors
----------------

**Problem:** Migration fails or leaves database in inconsistent state

**Solutions:**

1. Check current migration status:

   .. code-block:: bash

       uv run noiz db current

2. View migration history:

   .. code-block:: bash

       uv run noiz db history

3. Force database to specific version (use with caution):

   .. code-block:: bash

       uv run noiz db stamp <revision_id>

4. Start fresh (WARNING: destroys all data):

   .. code-block:: bash

       # SQLite
       rm $NOIZ_DATABASE_URL
       uv run noiz db upgrade

       # PostgreSQL
       dropdb noiz
       createdb noiz
       psql noiz -c "CREATE EXTENSION postgis;"
       uv run noiz db upgrade

MSeedIndex Not Found
--------------------

**Problem:** "NOIZ_MSEEDINDEX_EXECUTABLE not found: mseedindex"

**Solutions:**

1. Check if mseedindex is in PATH:

   .. code-block:: bash

       which mseedindex

2. Install mseedindex:

   .. code-block:: bash

       # Via package manager (if available)
       sudo apt-get install mseedindex  # Ubuntu/Debian
       brew install mseedindex          # macOS

       # Or from source
       git clone https://github.com/iris-edu/mseedindex.git
       cd mseedindex
       make
       sudo make install

3. Specify full path in .env:

   .. code-block:: bash

       NOIZ_MSEEDINDEX_EXECUTABLE=/usr/local/bin/mseedindex

Best Practices
==============

Security
--------

1. **Never commit secrets** - Add ``.env`` to ``.gitignore``
2. **Use strong passwords** for PostgreSQL users
3. **Restrict file permissions** on .env files: ``chmod 600 .env``
4. **Use different passwords** for dev, test, and production
5. **Set NOIZ_FLASK_ENV=production** in production environments
6. **Use environment variables** in CI/CD, not .env files
7. **Rotate passwords regularly** in production

Configuration Management
------------------------

1. **Use .env.example** template (without secrets) in version control
2. **Document all required variables** in README
3. **Validate configuration** before deployment: ``noiz --help``
4. **Use absolute paths** for NOIZ_PROCESSED_DATA_DIR
5. **Test configuration changes** in non-production first
6. **Keep separate configs** for dev, test, and production
7. **Use DATABASE_URL** in production for simpler config

Development
-----------

1. **Use SQLite** for local development
2. **Use direnv** for automatic environment loading
3. **Run tests frequently** - ``just unit_tests``
4. **Use debug logging** - ``NOIZ_LOGLEVEL=DEBUG``
5. **Keep .env.example updated** when adding new variables
6. **Document non-obvious settings** in comments

Production
----------

1. **Use PostgreSQL** for production
2. **Set NOIZ_FLASK_ENV=production**
3. **Use INFO or WARNING** log level
4. **Backup database regularly**
5. **Monitor disk space** in NOIZ_PROCESSED_DATA_DIR
6. **Use specific version tags**, not ``main`` branch
7. **Test migrations on copy of production data** first
8. **Have rollback plan** for deployments

Further Reading
===============

- :doc:`configuration` - Detailed configuration reference
- :doc:`/development/environment_setup` - Development environment setup
- :doc:`/tutorials/create_noiz_project` - Creating a new Noiz project
- :doc:`running_system_tests_locally` - Running system tests
- :doc:`/development/design_documents/config_system` - Configuration system design

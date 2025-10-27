.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

Configuration
=============

Noiz uses environment variables for configuration management with type-safe validation via pydantic-settings.
All configuration variables use the ``NOIZ_`` prefix to avoid conflicts with other applications.

Quick Start
-----------

Set the required environment variables before running Noiz:

.. code-block:: bash

    # Database configuration (PostgreSQL example)
    export NOIZ_DATABASE_BACKEND=postgresql
    export NOIZ_POSTGRES_HOST=localhost
    export NOIZ_POSTGRES_PORT=5432
    export NOIZ_POSTGRES_USER=noiz
    export NOIZ_POSTGRES_PASSWORD=your_password
    export NOIZ_POSTGRES_DB=noiz

    # Processing configuration
    export NOIZ_PROCESSED_DATA_DIR=/data/processed

    # Run Noiz (mseedindex auto-discovered from PATH)
    noiz --help

Required Variables
------------------

These environment variables must be set for Noiz to function:

Database Connection
~~~~~~~~~~~~~~~~~~~

You must configure database access using one of two methods:

**Method 1: Connection URI (Recommended)**

.. code-block:: bash

    export NOIZ_DATABASE_URL=postgresql://user:password@host:port/database

This single variable contains all connection information.
It takes precedence if both methods are provided.

**Method 2: Individual Parameters**

.. code-block:: bash

    export NOIZ_POSTGRES_HOST=localhost
    export NOIZ_POSTGRES_PORT=5432
    export NOIZ_POSTGRES_USER=noiz
    export NOIZ_POSTGRES_PASSWORD=your_password
    export NOIZ_POSTGRES_DB=noiz

All five parameters are required when using this method.

Processing Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    export NOIZ_PROCESSED_DATA_DIR=/path/to/processed/data

``NOIZ_PROCESSED_DATA_DIR``
    Directory where processed seismic data will be stored.
    The directory will be created automatically if it does not exist.
    Must be an empty directory or a new path to avoid naming conflicts.

Optional Variables
------------------

MiniSEED Indexing
~~~~~~~~~~~~~~~~~

.. code-block:: bash

    export NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex

Default: Auto-discovered from PATH

``NOIZ_MSEEDINDEX_EXECUTABLE``
    Path to the mseedindex executable.
    If not set, Noiz will automatically search for ``mseedindex`` in your PATH.
    Since mseedindex is installed as a Python dependency, it is usually auto-discovered without configuration.

    Only set this variable if:

    - You want to use a specific mseedindex binary
    - The auto-discovery fails
    - You have multiple mseedindex installations

Database Backend Selection
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    export NOIZ_DATABASE_BACKEND=postgresql  # or sqlite

Default: ``sqlite``

Noiz supports two database backends:

- ``postgresql``: Production use with PostgreSQL database
- ``sqlite``: Development, testing, or single-user scenarios (default)

Flask Environment
~~~~~~~~~~~~~~~~~

.. code-block:: bash

    export NOIZ_FLASK_ENV=development  # or production

Default: ``development``

Controls Flask application behavior:

- ``development``: Enables debug mode, debug toolbar, verbose logging
- ``production``: Disables debug features for performance and security

Logging Level
~~~~~~~~~~~~~

.. code-block:: bash

    export NOIZ_LOGLEVEL=INFO

Default: ``INFO``

Valid values: ``DEBUG``, ``INFO``, ``WARNING``, ``WARN``, ``ERROR``, ``CRITICAL``

Controls the verbosity of application logging.

Database Backend Options
------------------------

PostgreSQL Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

PostgreSQL is the recommended backend for production deployments.

**Using Connection URI:**

.. code-block:: bash

    export NOIZ_DATABASE_BACKEND=postgresql
    export NOIZ_DATABASE_URL=postgresql+psycopg2://user:password@host:5432/database

**Using Individual Parameters:**

.. code-block:: bash

    export NOIZ_DATABASE_BACKEND=postgresql
    export NOIZ_POSTGRES_HOST=localhost
    export NOIZ_POSTGRES_PORT=5432
    export NOIZ_POSTGRES_USER=noiz
    export NOIZ_POSTGRES_PASSWORD=secure_password
    export NOIZ_POSTGRES_DB=noiz_production

Port must be between 1 and 65535.
All five parameters are required if not using ``NOIZ_DATABASE_URL``.

SQLite Configuration
~~~~~~~~~~~~~~~~~~~~

SQLite is suitable for development, testing, or single-user scenarios.

**Default SQLite database:**

.. code-block:: bash

    export NOIZ_DATABASE_BACKEND=sqlite
    # Uses default: sqlite:///noiz.db

**Custom SQLite database path:**

.. code-block:: bash

    export NOIZ_DATABASE_BACKEND=sqlite
    export NOIZ_DATABASE_URL=sqlite:////absolute/path/to/database.db

Note the four slashes (``////``) for absolute paths in SQLite URIs.

Using .env Files
----------------

For development convenience, Noiz supports ``.env`` files for configuration.
Create a ``.env`` file in your project root:

.. code-block:: bash

    # .env file example
    NOIZ_DATABASE_BACKEND=sqlite
    NOIZ_DATABASE_URL=sqlite:///noiz.db
    NOIZ_PROCESSED_DATA_DIR=/tmp/noiz-data
    NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex
    NOIZ_FLASK_ENV=development
    NOIZ_LOGLEVEL=DEBUG

Load the file before running Noiz:

.. code-block:: bash

    # Load environment variables (bash/zsh)
    set -a
    source .env
    set +a

    # Or use direnv (https://direnv.net/)
    echo "dotenv" > .envrc
    direnv allow

    # Run Noiz
    noiz --help

The ``.env`` file must use valid shell syntax.
Validate syntax with: ``bash -n .env``

Flask Integration
-----------------

Using Noiz in Jupyter Notebooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Import and create the Flask application:

.. code-block:: python

    from noiz.app import create_app

    # Create Flask app (loads configuration automatically)
    app = create_app()

    # Use within app context
    with app.app_context():
        from noiz.database import db
        from noiz.models import Component

        # Query database
        components = Component.query.limit(10).all()

Accessing Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

**From Flask application code:**

.. code-block:: python

    from flask import current_app

    # Access NoizConfig object
    config = current_app.config['NOIZ_CONFIG']
    data_dir = config.processing.processed_data_dir

    # Or use Flask config dict
    data_dir = current_app.config['NOIZ_PROCESSED_DATA_DIR']

**From library code:**

.. code-block:: python

    from noiz.config import get_config

    config = get_config()
    db_uri = config.database.sqlalchemy_database_uri

CI/CD Configuration
-------------------

GitLab CI Example
~~~~~~~~~~~~~~~~~

Configure environment variables in ``.gitlab-ci.yml``:

.. code-block:: yaml

    variables:
      NOIZ_DATABASE_BACKEND: postgresql
      NOIZ_POSTGRES_HOST: postgres
      NOIZ_POSTGRES_PORT: "5432"
      NOIZ_POSTGRES_USER: noiztest
      NOIZ_POSTGRES_PASSWORD: noiztest
      NOIZ_POSTGRES_DB: noiztest
      NOIZ_PROCESSED_DATA_DIR: /tmp/processed
      NOIZ_MSEEDINDEX_EXECUTABLE: mseedindex

    test:
      services:
        - postgres:latest
      script:
        - noiz db upgrade
        - pytest

Docker Example
~~~~~~~~~~~~~~

Pass environment variables to Docker containers:

.. code-block:: bash

    docker run \
      -e NOIZ_DATABASE_BACKEND=postgresql \
      -e NOIZ_DATABASE_URL=postgresql://user:pass@db:5432/noiz \
      -e NOIZ_PROCESSED_DATA_DIR=/data \
      -e NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex \
      noiz:latest

Or use an environment file:

.. code-block:: bash

    docker run --env-file .env noiz:latest

Migration Guide
---------------

Upgrading from Old Environment Variable Names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are upgrading from a previous version of Noiz, update your environment variables to use the ``NOIZ_`` prefix:

.. list-table:: Environment Variable Migration
   :header-rows: 1
   :widths: 40 40 20

   * - Old Variable
     - New Variable
     - Status
   * - ``DATABASE_BACKEND``
     - ``NOIZ_DATABASE_BACKEND``
     - Required
   * - ``DATABASE_URL``
     - ``NOIZ_DATABASE_URL``
     - Required
   * - ``POSTGRES_HOST``
     - ``NOIZ_POSTGRES_HOST``
     - Required
   * - ``POSTGRES_PORT``
     - ``NOIZ_POSTGRES_PORT``
     - Required
   * - ``POSTGRES_USER``
     - ``NOIZ_POSTGRES_USER``
     - Required
   * - ``POSTGRES_PASSWORD``
     - ``NOIZ_POSTGRES_PASSWORD``
     - Required
   * - ``POSTGRES_DB``
     - ``NOIZ_POSTGRES_DB``
     - Required
   * - ``PROCESSED_DATA_DIR``
     - ``NOIZ_PROCESSED_DATA_DIR``
     - Required
   * - ``MSEEDINDEX_EXECUTABLE``
     - ``NOIZ_MSEEDINDEX_EXECUTABLE``
     - Required
   * - ``FLASK_ENV``
     - ``NOIZ_FLASK_ENV``
     - Optional
   * - ``LOGLEVEL``
     - ``NOIZ_LOGLEVEL``
     - Optional

The old variable names are supported temporarily for backward compatibility, but will be removed in a future release.
Update your configuration to use the new names.

Automated Migration
~~~~~~~~~~~~~~~~~~~

Use a shell script to update your ``.env`` file:

.. code-block:: bash

    #!/bin/bash
    # migrate-env.sh - Update environment variable names

    sed -i.bak \
      -e 's/^DATABASE_BACKEND=/NOIZ_DATABASE_BACKEND=/' \
      -e 's/^DATABASE_URL=/NOIZ_DATABASE_URL=/' \
      -e 's/^POSTGRES_HOST=/NOIZ_POSTGRES_HOST=/' \
      -e 's/^POSTGRES_PORT=/NOIZ_POSTGRES_PORT=/' \
      -e 's/^POSTGRES_USER=/NOIZ_POSTGRES_USER=/' \
      -e 's/^POSTGRES_PASSWORD=/NOIZ_POSTGRES_PASSWORD=/' \
      -e 's/^POSTGRES_DB=/NOIZ_POSTGRES_DB=/' \
      -e 's/^PROCESSED_DATA_DIR=/NOIZ_PROCESSED_DATA_DIR=/' \
      -e 's/^MSEEDINDEX_EXECUTABLE=/NOIZ_MSEEDINDEX_EXECUTABLE=/' \
      -e 's/^FLASK_ENV=/NOIZ_FLASK_ENV=/' \
      -e 's/^LOGLEVEL=/NOIZ_LOGLEVEL=/' \
      .env

    echo "Migrated .env file (backup saved as .env.bak)"

Validation and Error Handling
------------------------------

Configuration Validation
~~~~~~~~~~~~~~~~~~~~~~~~

Noiz validates all configuration at startup using pydantic-settings.
Invalid configuration will fail fast with clear error messages:

.. code-block:: text

    ValidationError: 1 validation error for NoizConfig
    postgres_port
      Invalid NOIZ_POSTGRES_PORT: 99999. Must be between 1 and 65535

Common validation checks:

- Database backend must be ``postgresql`` or ``sqlite``
- PostgreSQL requires either URI or all five connection parameters
- Port must be between 1 and 65535
- Processed data directory must not be a file
- Processed data directory must be empty or non-existent
- Mseedindex executable must exist and be executable

Directory Validation Rules
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``NOIZ_PROCESSED_DATA_DIR`` has specific validation rules to prevent data conflicts:

1. **Path exists as file**: Error - must be a directory
2. **Path exists as non-empty directory**: Error - prevents naming conflicts
3. **Path exists as empty directory**: Accepted silently
4. **Path does not exist**: Auto-created with parents if needed (warns if multiple parents created)

Secret Masking
~~~~~~~~~~~~~~

Passwords and sensitive values are automatically masked in logs:

- ``NOIZ_POSTGRES_PASSWORD`` is never logged in plain text
- Database URIs have password components masked
- Configuration repr shows ``******`` for secrets

This prevents accidental password leaks in application logs or error messages.

Troubleshooting
---------------

Configuration Not Found
~~~~~~~~~~~~~~~~~~~~~~~

**Error:** ``ValidationError: field required``

**Solution:** Ensure environment variables are set with the ``NOIZ_`` prefix.

.. code-block:: bash

    # Wrong
    export PROCESSED_DATA_DIR=/data

    # Correct
    export NOIZ_PROCESSED_DATA_DIR=/data

Database Connection Failed
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Error:** ``PostgreSQL backend requires either: NOIZ_DATABASE_URL OR all NOIZ_POSTGRES_* parameters``

**Solution:** Provide complete connection information using one of the two methods.

Check that all five parameters are set if using individual parameters:

.. code-block:: bash

    echo $NOIZ_POSTGRES_HOST
    echo $NOIZ_POSTGRES_PORT
    echo $NOIZ_POSTGRES_USER
    echo $NOIZ_POSTGRES_PASSWORD
    echo $NOIZ_POSTGRES_DB

Directory Validation Failed
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Error:** ``NOIZ_PROCESSED_DATA_DIR points to non-empty directory``

**Solution:** Use an empty directory or specify a new path.

.. code-block:: bash

    # Option 1: Use a new directory
    export NOIZ_PROCESSED_DATA_DIR=/data/processed_new

    # Option 2: Clear existing directory (CAUTION: deletes data)
    rm -rf /data/processed/*

    # Option 3: Use a timestamped directory
    export NOIZ_PROCESSED_DATA_DIR=/data/processed_$(date +%Y%m%d)

Executable Not Found
~~~~~~~~~~~~~~~~~~~~

**Error:** ``NOIZ_MSEEDINDEX_EXECUTABLE not found: mseedindex``

**Solution:** Install mseedindex or provide the full path to the executable.

.. code-block:: bash

    # Option 1: Install mseedindex and ensure it's in PATH
    which mseedindex

    # Option 2: Provide absolute path
    export NOIZ_MSEEDINDEX_EXECUTABLE=/usr/local/bin/mseedindex

    # Option 3: Install via package manager
    apt-get install mseedindex  # Debian/Ubuntu
    brew install mseedindex     # macOS

Best Practices
--------------

1. **Use .env files for development**, environment variables for production
2. **Never commit .env files** containing passwords to version control
3. **Use NOIZ_DATABASE_URL** in production for cleaner configuration
4. **Set NOIZ_FLASK_ENV=production** in production for security and performance
5. **Use separate databases** for development, testing, and production
6. **Validate configuration** before deployment by running: ``noiz --help``
7. **Monitor logs** for configuration warnings at application startup
8. **Use absolute paths** for NOIZ_PROCESSED_DATA_DIR to avoid confusion
9. **Test configuration changes** in non-production environments first
10. **Document custom configurations** specific to your deployment

Further Reading
---------------

- :doc:`/tutorials/create_noiz_project` - Setting up a new Noiz project
- :doc:`/guides/running_system_tests_locally` - Running tests with custom configuration
- :doc:`/development/design_documents/config_system` - Technical design documentation

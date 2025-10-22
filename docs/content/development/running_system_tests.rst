.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

========================
Running System Tests
========================

This document describes how to run Noiz system tests locally.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

System tests validate the complete Noiz workflow from data ingestion through processing.
They require PostgreSQL with HSTORE extension and a compiled mseedindex with PostgreSQL support.

Prerequisites
=============

PostgreSQL Database
-------------------

Start PostgreSQL with Docker::

    docker run -d \
        --name noiz-postgres \
        -e POSTGRES_USER=noiz \
        -e POSTGRES_PASSWORD=noiz \
        -e POSTGRES_DB=noiz \
        -p 5432:5432 \
        noiz-postgres:local

Or use the ``noiz-postgres:local`` image from ``docker/postgres-image/``.

Docker Image with Ancient mseedindex
-------------------------------------

System tests currently require ancient mseedindex (v2.7.1) compiled with PostgreSQL support.
This version is included in the CI Docker image.

Build the test image::

    docker build --platform linux/amd64 -f docker/noiz-image/Dockerfile -t noiz:test-local .

This builds an image containing:

- Python 3.10
- Noiz source code
- mseedindex v2.7.1 compiled with PostgreSQL flags
- All dependencies

.. note::
   The PyPI version of mseedindex (3.0.8) is NOT compiled with PostgreSQL support by default.
   We plan to move to JSON-based mseedindex output to eliminate this dependency.
   See: ``specs/002-noiz-modernization/research.md`` (Question 6)

Running Tests
=============

Method 1: Docker Container (Recommended)
-----------------------------------------

This runs tests in the exact CI environment::

    # 1. Clear and prepare database
    docker exec noiz-postgres psql -U noiz -d noiz -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE EXTENSION IF NOT EXISTS hstore;"

    # 2. Run system tests in container
    docker run --rm \
      --link noiz-postgres:postgres \
      -e POSTGRES_HOST=postgres \
      -e POSTGRES_PORT=5432 \
      -e POSTGRES_USER=noiz \
      -e POSTGRES_PASSWORD=noiz \
      -e POSTGRES_DB=noiz \
      -e PROCESSED_DATA_DIR=/tmp/processed \
      -e MSEEDINDEX_EXECUTABLE=/mseedindex/mseedindex \
      -e FLASK_APP=noiz.app:create_app \
      noiz:test-local \
      bash -c 'mkdir -p /tmp/processed && cd /noiz && uv run flask db upgrade && uv run pytest --runcli tests/system_tests/cli/test_data_ingestion.py -v'

Method 2: Using just (CI-like)
-------------------------------

The ``just run_system_tests`` command automates database creation::

    just run_system_tests

This creates:

- Fresh PostgreSQL database with timestamp
- HSTORE extension enabled
- Runs migrations
- Executes system tests

.. warning::
   This requires local PostgreSQL installation and mseedindex compiled with PostgreSQL support.
   Most developers should use Method 1 (Docker) instead.

Understanding Test Results
==========================

Expected Failures
-----------------

Some tests are marked ``xfail`` (expected to fail):

- ``test_add_soh_files`` - SOH functionality incomplete
- Plotting tests with missing data

Known Issues (Pre-existing)
----------------------------

The following failures are **not** related to ULID migration work:

- ``test_run_beamforming_extract_avg_abspower_save_avg_abspower`` - Empty array plotting issue
- ``test_plot_beamforming_freq_slowness`` - matplotlib dimension mismatch
- ``test_plot_beamforming_freq_velocity`` - matplotlib dimension mismatch

These are pre-existing beamforming plotting bugs in the visualization code.

Success Criteria
----------------

**System tests validate ULID migration if**:

- ✅ Data ingestion tests pass (inventory, seismic data, timespans)
- ✅ Configuration tests pass (all param types)
- ✅ Processing tests pass (datachunks, cross-correlations, stacking, PPSD)
- ✅ No foreign key constraint violations
- ✅ Parallel processing completes without data loss

**Current status** (as of 2025-10-16 with Phase 4 Parts A+B):

- **36 passed** - All critical workflows working
- **3 failed** - Pre-existing plotting bugs (unrelated to ULID)
- **6 xfailed** - Known incomplete features

ULID Migration Status
=====================

Completed
---------

**Phase 2: Foundation** (branch: ``phase2-ulid-foundation``)

- Database backend abstraction (SQLite/PostgreSQL)
- ULIDMixin implementation
- SQLite PRAGMA configuration

**Phase 4 Part A: Models** (branch: ``phase4-ulid-model-migration``)

- Added ULID to 6 file entities
- Added ULID + file_ulid FK to 7 result entities

**Phase 4 Part B: Migrations** (branch: ``phase4-ulid-model-migration``)

- Migration 1: File entities with ULID
- Migration 2: Result entities with ULID + file_ulid FK
- Tested: upgrade/downgrade both work

**System Test Validation**: ✅ PASSING (36/39 non-xfail tests)

Remaining Work
--------------

Phase 4 Part C-F still needed (30 tasks):

- Part C: Update worker functions to generate ULIDs upfront
- Part D: Update bulk insert logic to use file_ulid
- Part E: Implement resume detection via ULID queries
- Part F: Add integration tests for parallel processing + resume

See: ``specs/002-noiz-modernization/tasks.md`` for complete task list.

Troubleshooting
===============

"Database does not exist"
-------------------------

Create the database first::

    docker exec noiz-postgres psql -U noiz -c "CREATE DATABASE noiz;"

"type hstore does not exist"
----------------------------

Enable the extension::

    docker exec noiz-postgres psql -U noiz -d noiz -c "CREATE EXTENSION hstore;"

"mseedindex: command not found" in Docker
------------------------------------------

Verify the image was built with mseedindex::

    docker run --rm noiz:test-local ls -la /mseedindex/mseedindex

Should show the compiled binary.

Tests fail with "assert X == Y" (wrong counts)
-----------------------------------------------

Database has stale data from previous runs.
Clear the schema::

    docker exec noiz-postgres psql -U noiz -d noiz -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public; CREATE EXTENSION hstore;"

Future: JSON-based mseedindex
==============================

The current approach requires:

- Ancient mseedindex (v2.7.1)
- Compiled with PostgreSQL support
- Direct database writes with HSTORE/ARRAY types

**Planned improvement**:

Use modern mseedindex (3.0.8+) from PyPI with JSON output::

    # Future approach
    pip install noiz  # includes mseedindex>=3.0.8
    mseedindex -json output.json seismic_data/*.mseed
    noiz data import-index output.json

This will:

- Eliminate Docker compilation
- Support SQLite natively
- Decouple from mseedindex schema
- Simplify installation dramatically

See: ``specs/002-noiz-modernization/research.md`` for technical details.

References
==========

- System test source: ``tests/system_tests/cli/test_data_ingestion.py``
- CI configuration: ``.gitlab-ci.yml``
- Docker images: ``docker/noiz-image/Dockerfile``
- mseedindex documentation: https://github.com/EarthScope/mseedindex

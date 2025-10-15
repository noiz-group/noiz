.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

====================
Refactoring Roadmap
====================

This document tracks the major refactoring efforts needed to modernize the Noiz codebase,
fix critical bugs, and improve maintainability.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

**Total Estimated Time**: 13-17 weeks (3-4 months)

Quick Wins (Can do in first week)
----------------------------------

* Phase 0.1: Fix scipy version (30 minutes)
* Phase 0.6.1: Test publish to PyPI (2-3 hours)
* Phase 0.5: Vendor migrations (1 hour)

Phase 0: Critical Infrastructure Fixes (4-5 weeks)
===================================================

Dependencies & Python Compatibility
------------------------------------

0.1: Fix scipy version typo (CRITICAL - BLOCKS INSTALLATION)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Current**: ``scipy==1.15.3`` (doesn't exist!)
* **Fix**: ``scipy>=1.11.0,<2.0``
* **File**: ``pyproject.toml:35``

0.2: Upgrade all dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Update the following dependencies:

* ``dask[complete]==2021.11.2`` → ``dask[complete]>=2024.1.0`` (3 years old!)
* ``flask==2.0.2`` → ``flask>=3.0.0`` (security vulnerabilities)
* ``pandas==1.3.4`` → ``pandas>=2.0.0``
* ``matplotlib==3.5.0`` → ``matplotlib>=3.8.0``
* ``numpy==1.23.5`` → ``numpy>=1.26.0``
* ``pydantic~=1.8`` → ``pydantic>=2.0.0`` (breaking changes, requires migration)

**File**: ``pyproject.toml:20-42``

0.3: Add Python 3.11, 3.12, 3.13 compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Current**: ``requires-python = ">= 3.10, < 3.11"`` (locked to 3.10 only!)
* **Change to**: ``requires-python = ">= 3.10, < 3.14"``
* **Test on all versions in CI**
* **File**: ``pyproject.toml:19``

Database Layer
--------------

0.4: Migrate to SQLAlchemy 2.0 (MAJOR EFFORT)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Current**: ``sqlalchemy >=1.4.0, <2.0`` (explicitly blocked)
* **CI already runs with**: ``SQLALCHEMY_WARN_20=1``

**Changes needed**:

* Replace ``Query`` with ``select()`` statements
* Update ``declarative_base()`` usage
* Fix ``session.query()`` → ``session.execute(select())``
* Update all relationship configurations
* Fix lazy loading patterns
* Also upgrade: ``flask-sqlalchemy==2.5.1`` → ``flask-sqlalchemy>=3.1.0``

**Files**: All ``src/noiz/api/*.py``, ``src/noiz/models/*.py``

**Reference**: https://docs.sqlalchemy.org/en/20/changelog/migration_20.html

0.9: Add proper session scoping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Problem**: Raw ``db.session`` usage everywhere, no scoped sessions
* **Fix**: Use ``scoped_session`` for thread-safety
* **Add context managers for session lifecycle**
* **Files**: ``src/noiz/database.py``, all API modules

0.10: Add DB connection pooling configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Expose pool size, timeout, recycle settings
* Add pool pre-ping for connection health checks
* **File**: ``src/noiz/settings.py``, ``src/noiz/database.py``

Code Quality
------------

0.8: Fix bare exception catches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

93 FIXME/TODO comments found.

**Files with** ``except Exception:``:

* ``src/noiz/processing/beamforming.py``
* ``src/noiz/processing/datachunk.py``
* ``src/noiz/processing/timeseries.py``
* ``src/noiz/api/crosscorrelations.py``

**Tasks**:

* Replace with specific exception types
* Add proper error context and logging

0.11: Add retry logic for transient failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Database deadlocks during parallel processing
* Network failures during file I/O
* Use ``tenacity`` library
* **Files**: ``src/noiz/api/helpers.py``

Observability
-------------

0.12: Add structured logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Current**: Plain loguru text logs
* **Add**: JSON structured logging with context
* **Include**: trace_id, operation, duration, errors
* **Consider**: OpenTelemetry integration
* **Files**: Throughout codebase

Packaging & Release (HIGH PRIORITY!)
-------------------------------------

0.5: Vendor migrations in package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Current**: ``migrations/`` not included in package distribution
* **Fix**: Add to ``pyproject.toml`` includes
* **Test**: Verify migrations work when installed from PyPI
* **File**: ``pyproject.toml:109-112``

0.6: Setup proper packaging for PyPI release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Current**: Using experimental ``uv_build`` backend
* **Consider**: Switch to ``hatchling`` or ``setuptools``
* **Add**: Long description from README
* **Add**: Proper classifiers and keywords
* **Test**: Build wheel and sdist locally
* **File**: ``pyproject.toml``

**Action Items**:

1. Fix ``pyproject.toml`` build system (use stable backend)
2. Add ``migrations/`` to package includes
3. Test local build: ``python -m build``
4. Test local install: ``pip install dist/noiz-*.whl``

0.6.1: TEST PUBLISH TO PyPI (DO THIS EARLY!)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Create PyPI account if needed
* Get API token from PyPI
* Do a test publish (can yank immediately after)

**Steps**:

.. code-block:: bash

    # Build
    python -m build

    # Test upload to TestPyPI first
    python -m twine upload --repository testpypi dist/*

    # Test install from TestPyPI
    pip install --index-url https://test.pypi.org/simple/ noiz

    # If works, publish to real PyPI
    python -m twine upload dist/*

    # Can yank right after to test the process
    # (Yanked versions can't be re-uploaded with same version)

**Why do this early?**

* Validates packaging is correct
* Identifies issues before major refactoring
* Tests the complete distribution pipeline
* Can iterate quickly on packaging fixes

0.7: Add release automation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Manual version bumping is error-prone
* **Add**: ``bump2version`` or ``commitizen`` for semantic versioning
* **Add**: Automated changelog generation
* **Add**: CI job to publish to PyPI on git tags
* **Files**: ``.gitlab-ci.yml``, add ``.bumpversion.cfg``
* **Bonus**: Add GitHub/GitLab release notes generation

Security
--------

0.13: Remove hardcoded secrets from environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Found in shell environment**:

* ``GITLAB_PASSWORD=glpat-xM8CxWRT-fFsGF3DG_DA``
* ``GITLAB_TOKEN=glpat-_K9FQUVjTfR-FApEyPFB``
* ``JIRA_API_TOKEN=ATATT3xFfGF05...``

These should be in a secrets manager, not environment variables.

**Add**: Warning if sensitive vars detected in environment

CI/CD & Docker
--------------

0.14: Add multi-arch Docker builds (arm64)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Current**: Only ``--platform linux/amd64``
* **Add**: ARM64 for Apple Silicon and AWS Graviton
* **File**: ``.gitlab-ci.yml:61,68,77,86``

0.15: Fix parallel system tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Current**: ``allow_failure: true`` for parallel tests (!)
* This hides real failures
* **Fix**: Make parallel tests pass reliably
* **File**: ``.gitlab-ci.yml:138``

Phase 1: UUID Migration - Fix Multiprocessing (3-4 weeks)
==========================================================

Problem Statement
-----------------

**Critical Bug**: Worker processes create DB objects without IDs, causing foreign key relationship
failures during bulk insert.

**Root Cause**:

.. code-block:: python

    # In worker process:
    ccf_file = CrosscorrelationCartesianFile(filepath=str(filepath))  # No ID yet!
    xcorr = CrosscorrelationCartesian(..., file=ccf_file)  # References file without ID

    # In coordinator:
    files = [x.file for x in results]
    bulk_add_objects(files)  # Files get IDs from DB here
    bulk_add_objects(results)  # But results still reference old file objects!

Solution: UUID Primary Keys
----------------------------

Generate UUIDs in Python **before** DB interaction. Workers create objects with stable,
unique IDs immediately.

Tasks
-----

1.1: Add UUID columns to all models (File models first)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Add ``uuid = db.Column(db.UUID, unique=True, nullable=False, default=uuid.uuid4)``
* Keep existing integer ``id`` columns (parallel operation during transition)

**Models to update** (15+ files):

* ``DatachunkFile``, ``CrosscorrelationCartesianFile``, ``CrosscorrelationCylindricalFile``
* ``ProcessedDatachunkFile``, ``BeamformingFile``, ``PPSDFile``, ``EventDetectionFile``
* ``Datachunk``, ``CrosscorrelationCartesian``, ``CrosscorrelationCylindrical``
* ``ProcessedDatachunk``, ``BeamformingResult``, ``PPSDResult``
* ``DatachunkStats``, ``QCOneResults``, ``QCTwoResults``, ``CCFStack``

**Files**: ``src/noiz/models/*.py``

1.2: Create database migration for UUID columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Add PostgreSQL UUID extension
* Add UUID columns with ``nullable=True`` initially
* Populate UUIDs for existing rows: ``UPDATE table SET uuid = uuid_generate_v4()``
* Make UUID columns ``NOT NULL``
* Add unique constraints on UUID columns
* **File**: New migration in ``migrations/versions/``

1.3: Update models to auto-generate UUIDs in __init__
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import uuid as uuid_lib

    class CrosscorrelationCartesianFile(db.Model):
        id = db.Column(db.BigInteger, primary_key=True)  # Keep for now
        uuid = db.Column(db.UUID, unique=True, nullable=False)

        def __init__(self, **kwargs):
            if 'uuid' not in kwargs:
                kwargs['uuid'] = uuid_lib.uuid4()
            super().__init__(**kwargs)

**Files**: All model files

1.4: Update all FK relationships to reference UUIDs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Add ``file_uuid`` columns alongside existing ``file_id`` FK columns
* Add foreign key constraints: ``db.ForeignKey('table.uuid')``
* Update relationship definitions
* **Files**: All models with relationships

1.5: Update bulk insert/upsert logic to use UUIDs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Ensure UUID is set before ``session.add_all()``
* Update upsert conflict resolution to use UUID
* **Files**: ``src/noiz/api/helpers.py``

1.6: Update all upsert commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Modify ``_prepare_upsert_command_*`` functions
* Change ``on_conflict_do_update`` to use UUID constraints

**Files**:

* ``src/noiz/api/crosscorrelations.py:148,829``
* ``src/noiz/api/datachunk.py:390,590,779``
* ``src/noiz/api/beamforming.py``
* ``src/noiz/api/ppsd.py``
* ``src/noiz/api/qc.py``
* ``src/noiz/api/stacking.py``

1.7: Test UUID implementation with small dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Run full pipeline with ``--parallel`` and ``--no_parallel``
* Verify all inserts succeed
* Check foreign key integrity

1.8: Migrate to UUID as primary key (deprecate integer ID)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Switch code to use ``uuid`` as primary key
* Eventually remove integer ``id`` columns (breaking change - needs major version bump)

Phase 2: Fix Worker Code for Multiprocessing (1 week)
======================================================

2.1: Update worker functions to generate UUIDs upfront
-------------------------------------------------------

.. code-block:: python

    def _crosscorrelate_for_timespan(...):
        import uuid as uuid_lib

        # Generate UUIDs before creating objects
        file_uuid = uuid_lib.uuid4()
        result_uuid = uuid_lib.uuid4()

        ccf_file = CrosscorrelationCartesianFile(
            uuid=file_uuid,
            filepath=str(filepath)
        )

        xcorr = CrosscorrelationCartesian(
            uuid=result_uuid,
            file_uuid=file_uuid,  # Use UUID reference
            ...
        )

**Files**:

* ``src/noiz/api/crosscorrelations.py:436-514,800-826``
* ``src/noiz/processing/datachunk.py:720-844``
* ``src/noiz/api/beamforming.py``
* ``src/noiz/api/ppsd.py``
* Other worker functions

2.2: Test multiprocessing with UUID-based writes
-------------------------------------------------

* Run parallel system tests
* Increase batch sizes to stress test
* Monitor for missing records

2.3: Verify all results are written to DB correctly
----------------------------------------------------

* Count expected vs actual records
* Check for orphaned files
* Validate FK integrity

Phase 3: SQLite Support (2 weeks)
==================================

3.1: Abstract PostgreSQL-specific SQL
--------------------------------------

* Replace ``insert().on_conflict_do_update()`` with database-agnostic approach
* Use SQLAlchemy 2.0 ``Insert`` API properly
* Consider using ``merge()`` for upserts
* **Files**: All API modules with upsert commands

3.2: Create SQLite-compatible migrations
-----------------------------------------

* UUID handling: SQLite uses CHAR(36) or BLOB for UUIDs
* Foreign keys: Ensure ``PRAGMA foreign_keys = ON``
* Indexes: Verify index creation syntax
* **Files**: New migrations, update migration tests

3.3: Add database backend configuration
----------------------------------------

.. code-block:: python

    # settings.py
    DATABASE_BACKEND = env.str("DATABASE_BACKEND", "postgresql")  # or "sqlite"

    if DATABASE_BACKEND == "sqlite":
        DATABASE_URL = env.str("DATABASE_URL", "sqlite:///noiz.db")

**File**: ``src/noiz/settings.py``

3.4: Test full pipeline with SQLite backend
--------------------------------------------

* Run all tests with SQLite
* Verify performance is acceptable
* Document limitations (if any)

Phase 4: Multiple Execution Backends (2-3 weeks)
=================================================

Goal: Support three execution modes
------------------------------------

**Execution Modes**:

1. **Sequential** - Single-threaded, for debugging and small datasets
2. **Multiprocessing** - Local parallel execution with multiprocessing.Pool
3. **Dask** - Distributed execution for future cluster deployments

**Design Principle**: Make execution backend configurable. Dask becomes optional but remains
supported for future distributed deployments.

4.1: Create execution backend abstraction
------------------------------------------

.. code-block:: python

    # In api/execution_backends.py

    from abc import ABC, abstractmethod
    from typing import List, Callable, Any
    from enum import Enum

    class ExecutionMode(Enum):
        SEQUENTIAL = "sequential"
        MULTIPROCESSING = "multiprocessing"
        DASK = "dask"

    class ExecutionBackend(ABC):
        """Abstract base class for execution backends."""

        @abstractmethod
        def map(self, func: Callable, inputs: List[Any]) -> List[Any]:
            """Execute function on inputs and return results."""
            pass

        @abstractmethod
        def shutdown(self) -> None:
            """Clean up resources."""
            pass


    class SequentialBackend(ExecutionBackend):
        """Sequential execution (single-threaded)."""

        def map(self, func: Callable, inputs: List[Any]) -> List[Any]:
            return [func(inp) for inp in inputs]

        def shutdown(self) -> None:
            pass


    class MultiprocessingBackend(ExecutionBackend):
        """Parallel execution using multiprocessing.Pool."""

        def __init__(self, workers: int = 4):
            from multiprocessing import Pool
            self.pool = Pool(processes=workers)

        def map(self, func: Callable, inputs: List[Any]) -> List[Any]:
            return self.pool.map(func, inputs)

        def shutdown(self) -> None:
            self.pool.close()
            self.pool.join()


    class DaskBackend(ExecutionBackend):
        """Distributed execution using Dask."""

        def __init__(self, scheduler_address: str = None):
            from dask.distributed import Client
            if scheduler_address:
                self.client = Client(scheduler_address)
            else:
                # Local cluster
                self.client = Client()

        def map(self, func: Callable, inputs: List[Any]) -> List[Any]:
            futures = self.client.map(func, inputs)
            return self.client.gather(futures)

        def shutdown(self) -> None:
            self.client.close()


    def get_execution_backend(
        mode: ExecutionMode,
        workers: int = 4,
        scheduler_address: str = None
    ) -> ExecutionBackend:
        """Factory function to get appropriate backend."""
        if mode == ExecutionMode.SEQUENTIAL:
            return SequentialBackend()
        elif mode == ExecutionMode.MULTIPROCESSING:
            return MultiprocessingBackend(workers=workers)
        elif mode == ExecutionMode.DASK:
            return DaskBackend(scheduler_address=scheduler_address)
        else:
            raise ValueError(f"Unknown execution mode: {mode}")

**Files**: New file ``src/noiz/api/execution_backends.py``

4.2: Update parallel execution code
------------------------------------

.. code-block:: python

    # In api/helpers.py

    def _run_calculate_and_upsert(
        inputs,
        calculation_task,
        upserter_callable,
        batch_size=5000,
        execution_mode: ExecutionMode = ExecutionMode.MULTIPROCESSING,
        workers: int = 4,
        dask_scheduler: str = None,
    ):
        """
        Execute calculations with configurable backend.

        Args:
            execution_mode: Sequential, multiprocessing, or dask
            workers: Number of workers (for multiprocessing/dask)
            dask_scheduler: Dask scheduler address (for dask mode)
        """
        backend = get_execution_backend(
            mode=execution_mode,
            workers=workers,
            scheduler_address=dask_scheduler
        )

        try:
            for batch in more_itertools.chunked(inputs, batch_size):
                results = backend.map(calculation_task, batch)
                # Flatten results
                flat_results = list(more_itertools.flatten(results))
                # Insert to DB
                _handle_file_and_result_inserts(flat_results, ...)
        finally:
            backend.shutdown()

**Removes**:

* Dask ``client.restart()`` memory workarounds
* ``sleep(2)`` hacks
* Hardcoded Dask-specific code

**Files**: ``src/noiz/api/helpers.py:219-323``

4.3: Add CLI flags for execution mode
--------------------------------------

.. code-block:: python

    # In cli.py

    @click.option(
        "--execution-mode",
        type=click.Choice(["sequential", "multiprocessing", "dask"]),
        default="multiprocessing",
        help="Execution backend"
    )
    @click.option(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers"
    )
    @click.option(
        "--dask-scheduler",
        type=str,
        default=None,
        help="Dask scheduler address (optional, for dask mode)"
    )
    def run_crosscorrelations(..., execution_mode, workers, dask_scheduler):
        """Run cross-correlations with configurable backend."""
        ...

**Usage examples**:

.. code-block:: bash

    # Local multiprocessing (default)
    noiz processing run_crosscorrelations --execution-mode multiprocessing --workers 8

    # Sequential (debugging)
    noiz processing run_crosscorrelations --execution-mode sequential

    # Dask with local cluster
    noiz processing run_crosscorrelations --execution-mode dask --workers 8

    # Dask with remote scheduler
    noiz processing run_crosscorrelations --execution-mode dask \
        --dask-scheduler tcp://scheduler.example.com:8786

4.4: Make Dask an optional dependency
--------------------------------------

.. code-block:: toml

    # In pyproject.toml

    [project]
    dependencies = [
        # Core dependencies (no dask)
        ...
    ]

    [project.optional-dependencies]
    dask = [
        "dask[complete]>=2024.1.0",
        "distributed>=2024.1.0",
    ]

    # Install with: pip install noiz[dask]
    # Or with uv: uv sync --extra dask

**Benefits**:

* Lighter default installation
* Dask available when needed
* Users choose based on their needs

4.5: Update documentation
--------------------------

**Document execution modes**:

+------------------+---------------+-------------------+------------------------+
| Feature          | Sequential    | Multiprocessing   | Dask                   |
+==================+===============+===================+========================+
| Parallelism      | None          | Local only        | Local or distributed   |
+------------------+---------------+-------------------+------------------------+
| Use Case         | Debugging     | Local processing  | Large-scale processing |
+------------------+---------------+-------------------+------------------------+
| Setup Complexity | Minimal       | Minimal           | Requires scheduler     |
+------------------+---------------+-------------------+------------------------+
| Scalability      | Poor          | Limited (1 node)  | Excellent              |
+------------------+---------------+-------------------+------------------------+
| Dependencies     | None extra    | None extra        | Dask (optional)        |
+------------------+---------------+-------------------+------------------------+
| Memory Usage     | Low           | Medium            | Configurable           |
+------------------+---------------+-------------------+------------------------+

**Update**:

* CLI documentation with execution mode flags
* Performance comparison guide
* When to use each mode

Phase 5: Type Hints & Code Quality (2-3 weeks)
===============================================

5.1: Add return type hints to processing functions
---------------------------------------------------

* All functions in ``src/noiz/processing/*.py`` (23 files)
* Use proper types from ``typing`` and ``typing_extensions``
* **Files**: ``src/noiz/processing/*.py``

5.2: Fix mypy type errors in api/ modules
------------------------------------------

* Remove ``# mypy: ignore-errors`` from ``src/noiz/cli.py``
* Fix type errors throughout API layer
* Aim for ``mypy --strict`` compliance
* **Files**: ``src/noiz/api/*.py``, ``src/noiz/cli.py:1``

5.3: Update type aliases for UUID-based types
----------------------------------------------

* Change ID types from ``int`` to ``UUID`` in TypedDict definitions
* Update function signatures
* **File**: ``src/noiz/models/type_aliases.py``

Additional Issues to Address
=============================

Architecture
------------

* Remove global state (``PROCESSED_DATA_DIR`` as global variable)
* Add dependency injection for testability
* Decouple layers (api → processing → models)
* Add async/await support for I/O operations

Error Handling
--------------

* Add circuit breakers for external dependencies
* Improve error messages with context
* Add error tracking/reporting integration

Testing
-------

* Increase test coverage (current unknown)
* Add integration tests for parallel processing
* Add database migration tests
* Mock external dependencies properly

Performance
-----------

* Profile and optimize hot paths
* Add caching where appropriate
* Optimize database queries (reduce N+1 queries)
* Add connection pooling monitoring

Documentation
-------------

* Update all docstrings to use proper format
* Add architecture decision records (ADRs)
* Document migration guides for major changes
* Add troubleshooting guide

File Management
---------------

* Validate ``PROCESSED_DATA_DIR`` exists and is writable at startup
* Add file cleanup on errors
* Handle race conditions with concurrent file writes
* Add disk space monitoring

Known Workarounds to Remove
============================

Search for these in codebase:

* ``# FIXME remove this workaround when database will be upgraded`` (models/crosscorrelation.py:81, datachunk.py:86)
* ``db.session.expunge_all()`` called everywhere (fighting the ORM)
* Manual validation of ObsPy Stream objects

Priority Order
==============

1. **Phase 0.1** - Fix scipy version (IMMEDIATE - blocks installation)
2. **Phase 0.5-0.7** - PyPI packaging & test publish (DISTRIBUTION - can yank after testing)
3. **Phase 0.4** - Migrate to SQLAlchemy 2.0 (FOUNDATION for everything)
4. **Phase 1** - UUID migration (FIXES critical multiprocessing bug)
5. **Phase 2** - Fix worker code (COMPLETES multiprocessing fix)
6. **Phase 0.2-0.3** - Upgrade deps and Python versions (MAINTAINABILITY)
7. **Phase 4** - Multiple execution backends (FLEXIBILITY - keeps Dask as option)
8. **Phase 3** - SQLite support (USER EXPERIENCE)
9. **Phase 5** - Type hints (CODE QUALITY)
10. **Phase 0.8-0.15** - Everything else (POLISH)

Notes
=====

* Many changes are **breaking** - will require major version bump (v1.0.0)
* SQLAlchemy 2.0 migration is most complex - budget extra time
* UUID migration can be done incrementally with dual-key approach
* Test thoroughly at each phase before proceeding
* Consider feature flags for gradual rollout

See Also
========

* :doc:`architecture` - Code quality and architecture analysis
* :doc:`s3_storage` - S3-compatible storage implementation plan
* :doc:`../coding_standards` - Coding conventions
* :doc:`../type_checking` - Type checking requirements

.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

===========
Development
===========

This section contains information and guidance for developers working on Noiz,
including design decisions, refactoring plans, and development standards.

.. toctree::
    :maxdepth: 2
    :caption: Getting Started

    environment_setup
    coding_standards
    type_checking
    pre_commit_hooks

.. toctree::
    :maxdepth: 2
    :caption: Design Decisions

    design_decisions/index

.. toctree::
    :maxdepth: 1
    :caption: Reference

    deprecations

Overview
========

Noiz is an ambient seismic noise processing application built with:

* **Python** 3.10+ (migrating to 3.11-3.13 support)
* **Flask** + Flask-SQLAlchemy for web framework
* **PostgreSQL** with PostGIS for database (SQLite support planned)
* **ObsPy** for seismic data processing
* **Dask** for parallel processing (planned removal)

Development Standards
=====================

Type Hints
----------

**Type hints are mandatory** for all new code:

* All function signatures must include type hints
* Use ``typing`` module for complex types
* Use ``Optional[T]`` for nullable parameters
* Document return types explicitly

See :doc:`type_checking` for full details.

Documentation
-------------

All documentation must be in **reStructuredText (RST)** format:

* Public API: Sphinx docstrings in RST
* Design decisions: ``docs/content/development/design_decisions/``
* Development guides: ``docs/content/development/``

**Important**: Do not use emojis in any documentation.

Code Quality
------------

Before committing:

.. code-block:: bash

    # Format code
    ruff format .

    # Check style
    ruff check .

    # Check types
    mypy src/noiz

    # Run tests
    pytest

Pre-commit hooks will automatically run these checks. See :doc:`pre_commit_hooks`.

Quick Start for New Contributors
=================================

1. **Setup Environment**

   Follow :doc:`environment_setup` to install dependencies and configure your development environment.

2. **Read Coding Standards**

   Review :doc:`coding_standards` to understand our conventions and requirements.

3. **Configure Pre-commit Hooks**

   Install pre-commit hooks as described in :doc:`pre_commit_hooks`.

4. **Understand Architecture**

   Read :doc:`design_decisions/architecture` to understand the codebase structure.

5. **Find an Issue**

   Look for issues tagged "good first issue" or "help wanted" in the issue tracker.

6. **Submit Pull Request**

   Follow the contribution workflow and code review process.

Development Workflow
====================

Creating a Feature
------------------

1. **Create a feature branch**

   .. code-block:: bash

       git checkout -b feature/my-feature

2. **Make changes with type hints**

   .. code-block:: python

       from typing import List
       from noiz.models import Datachunk

       def process_datachunks(chunks: List[Datachunk]) -> List[ProcessedDatachunk]:
           """Process a list of datachunks."""
           ...

3. **Write tests**

   .. code-block:: python

       def test_process_datachunks() -> None:
           """Test datachunk processing."""
           chunks = [create_test_datachunk()]
           result = process_datachunks(chunks)
           assert len(result) == 1

4. **Run checks**

   .. code-block:: bash

       # Pre-commit will run automatically
       git commit -m "feat(processing): add datachunk processor"

5. **Push and create merge request**

   .. code-block:: bash

       git push origin feature/my-feature
       # Create MR in GitLab

Fixing a Bug
------------

1. **Write a failing test** that reproduces the bug
2. **Fix the bug** in the code
3. **Verify the test passes**
4. **Submit pull request** with test and fix

Code Review Process
-------------------

All code must be reviewed before merging:

* **Reviewers check**:
    * Type hints are present
    * Tests are included
    * Documentation is updated
    * Code follows standards
    * No security issues

* **CI must pass**:
    * All tests pass
    * Type checking passes
    * Linting passes
    * Documentation builds

Project Structure
=================

Understanding the codebase:

.. code-block:: text

    noiz/
    ├── src/noiz/              # Source code
    │   ├── models/            # SQLAlchemy ORM models
    │   ├── api/               # High-level API (orchestration)
    │   ├── processing/        # Core processing algorithms
    │   ├── cli.py             # Click-based CLI
    │   ├── app.py             # Flask application
    │   └── database.py        # Database setup
    ├── tests/                 # Test suite
    ├── docs/                  # Documentation (Sphinx)
    ├── migrations/            # Database migrations
    └── docker/                # Docker configurations

See :doc:`design_decisions/architecture` for detailed architecture.

Key Concepts
============

Processing Pipeline
-------------------

Noiz processes seismic data through a pipeline:

1. **Data Ingestion** - Import raw seismic data
2. **Timespan Generation** - Create time windows
3. **Datachunk Preparation** - Segment raw data
4. **QC One** - Quality control on individual traces
5. **Datachunk Processing** - Preprocessing/filtering
6. **Cross-correlation** - Correlate trace pairs
7. **QC Two** - Quality control on correlations
8. **Stacking** - Time-domain stacking
9. **Analysis** - Beamforming, PPSD calculations

Separation of Concerns
----------------------

The codebase follows a layered architecture:

* **CLI Layer** (``cli.py``) - User interface
* **API Layer** (``api/``) - Orchestration, DB queries
* **Processing Layer** (``processing/``) - Pure computation
* **Models Layer** (``models/``) - Data definitions

**Rule**: Processing layer must not import from API layer.

Configuration System
--------------------

Processing parameters are stored as TOML files:

* Examples in ``config_examples/`` directory
* Ingested into database via CLI
* Each stage has its own params type
* Configurations are versioned

Active Development Work
=======================

Ongoing Refactoring
-------------------

See :doc:`design_decisions/refactoring_roadmap` for the complete refactoring plan.

**Priority items:**

1. Fix scipy version (CRITICAL - blocks installation)
2. PyPI packaging and test publish
3. Migrate to SQLAlchemy 2.0
4. UUID migration (fixes multiprocessing bug)
5. Upgrade Python and dependencies

Architecture Improvements
-------------------------

See :doc:`design_decisions/architecture` for detailed architectural issues.

**Focus areas:**

* File tracking (11+ untracked file writes)
* QC system renaming (QCOne to DatachunkQC, etc.)
* Refactor large files (beamforming.py is 1,439 lines)
* Standardize error handling
* Add database connection pooling

S3 Storage Support
------------------

See :doc:`design_decisions/s3_storage` for S3-compatible filesystem support.

**Goals:**

* Support S3, MinIO, and local storage
* Use fsspec for abstraction
* Transparent caching for remote files
* Cloud-ready deployment

Contributing Guidelines
=======================

Code Requirements
-----------------

**All contributions must have:**

* Type hints on all functions
* RST docstrings for public APIs
* Unit tests for new functionality
* Updated documentation
* No emojis in documentation or code

**Before submitting:**

* Run pre-commit hooks
* Verify type checking passes
* Ensure tests pass
* Build documentation locally

Communication
-------------

* **Questions**: Use GitLab issues or discussion board
* **Bugs**: Create issue with reproducible example
* **Features**: Discuss in issue before implementing
* **Security**: Report privately to maintainers

License
-------

All contributions must be licensed under CECILL-B license.
Add license header to new files:

.. code-block:: python

    # SPDX-License-Identifier: CECILL-B
    # Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
    # Copyright © 2019-2023 Contributors to the Noiz project.

Resources
=========

Documentation
-------------

* `User guides <../guides/index.html>`_
* `API reference <../autoapi/index.html>`_
* `Tutorials <../tutorials/index.html>`_

External Links
--------------

* **ObsPy**: https://docs.obspy.org/
* **SQLAlchemy**: https://docs.sqlalchemy.org/
* **Flask**: https://flask.palletsprojects.com/
* **Ruff**: https://docs.astral.sh/ruff/
* **Mypy**: https://mypy.readthedocs.io/

Getting Help
============

If you're stuck:

1. Check existing documentation
2. Search closed issues
3. Ask in the discussion board
4. Open a new issue with:

   * Clear description
   * Steps to reproduce
   * Expected vs actual behavior
   * Environment details (OS, Python version)

Next Steps
==========

Start contributing:

1. Set up your environment: :doc:`environment_setup`
2. Read coding standards: :doc:`coding_standards`
3. Understand type checking: :doc:`type_checking`
4. Configure pre-commit hooks: :doc:`pre_commit_hooks`
5. Review architecture: :doc:`design_decisions/architecture`
6. Find an issue to work on
7. Submit your first contribution!

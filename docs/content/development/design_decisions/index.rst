.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

================
Design Decisions
================

This section documents major architectural decisions, refactoring plans, and technical
design documents for the Noiz project.

.. contents:: Table of Contents
   :local:
   :depth: 1

Overview
========

These documents capture the "why" behind major technical decisions, architectural patterns,
and planned refactoring efforts. They serve as both historical context and forward-looking
roadmaps for development.

**Document Types**:

* **Architecture Analysis** - Current state assessment and improvement proposals
* **Implementation Plans** - Detailed design for new features or major changes
* **Refactoring Roadmaps** - Prioritized plans for code quality improvements

Refactoring Roadmap
===================

:doc:`refactoring_roadmap` - Complete plan for modernizing the Noiz codebase

**Key Topics**:

* **Phase 0**: Critical infrastructure fixes (scipy, PyPI packaging, SQLAlchemy 2.0)
* **Phase 1**: UUID migration to fix multiprocessing bugs
* **Phase 2**: Worker code updates for parallel processing
* **Phase 3**: SQLite support for easier deployment
* **Phase 4**: Remove Dask dependency
* **Phase 5**: Complete type hints coverage

**Priority**: CRITICAL - Several items block installation and distribution

**Timeline**: 13-17 weeks total

**Quick Wins** (first week):

* Fix scipy version typo (30 minutes)
* Vendor migrations in package (1 hour)
* Test publish to PyPI (2-3 hours)

See: :doc:`refactoring_roadmap` for complete details

Architecture Analysis
=====================

:doc:`architecture` - Comprehensive code quality and architecture assessment

**Key Findings**:

* **Strengths**: Clean layer separation, no circular dependencies
* **File Tracking Issues**: 11+ untracked file writes (plots, caches, debug outputs)
* **Large Files**: beamforming.py at 1,439 lines needs splitting
* **QC Naming**: Generic "QCOne" and "QCTwo" should be renamed

**Priority Areas**:

1. **Critical Infrastructure** (4-5 weeks) - Must complete first
2. **File Tracking Fixes** (1-2 weeks) - Prevents data loss
3. **QC System Renaming** (2-3 weeks) - Improves clarity
4. **Refactor Large Files** (2-3 weeks) - Improves maintainability
5. **Error Handling** (1 week) - Standardize patterns

**Total Effort**: 18-26 weeks (4.5-6.5 months)

See: :doc:`architecture` for detailed analysis

S3-Compatible Storage
=====================

:doc:`s3_storage` - Plan for S3-compatible filesystem support

**Goal**: Enable cloud deployment with S3, MinIO, or other object storage backends

**Key Features**:

* **fsspec-based** abstraction layer
* **Transparent caching** for remote files
* **Backward compatible** with local filesystem
* **URI-based paths** (``s3://bucket/path``, ``file:///local/path``)
* **Multiple backends**: AWS S3, Google Cloud Storage, Azure Blob, MinIO

**Implementation Phases**:

1. **Foundation** (Week 1) - Create storage abstraction layer
2. **Path Helpers** (Week 1-2) - Migrate to URI-based paths
3. **File I/O** (Week 2-3) - Update all read/write operations
4. **Database** (Week 3) - Store URIs in database
5. **Documentation** (Week 4) - Config examples and migration guides

**Benefits**:

* Cloud-native deployment
* Horizontal scalability
* Cost-effective archival storage
* No shared filesystem required

**Timeline**: 3-4 weeks

**Priority**: Medium (enables cloud deployment after core fixes)

See: :doc:`s3_storage` for implementation details

Transferrable Configuration System
===================================

:doc:`config_system` - ID-based configuration system with dependency tracking

**Goal**: Enable full scientific reproducibility through transferrable processing
pipelines

**Key Features**:

* **Human-friendly IDs**: ``dc_2023_highfreq_v1`` instead of database IDs
* **Explicit dependencies**: Parent-child relationships in configs
* **Pipeline export**: Complete processing chains in single TOML file
* **Visualization**: Dependency graphs and tree views
* **Flask editor**: Visual pipeline builder

**Problem Solved**:

Current configs cannot be shared between users.
No way to export complete processing pipeline with all parameter relationships.
Results cannot be reproduced without manual config reconstruction.

**Implementation Phases**:

1. **Core Infrastructure** (2 weeks) - Parser, validator, database schema
2. **Import/Export** (2 weeks) - Pipeline serialization
3. **Visualization** (1-2 weeks) - Dependency graphs, CLI tools
4. **Flask Editor** (3-4 weeks) - Visual pipeline builder
5. **Migration** (1 week) - Update existing configs

**Benefits**:

* Publications include complete processing parameters
* Collaborators can replicate exact pipeline
* Version control for processing workflows
* Educational pipeline templates

**Timeline**: 8-10 weeks

**Priority**: High (critical for scientific reproducibility)

See: :doc:`config_system` for complete design

Pluggable Architecture
======================

:doc:`plugin_architecture` - Transform Noiz into modular plugin platform

**Goal**: Enable external developers to create and distribute processing plugins

**Key Features**:

* **Self-contained plugins**: Each processing step is independent module
* **Own models/api/processing/cli**: Plugin contains all its code
* **Plugin discovery**: Automatic loading via entry points
* **External development**: PyPI-distributed plugins
* **Dependency resolution**: Plugins declare requirements

**Current Problem**:

Monolithic codebase where all processing shares infrastructure.
Cannot add new processing methods without forking entire project.
No way to distribute custom processing as separate package.

**Plugin Structure**:

::

    noiz/
    ├── core/               # Minimal framework
    └── plugins/
        ├── datachunk/      # Self-contained
        │   ├── models.py
        │   ├── api.py
        │   ├── processing.py
        │   └── cli.py
        └── beamforming/    # Self-contained
            └── ...

**Implementation Phases**:

1. **Preparation** (2 weeks) - Design and proof of concept
2. **Core Framework** (4 weeks) - Plugin system infrastructure
3. **Migrate Existing** (6-8 weeks) - Convert to plugins
4. **External Support** (2 weeks) - Developer tools
5. **Polish** (2 weeks) - Documentation and migration

**Benefits**:

* External developers can extend Noiz
* Install only needed processing modules
* Specialized maintainers per plugin
* Community-driven development

**Timeline**: 16-20 weeks

**Priority**: High (enables ecosystem growth)

**Dependencies**: Should implement after config_system

See: :doc:`plugin_architecture` for complete design

Roadmap Integration
===================

Recommended Implementation Order
--------------------------------

The three design documents are interconnected. Here's the recommended order:

1. **Phase 0: Infrastructure Fixes** (:doc:`refactoring_roadmap` Phase 0)

   * Fix scipy version
   * PyPI packaging and test publish
   * SQLAlchemy 2.0 migration
   * Python 3.11-3.13 support

   **Duration**: 4-5 weeks

   **Why First**: Unblocks everything else

2. **File Tracking Fixes** (:doc:`architecture` Priority 2)

   * Create File models for plots and caches
   * Remove hardcoded paths
   * Track all file writes in database

   **Duration**: 1-2 weeks

   **Why Second**: Ensures all files tracked before S3 migration

3. **S3 Storage Implementation** (:doc:`s3_storage`)

   * Implement storage abstraction layer
   * Migrate to URI-based paths
   * Add S3/MinIO support

   **Duration**: 3-4 weeks

   **Why Third**: Builds on clean file tracking system

4. **UUID Migration** (:doc:`refactoring_roadmap` Phase 1-2)

   * Add UUID columns to models
   * Fix multiprocessing bugs
   * Update worker code

   **Duration**: 4-5 weeks

   **Why Fourth**: Benefits from S3 storage layer

5. **QC Renaming & Code Quality** (:doc:`architecture` Priority 3-4)

   * Rename QCOne/QCTwo
   * Refactor large files
   * Standardize error handling

   **Duration**: 5-7 weeks

   **Why Fifth**: Improves maintainability

6. **Modernization** (:doc:`refactoring_roadmap` Phase 3-5)

   * Multiple execution backends (sequential/multiprocessing/dask)
   * Add SQLite support
   * Complete type hints

   **Duration**: 5-8 weeks

   **Why Last**: Polishing and flexibility improvements

**Total Timeline**: 22-31 weeks (5.5-7.5 months)

Dependencies and Synergies
---------------------------

**Blocking Dependencies**:

* S3 storage BLOCKED BY: Phase 0 (infrastructure), File tracking fixes
* UUID migration BLOCKED BY: Phase 0 (SQLAlchemy 2.0)
* SQLite support BLOCKED BY: UUID migration

**Synergies**:

* S3 storage + UUID migration: Both modify File models
* Multiple backends + S3 storage: Dask already uses fsspec
* File tracking + S3 storage: Clean file handling before cloud migration

Contributing to Design Decisions
=================================

Proposing New Design Documents
-------------------------------

When proposing major architectural changes or new features:

1. **Research Phase**

   * Analyze current implementation
   * Identify problems and constraints
   * Research alternative solutions
   * Evaluate trade-offs

2. **Document Structure**

   Use this template for new design documents:

   .. code-block:: rst

       ==================
       Your Title Here
       ==================

       :Status: Proposed / In Progress / Implemented / Deprecated
       :Author: Your Name
       :Created: YYYY-MM-DD
       :Updated: YYYY-MM-DD

       Overview
       ========

       Brief summary of the problem and proposed solution.

       Problem Statement
       =================

       Detailed description of the problem to solve.

       Proposed Solution
       =================

       Your design proposal with code examples.

       Alternatives Considered
       =======================

       Other options you evaluated and why you rejected them.

       Implementation Plan
       ===================

       Step-by-step plan with timeline estimates.

       Migration Strategy
       ==================

       How to transition from current to new implementation.

       Risks and Mitigation
       ====================

       Potential problems and how to address them.

3. **Review Process**

   * Create issue in GitLab with "design-decision" label
   * Link to design document in ``docs/content/development/design_decisions/``
   * Discuss with maintainers
   * Iterate based on feedback
   * Update document status when implementation begins

4. **Implementation**

   * Reference design document in commit messages
   * Update document status and add implementation notes
   * Add "Implemented" status when complete

Updating Existing Documents
----------------------------

Design documents should be living documents:

* **Status Changes**: Update as work progresses
* **Implementation Notes**: Add learnings during implementation
* **Corrections**: Fix inaccuracies discovered during implementation
* **Timeline Updates**: Adjust estimates based on actual progress

Document Status Values
----------------------

* **Proposed** - Initial draft, under review
* **Accepted** - Approved for implementation
* **In Progress** - Currently being implemented
* **Implemented** - Complete and deployed
* **Deprecated** - Superseded by newer design or no longer relevant

Related Documentation
=====================

Development Guides
------------------

* :doc:`../environment_setup` - Setting up development environment
* :doc:`../coding_standards` - Code conventions and requirements
* :doc:`../type_checking` - Type checking with mypy
* :doc:`../pre_commit_hooks` - Pre-commit hook configuration

API Documentation
-----------------

* `API Reference <../../autoapi/index.html>`_ - Auto-generated API documentation
* `Processing Functions <../../autoapi/noiz/processing/index.html>`_ - Processing layer
* `Models <../../autoapi/noiz/models/index.html>`_ - Database models

User Guides
-----------

* `User Guides <../../guides/index.html>`_ - How to use Noiz
* `Tutorials <../../tutorials/index.html>`_ - Step-by-step tutorials

Summary
=======

These design documents provide:

* **Context**: Why decisions were made
* **Roadmaps**: Where development is heading
* **Implementation Details**: How to execute major changes
* **Integration**: How pieces fit together

**Start here**:

* New contributors: Read :doc:`architecture` for current state assessment
* Active developers: Follow :doc:`refactoring_roadmap` for priority order
* Cloud deployment: Review :doc:`s3_storage` for storage options
* Scientific reproducibility: Review :doc:`config_system` for transferrable configs
* Platform extensibility: Review :doc:`plugin_architecture` for plugin system

.. toctree::
   :maxdepth: 1
   :caption: Design Documents

   refactoring_roadmap
   architecture
   s3_storage
   config_system
   plugin_architecture

See Also
========

* :doc:`../index` - Development documentation index
* :doc:`../coding_standards` - Code conventions
* :doc:`../type_checking` - Type checking requirements

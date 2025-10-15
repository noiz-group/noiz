.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

====================================
Code Quality & Architecture Analysis
====================================

**Comprehensive Code Quality & Architecture Analysis**

:Analysis Date: 2025-10-15
:Last Active Development: ~2021 (4 years ago)

.. contents:: Table of Contents
   :local:
   :depth: 2

Executive Summary
=================

This document provides a thorough analysis of the Noiz codebase after 4 years of minimal development,
identifying architectural issues, code quality concerns, and proposing concrete improvements. The
analysis focuses on separation of concerns, file tracking issues, QC naming improvements, and overall
modernization needs.

Key Findings
------------

**Strengths**:

* Good separation: No circular dependencies between ``api/`` and ``processing/``
* Minimal TODOs: Only 3 FIXME/TODO comments found (better than expected)

**Weaknesses**:

* File tracking issues: 11+ locations write files without DB tracking (plots, caches, debug outputs)
* Large monolithic files: ``beamforming.py`` at 1,439 lines needs refactoring

**Warnings**:

* QC naming: Generic "QCOne" and "QCTwo" should be renamed to descriptive names

Architecture Overview
=====================

Current Layer Structure
-----------------------

::

    ┌─────────────────────────────────────────┐
    │           CLI (cli.py)                   │  ← User-facing commands
    └─────────────────────────────────────────┘
                       ↓
    ┌─────────────────────────────────────────┐
    │        API Layer (api/)                  │  ← Orchestration, DB queries
    │  - Fetches DB objects                    │
    │  - Prepares calculation inputs           │
    │  - Calls processing/ functions           │
    │  - Handles bulk DB operations            │
    └─────────────────────────────────────────┘
                       ↓
    ┌─────────────────────────────────────────┐
    │    Processing Layer (processing/)        │  ← Pure computation
    │  - Signal processing algorithms          │
    │  - File I/O operations                   │
    │  - NO direct DB session usage ✓          │
    └─────────────────────────────────────────┘
                       ↓
    ┌─────────────────────────────────────────┐
    │       Models Layer (models/)             │  ← Data definitions
    │  - SQLAlchemy ORM models                 │
    │  - Type aliases                          │
    └─────────────────────────────────────────┘

Key Architectural Strengths
----------------------------

1. **Clean layer separation**: ``processing/`` has ZERO imports from ``api/``
   maintaining unidirectional dependency flow
2. **No DB coupling in processing**: Processing layer doesn't use
   ``db.session`` directly
3. **Type-safe inputs**: Uses TypedDicts for passing data between layers
4. **Proper use of generators**: Batch processing uses generators to manage
   memory

Separation of Concerns Analysis
================================

What's Working Well
-------------------

1. API Layer Properly Orchestrates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Location**: ``src/noiz/api/``

**Pattern**: API functions query DB, prepare inputs, call processing functions, handle bulk upserts

**Example** (``api/beamforming.py:106-136``):

.. code-block:: python

    def run_beamforming(...):
        # 1. Prepare inputs (DB queries)
        calculation_inputs = _prepare_inputs_for_beamforming_runner(...)

        # 2. Delegate to processing
        if parallel:
            _run_calculate_and_upsert_on_dask(
                calculation_task=calculate_beamforming_results_wrapper,  # from processing/
                ...
            )

2. Processing Layer is Pure
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Location**: ``src/noiz/processing/``

**Pattern**: Pure functions that take inputs, compute results, return objects

**Example** (``processing/ppsd.py:24-155``):

.. code-block:: python

    def calculate_ppsd_wrapper(inputs: PPSDRunnerInputs) -> Tuple[PPSDResult, ...]:
        # Pure computation, no DB access
        # Returns result objects for API layer to persist

3. Models Define Clear Contracts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Location**: ``src/noiz/models/``

* 17 model files, each focused on specific domain
* Good modularity: Separate files for ``qc.py``, ``beamforming.py``, ``crosscorrelation.py``, etc.

Areas Needing Improvement
--------------------------

1. Large Monolithic Files
~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 10 25 35

   * - File
     - Lines
     - Issue
     - Proposed Fix
   * - ``processing/beamforming.py``
     - 1,439
     - Too large, mixed concerns
     - Split into multiple files
   * - ``processing/datachunk.py``
     - 886
     - Mixes fetching and processing
     - Split fetching into ``api/`` layer
   * - ``processing/configs.py``
     - 780
     - All config parsing in one file
     - Split by config type
   * - ``processing/event_detection.py``
     - 767
     - Complex event detection logic
     - Extract validation, plotting

**Recommended**: Files over 500 lines should be reviewed for splitting
opportunities.

2. Inconsistent Error Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Issue**: Mix of raising exceptions and logging errors

**Location**: Throughout ``processing/`` and ``api/``

**Example** (``api/helpers.py:115-120``):

.. code-block:: python

    except (IntegrityError, UnmappedInstanceError, InvalidRequestError) as e:
        logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
        db.session.rollback()
        logger.warning("Retrying with upsert")

**Problem**: Silent failures in parallel processing make debugging difficult

**Fix**: Establish consistent error handling policy (see Proposed Improvements)

3. Mixed Responsibilities in Processing Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some processing functions do too much:

* ``processing/beamforming.py``: Mixes basis construction, convolution, peak detection, and result assembly
* ``processing/configs.py``: Mixes TOML parsing, validation, and DB object creation

File Tracking Issues
====================

Critical Finding: Untracked File Writes
----------------------------------------

**Problem**: After 4 years, several processing functions write files directly without creating
corresponding database entries. This creates **data loss risk** and makes it impossible to
track data lineage.

Files WITH Proper DB Tracking
------------------------------

These follow the correct pattern: Create File model → Write to disk → Store
File reference in result

.. list-table::
   :header-rows: 1
   :widths: 35 25 30 10

   * - Location
     - File Type
     - DB Model
     - Status
   * - ``datachunk_processing.py:254-257``
     - Processed datachunk
     - ``ProcessedDatachunkFile``
     - OK
   * - ``datachunk.py:825-826``
     - Raw datachunk
     - ``DatachunkFile``
     - OK
   * - ``component.py:133-136``
     - Inventory XML
     - ``ComponentFile``
     - OK
   * - ``ppsd.py:148-153``
     - PPSD results
     - ``PPSDFile``
     - OK
   * - ``event_detection.py:213-218``
     - Event traces
     - ``EventDetectionFile``
     - OK

**Pattern** (correct):

.. code-block:: python

    # 1. Create File model
    proc_datachunk_file = ProcessedDatachunkFile(filepath=str(filepath))

    # 2. Write to disk
    st.write(proc_datachunk_file.filepath, format="mseed")

    # 3. Store file reference in result
    processed_datachunk = ProcessedDatachunk(
        file=proc_datachunk_file,  # ← DB tracks this file
        ...
    )
    return processed_datachunk

Files WITHOUT DB Tracking
--------------------------

These write files but don't create DB records, causing **data loss**:

.. list-table::
   :header-rows: 1
   :widths: 30 20 25 25

   * - Location
     - File Type
     - Issue
     - Impact
   * - ``ppsd.py:212``
     - PPSD plot (PNG)
     - ``fig.savefig(filepath)``
     - Plots untracked
   * - ``ppsd.py:326``
     - PPSD temporal plot
     - ``fig.savefig(filepath)``
     - Same as above
   * - ``event_detection.py:672``
     - Event detection plot
     - ``fig.savefig(str(outfile))``
     - Plots untracked
   * - ``beamforming.py:431``
     - Basis cache (NPZ)
     - ``np.savez(path_basis, ...)``
     - Cache files untracked
   * - ``beamforming.py:458``
     - Convolved basis cache
     - ``np.savez(new_file_name)``
     - Same as above
   * - ``beamforming.py:935``
     - Beamforming result
     - ``np.savez_compressed``
     - File not in DB
   * - ``beamforming.py:1081``
     - Debug plot
     - Hardcoded path
     - CRITICAL issue
   * - ``array_analysis.py:313``
     - Power maps
     - Hardcoded names
     - CRITICAL issue
   * - ``io.py:30``
     - Generic NPZ export
     - ``np.savez``
     - Context-dependent
   * - ``event_detection.py:228``
     - Characteristic function
     - ``np.savez(file=...)``
     - Data not tracked
   * - ``event_detection.py:594-599``
     - Event confirmation files
     - Multiple writes
     - Stage untracked

Severity Assessment
-------------------

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Severity
     - Count
     - Description
   * - CRITICAL
     - 2
     - Hardcoded paths/names (impossible to track)
   * - HIGH
     - 5
     - Results/caches not tracked (data loss risk)
   * - MEDIUM
     - 4
     - Plots not tracked (minor inconvenience)

Recommended Fix Pattern
-----------------------

**Create new File models**:

.. code-block:: python

    # Add to models/
    class PPSDPlotFile(db.Model):
        __tablename__ = "ppsd_plot_file"
        id = db.Column(db.BigInteger, primary_key=True)
        filepath = db.Column(db.UnicodeText, nullable=False, unique=True)
        plot_type = db.Column(db.Unicode(50))  # "temporal", "2d", etc.

    class BeamformingBasisFile(db.Model):
        __tablename__ = "beamforming_basis_file"
        id = db.Column(db.BigInteger, primary_key=True)
        filepath = db.Column(db.UnicodeText, nullable=False, unique=True)
        basis_type = db.Column(db.Unicode(50))  # "basis", "convolved_basis"

**Update processing functions**:

.. code-block:: python

    # Before (WRONG)
    fig.savefig(filepath)

    # After (CORRECT)
    plot_file = PPSDPlotFile(filepath=str(filepath), plot_type="temporal")
    fig.savefig(plot_file.filepath)
    ppsd_result.plot_file = plot_file  # Link to result

QC System Renaming Proposal
============================

Current Structure
-----------------

The current QC system uses generic names that don't indicate **what** they're checking:

::

    QCOne → Applied to: Datachunks
            Checks: GPS time errors, signal statistics, time bounds, rejected time periods

    QCTwo → Applied to: Crosscorrelations
            Checks: Time bounds, rejected time periods

Problems
--------

1. Names don't indicate purpose ("QCOne" could mean anything)
2. Not extensible (what happens when you add more QC steps?)
3. Confusing for new developers
4. Doesn't follow the pattern of other processing stages

Proposed Renaming
-----------------

Option A: Stage-Based Naming (RECOMMENDED)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rename QC steps to match the processing stage they validate:

.. list-table::
   :header-rows: 1
   :widths: 15 25 20 20 20

   * - Current
     - Proposed
     - Applied To
     - Config Table
     - Results Table
   * - ``QCOneConfig``
     - ``DatachunkQCConfig``
     - Datachunk
     - ``datachunk_qc_config``
     - ``datachunk_qc_results``
   * - ``QCOneResults``
     - ``DatachunkQCResults``
     - Datachunk
     - (same)
     - (same)
   * - ``QCTwoConfig``
     - ``CrosscorrelationQCConfig``
     - CrosscorrelationCartesian
     - ``crosscorrelation_qc_config``
     - ``crosscorrelation_qc_results``
   * - ``QCTwoResults``
     - ``CrosscorrelationQCResults``
     - CrosscorrelationCartesian
     - (same)
     - (same)

**New QC stages to add**:

.. list-table::
   :header-rows: 1
   :widths: 30 25 45

   * - Stage
     - Applied To
     - Purpose
   * - ``ProcessedDatachunkQC``
     - ProcessedDatachunk
     - Validate spectral whitening
   * - ``BeamformingQC``
     - BeamformingResult
     - Validate trace count and peak quality
   * - ``StackingQC``
     - StackingResult
     - Validate stacked CCFs SNR
   * - ``PPSDResultQC``
     - PPSDResult
     - Validate PPSD calculation quality

Option B: Purpose-Based Naming
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rename based on what the QC validates:

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Current
     - Proposed
     - Rationale
   * - ``QCOneConfig``
     - ``SignalQualityConfig``
     - Checks signal statistics and GPS
   * - ``QCTwoConfig``
     - ``TimeWindowQualityConfig``
     - Checks time windows are valid

**Pros**: More descriptive of actual checks

**Cons**: Less obvious which processing stage it applies to

Migration Plan
--------------

Phase 1: Add new names alongside old (no breaking changes)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # In models/qc.py
    class DatachunkQCConfig(db.Model):
        __tablename__ = "datachunk_qc_config"  # NEW table
        # ... copy all fields from QCOneConfig

    # Keep old class as alias during transition
    QCOneConfig = DatachunkQCConfig  # Backward compatibility

Phase 2: Update all references
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Update ``api/qc.py``: rename all fetch functions
* Update ``processing/qc.py``: rename calculation functions
* Update CLI commands: ``noiz processing run_qcone`` → ``noiz processing run_datachunk_qc``
* Update documentation

Phase 3: Deprecation warning period
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import warnings

    def fetch_qcone_config_single(config_id: int):
        warnings.warn(
            "fetch_qcone_config_single is deprecated, use fetch_datachunk_qc_config_single",
            DeprecationWarning,
            stacklevel=2
        )
        return fetch_datachunk_qc_config_single(config_id)

Phase 4: Remove old names (breaking change, major version bump)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Estimated effort**: 2-3 weeks for complete migration

Code Quality Issues
===================

1. Minimal TODOs/FIXMEs (Good News!)
-------------------------------------

Only **3 instances** found in the entire codebase:

.. list-table::
   :header-rows: 1
   :widths: 50 25 25

   * - File
     - Line
     - Comment
   * - ``processing/signal_utils.py``
     - Multi
     - 2 instances
   * - ``processing/configs.py``
     - 1
     - 1 instance

**Assessment**: This is excellent for a 4-year-old codebase.

2. Deprecated Dependencies
---------------------------

From ``pyproject.toml``:

.. code-block:: toml

    scipy==1.15.3           # ← DOESN'T EXIST! Blocks installation
    dask[complete]==2021.11.2   # 3+ years old
    sqlalchemy>=1.4.0,<2.0      # Explicitly blocks SQLAlchemy 2.0
    python>=3.10,<3.11          # Locked to Python 3.10 only

**Impact**: Users can't install Noiz with modern Python or dependencies.

3. Missing Type Hints
----------------------

**Coverage**: ~40% of functions have type hints

**Problem areas**:

* ``api/`` layer: Many functions missing return types
* ``processing/`` layer: Internal helper functions untyped
* ``models/``: Relationships not typed

**Example** (from ``api/beamforming.py``):

.. code-block:: python

    # Missing return type
    def run_beamforming(...):  # ← Should be: -> None
        ...

4. Inconsistent Documentation
------------------------------

**Mix of styles**:

* Some functions: Full Sphinx-style docstrings
* Some functions: "filldocs" placeholder
* Some functions: No docstring at all

**Example** (``api/ppsd.py:52``):

.. code-block:: python

    def fetch_ppsd_results(...) -> List[PPSDResult]:
        """filldocs"""  # ← Placeholder never filled

5. Hardcoded Paths
------------------

**Found instances**:

.. code-block:: python

    # beamforming.py:1081
    plt.savefig("/processed-data-dir/tmp_beamforming/" + str(self.midtime) + ".png")
    # ↑ HARDCODED ABSOLUTE PATH

    # obspy_derived/array_analysis.py:313-314
    np.savez("pow_map_%d.npz" % i, pow_map)
    # ↑ RELATIVE PATH, unpredictable location

**Fix**: Use ``PROCESSED_DATA_DIR`` from settings consistently.

6. No Connection Pooling Configuration
---------------------------------------

**Issue** (``database.py:16``):

.. code-block:: python

    db = SQLAlchemy()  # ← Uses defaults

**Problem**: No explicit connection pool configuration could cause connection exhaustion
in parallel processing.

**Recommended**:

.. code-block:: python

    from sqlalchemy.pool import QueuePool

    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_size': 20,
        'pool_recycle': 3600,
        'pool_pre_ping': True,
        'max_overflow': 10
    }

7. Bare Exception Catches
--------------------------

**Pattern** (from ``api/helpers.py``):

.. code-block:: python

    except (IntegrityError, UnmappedInstanceError, InvalidRequestError) as e:
        # Catches too broadly

**Better**: Catch specific exceptions, let unexpected errors propagate.

Proposed Improvements
=====================

Priority 1: Critical Infrastructure (Blocks Everything)
--------------------------------------------------------

**Must fix before any other work**:

1. **Fix scipy version** (30 min)

   * Change ``scipy==1.15.3`` to ``scipy>=1.11.0,<2.0``
   * See: :doc:`refactoring_roadmap` Phase 0.1

2. **Vendor migrations** (1 hour)

   * Include ``migrations/`` directory in package
   * See: :doc:`refactoring_roadmap` Phase 0.5

3. **PyPI packaging** (2-3 hours)

   * Setup ``pyproject.toml`` for PyPI
   * Test publish (can yank after)
   * See: :doc:`refactoring_roadmap` Phase 0.6-0.6.1

Priority 2: File Tracking Fixes (Prevents Data Loss)
-----------------------------------------------------

**Impact**: HIGH - prevents data loss and enables cleanup

**Effort**: 1-2 weeks

**Tasks**:

1. **Create new File models** for untracked files:

   .. code-block:: python

       # In models/plot_files.py
       class PlotFile(db.Model):
           id = db.Column(db.BigInteger, primary_key=True)
           filepath = db.Column(db.UnicodeText, unique=True)
           plot_type = db.Column(db.Unicode(50))
           created_at = db.Column(db.DateTime, default=datetime.utcnow)

       class BeamformingCacheFile(db.Model):
           id = db.Column(db.BigInteger, primary_key=True)
           filepath = db.Column(db.UnicodeText, unique=True)
           cache_type = db.Column(db.Unicode(50))  # "basis", "convolved_basis"

2. **Update processing functions** to create File models before writing

3. **Add relationships** to result models:

   .. code-block:: python

       class PPSDResult(db.Model):
           # Existing fields...
           plot_file_id = db.Column(db.BigInteger, db.ForeignKey("plot_file.id"))
           plot_file = db.relationship("PlotFile")

4. **Remove all hardcoded paths**:

   * Fix ``beamforming.py:1081``
   * Fix ``obspy_derived/array_analysis.py:313-314``
   * Use ``PROCESSED_DATA_DIR`` consistently

5. **Add migration** to create new tables

6. **Update documentation** on file handling patterns

Priority 3: QC Renaming (Improves Clarity)
-------------------------------------------

**Impact**: MEDIUM - improves developer experience

**Effort**: 2-3 weeks

**Tasks**:

1. **Create new models** with descriptive names (keep old as aliases)
2. **Add new QC stages**: ProcessedDatachunkQC, BeamformingQC, StackingQC, PPSDQC
3. **Update API functions** to use new names
4. **Update CLI commands** to use new names
5. **Add deprecation warnings** for old names
6. **Update documentation** and config examples
7. **Plan breaking change** for next major version

See QC System Renaming Proposal for full details.

Priority 4: Refactor Large Files (Improves Maintainability)
------------------------------------------------------------

**Impact**: MEDIUM - improves code maintainability

**Effort**: 2-3 weeks

**Files to split**:

1. **processing/beamforming.py** (1,439 lines)

   Split into:

   * ``beamforming_core.py`` - Main calculation logic (lines 1-400)
   * ``beamforming_basis.py`` - Basis construction (lines 401-600)
   * ``beamforming_peaks.py`` - Peak detection (lines 601-800)
   * ``beamforming_plotting.py`` - Visualization (lines 801-1000)
   * ``beamforming_validation.py`` - Validation helpers (lines 1001-1439)

2. **processing/datachunk.py** (886 lines)

   Move fetching logic to ``api/datachunk.py``

   Keep only: ``prepare_datachunk()``, ``fetch_data_for_datachunk()``

3. **processing/configs.py** (780 lines)

   Split by config type:

   * ``configs/datachunk.py``
   * ``configs/crosscorrelation.py``
   * ``configs/beamforming.py``
   * ``configs/ppsd.py``

Priority 5: Error Handling Standardization
-------------------------------------------

**Impact**: MEDIUM - improves debugging

**Effort**: 1 week

**Establish patterns**:

.. code-block:: python

    # In api/ layer - orchestration
    def run_crosscorrelations(...):
        try:
            inputs = prepare_inputs()
            results = calculate(inputs)
        except CorruptedDataException as e:
            logger.error(f"Data corrupted: {e}")
            if raise_errors:
                raise
            return
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            raise  # Always re-raise unexpected errors

    # In processing/ layer - computation
    def calculate_crosscorrelation(...):
        # Let exceptions propagate
        # Only catch specific, expected errors
        if not validate_input(data):
            raise ValueError("Invalid input: ...")

**Add structured logging**:

.. code-block:: python

    from structlog import get_logger

    logger = get_logger()
    logger.info("processing_started",
                timespan_id=ts.id,
                params_id=params.id)

Priority 6: Database Connection Management
-------------------------------------------

**Impact**: LOW - prevents connection issues in parallel processing

**Effort**: 1 day

**Add to database.py**:

.. code-block:: python

    def init_db(app):
        app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
            'pool_size': 20,              # Concurrent connections
            'pool_recycle': 3600,         # Recycle after 1 hour
            'pool_pre_ping': True,        # Test connections
            'max_overflow': 10,           # Burst capacity
            'echo_pool': True,            # Debug connection pool
        }
        db.init_app(app)

Priority 7: Type Hints Completion
----------------------------------

**Impact**: LOW - improves IDE support

**Effort**: 2-3 weeks

See: :doc:`refactoring_roadmap` Phase 5 for full details

Implementation Roadmap
======================

Phase 0: Infrastructure (4-5 weeks) - MUST DO FIRST
----------------------------------------------------

See :doc:`refactoring_roadmap` for details:

* Phase 0.1: Fix scipy version
* Phase 0.2-0.3: Upgrade dependencies and Python versions
* Phase 0.4: Migrate to SQLAlchemy 2.0
* Phase 0.5: Vendor migrations
* Phase 0.6: Setup PyPI packaging
* Phase 0.7: Add release automation

Phase 1: File Tracking Fixes (1-2 weeks)
-----------------------------------------

* Create new File models
* Update all processing functions
* Remove hardcoded paths
* Add migration
* Update documentation

Phase 2: QC Renaming (2-3 weeks)
---------------------------------

* Add new QC models with descriptive names
* Add new QC stages (ProcessedDatachunkQC, etc.)
* Update API and CLI
* Add deprecation warnings
* Update documentation

Phase 3: Code Quality (3-4 weeks)
----------------------------------

* Refactor large files
* Standardize error handling
* Add connection pooling
* Complete type hints

Phase 4: UUID Migration (4-6 weeks)
------------------------------------

See :doc:`refactoring_roadmap` Phase 1-2 for details on fixing multiprocessing issues

Phase 5: Modernization (4-6 weeks)
-----------------------------------

* Multiple execution backends (see :doc:`refactoring_roadmap` Phase 4)
* Add SQLite support (see :doc:`refactoring_roadmap` Phase 3)
* Complete type hints (see :doc:`refactoring_roadmap` Phase 5)

**Total Estimated Effort**: 18-26 weeks (4.5-6.5 months)

Appendix
========

File Statistics
---------------

::

    Total Python files: 75
    Total lines of code: ~23,000

    Largest files:
    1. processing/beamforming.py         1,439 lines
    2. processing/datachunk.py             886 lines
    3. processing/configs.py               780 lines
    4. processing/event_detection.py       767 lines
    5. processing/soh/parsing_params.py    729 lines

    Total functions in processing/: 195
    Average function size: ~43 lines

Model Files (17 total)
-----------------------

::

    models/
    ├── __init__.py
    ├── beamforming.py          # Beamforming results and params
    ├── component.py            # Network components
    ├── component_pair.py       # Component pairs for CCF
    ├── crosscorrelation.py     # Cross-correlation results
    ├── custom_db_types.py      # Custom column types
    ├── datachunk.py            # Raw and processed datachunks
    ├── event_detection.py      # Event detection results
    ├── mixins.py               # Common model mixins
    ├── ppsd.py                 # PPSD results
    ├── processing_params.py    # All processing parameters
    ├── qc.py                   # QCOne and QCTwo
    ├── soh.py                  # State of health data
    ├── stacking.py             # Stacking results
    ├── timeseries.py           # Generic timeseries
    ├── timespan.py             # Time windows
    └── type_aliases.py         # TypedDicts for inputs

API Files (13 total)
---------------------

::

    api/
    ├── __init__.py
    ├── beamforming.py          # Beamforming orchestration
    ├── component.py            # Component fetching
    ├── component_pair.py       # Pair fetching
    ├── configs.py              # Config ingestion
    ├── crosscorrelations.py    # CCF orchestration
    ├── datachunk.py            # Datachunk orchestration
    ├── event_detection.py      # Event detection orchestration
    ├── helpers.py              # Bulk DB operations (CRITICAL)
    ├── ppsd.py                 # PPSD orchestration
    ├── qc.py                   # QC orchestration
    ├── soh.py                  # SOH data handling
    ├── stacking.py             # Stacking orchestration
    └── timespan.py             # Timespan fetching

Dependencies Currently Outdated
--------------------------------

.. code-block:: toml

    scipy==1.15.3              # DOESN'T EXIST!
    dask[complete]==2021.11.2  # Nov 2021 (3+ years old)
    sqlalchemy<2.0             # Blocks SA 2.0
    flask-sqlalchemy==2.5.1    # Sep 2021 (3+ years old)
    obspy==1.3.0               # Nov 2021 (3+ years old)
    pandas==1.4.0              # Jan 2022 (3+ years old)

See Also
========

* :doc:`refactoring_roadmap` - Complete refactoring plan
* :doc:`s3_storage` - S3-compatible storage implementation
* :doc:`../coding_standards` - Coding conventions
* :doc:`../type_checking` - Type checking requirements

Document Information
====================

:Version: 1.0
:Last Updated: 2025-10-15
:Author: Architecture Analysis (Claude Code)

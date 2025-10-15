.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

=================
Coding Standards
=================

This document defines the coding conventions and standards for the Noiz project.
All contributors must follow these guidelines to ensure code consistency and maintainability.

.. contents:: Table of Contents
   :local:
   :depth: 2

Type Hints (MANDATORY)
======================

Type hints are **required** for all code in Noiz. This includes:

* All function signatures
* All method signatures
* Class attributes
* Module-level variables

Type Hint Requirements
----------------------

All Functions Must Have Type Hints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Every function** must include:

* Type hints for all parameters
* Return type annotation
* ``Optional[T]`` for nullable parameters
* Proper imports from ``typing`` module

**Example:**

.. code-block:: python

    from typing import List, Optional, Tuple
    from datetime import datetime
    from noiz.models import Datachunk, Component

    def fetch_datachunks(
        component: Component,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None,
        include_stats: bool = False,
    ) -> List[Datachunk]:
        """
        Fetch datachunks for a component within a time range.

        :param component: Component to fetch datachunks for
        :type component: Component
        :param start_date: Start of time range
        :type start_date: datetime
        :param end_date: End of time range
        :type end_date: datetime
        :param limit: Maximum number of datachunks to return
        :type limit: Optional[int]
        :param include_stats: Whether to eagerly load datachunk statistics
        :type include_stats: bool
        :return: List of datachunks matching the criteria
        :rtype: List[Datachunk]
        :raises ValueError: If start_date is after end_date
        """
        if start_date > end_date:
            raise ValueError("start_date must be before end_date")

        query = Datachunk.query.filter(
            Datachunk.component_id == component.id,
            Datachunk.starttime >= start_date,
            Datachunk.endtime <= end_date,
        )

        if limit is not None:
            query = query.limit(limit)

        return query.all()

Complex Types
~~~~~~~~~~~~~

For complex types, use appropriate ``typing`` constructs:

.. code-block:: python

    from typing import Union, Dict, Any, Callable, TypeVar, Generic
    from pathlib import Path

    # Union types for multiple accepted types
    PathLike = Union[str, Path]

    # Dict with specific key/value types
    ConfigDict = Dict[str, Any]

    # Callable with signature
    ProcessorFunc = Callable[[np.ndarray, float], np.ndarray]

    # Generic types with TypeVar
    T = TypeVar('T')

    def first_or_none(items: List[T]) -> Optional[T]:
        """Return first item or None if list is empty."""
        return items[0] if items else None

TypedDict for Structured Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``TypedDict`` for structured dictionaries passed between layers:

.. code-block:: python

    from typing import TypedDict
    from datetime import datetime

    class ProcessingInputs(TypedDict):
        """Inputs for datachunk processing."""
        datachunk_id: int
        params_id: int
        component_id: int
        timespan_id: int

    class ProcessingResult(TypedDict):
        """Result from datachunk processing."""
        datachunk_id: int
        file_path: str
        processing_time: float
        success: bool
        error_message: Optional[str]

    def process_batch(
        inputs: List[ProcessingInputs]
    ) -> List[ProcessingResult]:
        """Process a batch of datachunks."""
        ...

Documentation Standards
=======================

Docstring Format
----------------

All public APIs must use **Sphinx-style RST docstrings**:

.. code-block:: python

    def calculate_crosscorrelation(
        trace_a: obspy.Trace,
        trace_b: obspy.Trace,
        max_lag: float,
        method: str = "fft",
    ) -> np.ndarray:
        """
        Calculate cross-correlation between two seismic traces.

        This function computes the normalized cross-correlation between two
        ObsPy Trace objects using either FFT or time-domain methods.

        :param trace_a: First seismic trace
        :type trace_a: obspy.Trace
        :param trace_b: Second seismic trace (must have same sampling rate)
        :type trace_b: obspy.Trace
        :param max_lag: Maximum lag time in seconds
        :type max_lag: float
        :param method: Correlation method ('fft' or 'time')
        :type method: str
        :return: Cross-correlation function array
        :rtype: np.ndarray
        :raises ValueError: If sampling rates don't match
        :raises ValueError: If method is not 'fft' or 'time'

        Example
        -------

        .. code-block:: python

            >>> trace_a = obspy.read()[0]
            >>> trace_b = obspy.read()[0]
            >>> ccf = calculate_crosscorrelation(trace_a, trace_b, max_lag=10.0)
            >>> print(ccf.shape)
            (401,)

        Notes
        -----

        The FFT method is significantly faster for long traces but may have
        numerical precision issues for very large lags.

        See Also
        --------

        obspy.signal.cross_correlation.correlate : ObsPy's correlation function
        numpy.correlate : NumPy's correlation function
        """
        if trace_a.stats.sampling_rate != trace_b.stats.sampling_rate:
            raise ValueError("Sampling rates must match")

        if method not in ("fft", "time"):
            raise ValueError("method must be 'fft' or 'time'")

        ...

Private Functions
~~~~~~~~~~~~~~~~~

Private functions (starting with ``_``) should have minimal docstrings:

.. code-block:: python

    def _validate_trace(trace: obspy.Trace) -> None:
        """Validate trace has required attributes."""
        if not hasattr(trace, 'stats'):
            raise ValueError("Invalid trace object")

Code Style
==========

General Rules
-------------

* **Line length**: 119 characters (configured in ``ruff.toml``)
* **Indentation**: 4 spaces (no tabs)
* **Quotes**: Use double quotes ``"`` for strings
* **Imports**: Organized in order: stdlib, third-party, local
* **Naming**: Follow PEP 8 conventions

Naming Conventions
------------------

.. code-block:: python

    # Classes: PascalCase
    class DatachunkProcessor:
        pass

    # Functions and variables: snake_case
    def calculate_energy(data: np.ndarray) -> float:
        total_energy = np.sum(data ** 2)
        return total_energy

    # Constants: UPPER_SNAKE_CASE
    MAX_RETRY_ATTEMPTS = 3
    DEFAULT_BATCH_SIZE = 1000

    # Private: prefix with _
    def _internal_helper() -> None:
        pass

    _MODULE_CACHE: Dict[str, Any] = {}

Import Organization
-------------------

Imports must be organized in three blocks with blank lines between:

.. code-block:: python

    # Standard library
    import os
    import sys
    from datetime import datetime
    from pathlib import Path
    from typing import List, Optional

    # Third-party packages
    import numpy as np
    import obspy
    import pandas as pd
    from loguru import logger
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    # Local imports
    from noiz.database import db
    from noiz.models import Component, Datachunk
    from noiz.processing.signal_utils import bandpass_filter

Use ``isort`` or ``ruff`` to automatically organize imports.

Code Structure
==============

Function Size
-------------

* **Target**: Functions under 50 lines
* **Maximum**: 100 lines (should be rare)
* **If longer**: Refactor into smaller functions

**Bad:**

.. code-block:: python

    def process_everything(data):  # 300 lines of code
        # Load data
        # Validate data
        # Preprocess
        # Calculate FFT
        # Apply filters
        # Calculate correlation
        # Save results
        # Generate plots
        # Send notifications
        ...

**Good:**

.. code-block:: python

    def process_datachunk(datachunk: Datachunk) -> ProcessedDatachunk:
        """Process a single datachunk through the pipeline."""
        data = _load_datachunk_data(datachunk)
        data = _validate_and_preprocess(data)
        spectrum = _calculate_spectrum(data)
        filtered = _apply_filters(spectrum)
        result = _create_result(filtered, datachunk)
        return result

File Size
---------

* **Target**: Files under 500 lines
* **Maximum**: 800 lines
* **If larger**: Split into multiple files

**Example refactoring:**

.. code-block:: python

    # Before: beamforming.py (1,439 lines)
    # After:
    #   beamforming/core.py      (400 lines)
    #   beamforming/basis.py     (300 lines)
    #   beamforming/peaks.py     (300 lines)
    #   beamforming/plotting.py  (250 lines)
    #   beamforming/validation.py (189 lines)

Error Handling
==============

Specific Exceptions
-------------------

Always catch specific exceptions, never bare ``except``:

**Bad:**

.. code-block:: python

    try:
        result = process_data()
    except:  # DON'T DO THIS
        logger.error("Something went wrong")
        return None

**Good:**

.. code-block:: python

    try:
        result = process_data()
    except ValueError as e:
        logger.error(f"Invalid data: {e}")
        raise
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error processing data: {e}")
        raise

Exception Context
-----------------

Add context when re-raising exceptions:

.. code-block:: python

    try:
        datachunk = fetch_datachunk(datachunk_id)
    except DatachunkNotFoundError as e:
        raise ValueError(
            f"Cannot process non-existent datachunk {datachunk_id}"
        ) from e

Custom Exceptions
-----------------

Define custom exceptions in ``src/noiz/exceptions.py``:

.. code-block:: python

    class NoizError(Exception):
        """Base exception for Noiz."""
        pass

    class DatachunkNotFoundError(NoizError):
        """Raised when datachunk doesn't exist."""
        pass

    class CorruptedDataError(NoizError):
        """Raised when data file is corrupted."""
        pass

Logging
=======

Structured Logging
------------------

Use consistent log levels:

* ``DEBUG``: Detailed diagnostic information
* ``INFO``: General informational messages
* ``WARNING``: Warning messages (recoverable issues)
* ``ERROR``: Error messages (operation failed)
* ``CRITICAL``: Critical errors (application may crash)

.. code-block:: python

    from loguru import logger

    def process_datachunk(datachunk: Datachunk) -> ProcessedDatachunk:
        logger.debug(f"Starting processing of {datachunk}")
        logger.info(f"Processing datachunk {datachunk.id} for {datachunk.component}")

        try:
            result = _do_processing(datachunk)
            logger.info(f"Successfully processed datachunk {datachunk.id}")
            return result
        except ValueError as e:
            logger.warning(f"Skipping invalid datachunk {datachunk.id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to process datachunk {datachunk.id}: {e}")
            raise

Avoid Logging in Loops
-----------------------

Don't spam logs with repeated messages:

**Bad:**

.. code-block:: python

    for datachunk in datachunks:  # 100,000 datachunks
        logger.info(f"Processing {datachunk}")  # 100,000 log lines!

**Good:**

.. code-block:: python

    logger.info(f"Processing {len(datachunks)} datachunks")
    for i, datachunk in enumerate(datachunks):
        if i % 1000 == 0:
            logger.info(f"Progress: {i}/{len(datachunks)} datachunks")

Testing
=======

Test Requirements
-----------------

All new code must have tests:

* Unit tests for pure functions
* Integration tests for database operations
* System tests for end-to-end workflows

Test Structure
--------------

.. code-block:: python

    import pytest
    from noiz.processing.signal_utils import bandpass_filter

    def test_bandpass_filter_with_valid_input() -> None:
        """Test bandpass filter produces expected output."""
        # Arrange
        data = np.random.randn(1000)
        lowcut = 1.0
        highcut = 10.0
        sampling_rate = 100.0

        # Act
        filtered = bandpass_filter(data, lowcut, highcut, sampling_rate)

        # Assert
        assert len(filtered) == len(data)
        assert not np.array_equal(filtered, data)  # Data was modified

    def test_bandpass_filter_rejects_invalid_frequencies() -> None:
        """Test bandpass filter raises error for invalid frequencies."""
        data = np.random.randn(1000)

        with pytest.raises(ValueError, match="lowcut must be less than highcut"):
            bandpass_filter(data, lowcut=10.0, highcut=1.0, sampling_rate=100.0)

Type Checking
=============

Mypy Type Checker
-----------------

We use **mypy** for static type checking. All code must pass mypy checks before merging.

See :doc:`type_checking` for full mypy setup and configuration.

Running Type Checks
-------------------

Before committing:

.. code-block:: bash

    # Run mypy type checker
    mypy src/noiz

    # Run mypy with strict mode (goal for new code)
    mypy --strict src/noiz/processing/new_module.py

    # Check must pass with no errors

Tools
=====

Linting
-------

Use ``ruff`` for linting:

.. code-block:: bash

    # Check for issues
    ruff check .

    # Auto-fix issues
    ruff check --fix .

    # Format code
    ruff format .

Formatting
----------

Code formatting is handled by ``ruff format``:

* Line length: 119
* Quote style: Double quotes
* Trailing commas: Yes

RST Documentation Formatting
-----------------------------

All RST documentation must follow **one sentence per line** formatting.

This means:

* Each sentence starts on a new line
* Long sentences can be broken across multiple lines if needed
* Blank lines separate paragraphs
* Makes diffs cleaner and easier to review

**Example:**

.. code-block:: rst

    This is the first sentence.
    This is the second sentence.

    This is a new paragraph with a very long sentence that needs to be
    broken across multiple lines for better readability in the source file.

    This is the third paragraph.

Pre-commit Hooks
----------------

Install pre-commit hooks to run checks automatically:

.. code-block:: bash

    # Install pre-commit
    pip install pre-commit

    # Install hooks
    pre-commit install

    # Run manually
    pre-commit run --all-files

Git Workflow
============

Commit Messages
---------------

Follow conventional commit format:

.. code-block:: text

    type(scope): subject

    body

    footer

**Types:**

* ``feat``: New feature
* ``fix``: Bug fix
* ``docs``: Documentation changes
* ``refactor``: Code refactoring
* ``test``: Adding tests
* ``chore``: Maintenance tasks

**Example:**

.. code-block:: text

    feat(processing): add spectral whitening support

    Implement quefrency domain spectral whitening for datachunk processing.
    This improves cross-correlation quality by normalizing the spectrum.

    Closes #123

Branch Naming
-------------

* ``feature/description`` - New features
* ``fix/description`` - Bug fixes
* ``refactor/description`` - Refactoring
* ``docs/description`` - Documentation

Summary
=======

**Key Requirements:**

1. ✅ **Type hints are mandatory** for all functions
2. ✅ **RST docstrings** for all public APIs
3. ✅ **Ruff compliant** code style
4. ✅ **Mypy type checking** passes
5. ✅ **Tests** for all new code
6. ✅ **Meaningful** commit messages

**Before Every Commit:**

.. code-block:: bash

    # Format code
    ruff format .

    # Check style
    ruff check .

    # Check types
    mypy src/noiz

    # Run tests
    pytest

See Also
========

* :doc:`type_checking` - Mypy setup and type checking guide
* :doc:`design_decisions/index` - Architecture decisions
* :doc:`deprecations` - Deprecation policy

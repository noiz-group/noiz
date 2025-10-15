.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

==============
Type Checking
==============

Noiz uses static type checking to catch bugs early and improve code quality.
This document describes our type checking setup, configuration, and best practices.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

**Type checking is mandatory** for all code in Noiz. We use **mypy** as our primary
type checker to ensure type safety across the codebase.

Why Type Checking?
------------------

Type checking provides:

* **Early bug detection** - Catch type errors before runtime
* **Better IDE support** - Improved autocomplete and refactoring
* **Self-documenting code** - Type hints serve as inline documentation
* **Safer refactoring** - Confidence when making large changes
* **Reduced testing burden** - Many bugs caught statically

Current Tool: Mypy
==================

We currently use **mypy** (http://mypy-lang.org/) for static type checking.

Installation
------------

Mypy is included in the development dependencies:

.. code-block:: bash

    # Install with uv
    uv sync --all-groups

    # Or install separately
    pip install mypy

Configuration
-------------

Mypy is configured in ``pyproject.toml``:

.. code-block:: toml

    [tool.mypy]
    python_version = "3.10"
    warn_return_any = true
    warn_unused_configs = true
    disallow_untyped_defs = false  # TODO: Enable for new code
    disallow_any_unimported = false
    no_implicit_optional = true
    warn_redundant_casts = true
    warn_unused_ignores = true
    warn_no_return = true
    check_untyped_defs = true
    strict_equality = true

    # Ignore missing imports for third-party packages without stubs
    [[tool.mypy.overrides]]
    module = [
        "obspy.*",
        "loguru.*",
        "more_itertools.*",
    ]
    ignore_missing_imports = true

Running Mypy
------------

**Check entire codebase:**

.. code-block:: bash

    mypy src/noiz

**Check specific file:**

.. code-block:: bash

    mypy src/noiz/processing/datachunk.py

**Check with strict mode (for new code):**

.. code-block:: bash

    mypy --strict src/noiz/processing/new_module.py

**Show error codes:**

.. code-block:: bash

    mypy --show-error-codes src/noiz

CI Integration
--------------

Mypy runs automatically in CI/CD:

.. code-block:: yaml

    # In .gitlab-ci.yml
    mypy-check:
      stage: linting
      script:
        - mypy src/noiz
      allow_failure: false  # Must pass

Type Hints Requirements
=======================

All Functions
-------------

Every function must have complete type hints:

.. code-block:: python

    from typing import List, Optional
    from noiz.models import Datachunk

    def fetch_datachunks(
        component_id: int,
        limit: Optional[int] = None
    ) -> List[Datachunk]:
        """Fetch datachunks for a component."""
        ...

**Mypy will error if:**

* Parameters are missing type hints
* Return type is missing
* Type hints are inconsistent with usage

Class Attributes
----------------

Class attributes should be typed:

.. code-block:: python

    from typing import ClassVar, Dict

    class DatachunkProcessor:
        # Instance attribute
        batch_size: int

        # Class attribute
        cache: ClassVar[Dict[int, Datachunk]] = {}

        def __init__(self, batch_size: int) -> None:
            self.batch_size = batch_size

Generators and Iterables
-------------------------

Use proper generic types for generators:

.. code-block:: python

    from typing import Generator, Iterator, Iterable

    def fetch_in_batches(
        ids: List[int],
        batch_size: int
    ) -> Generator[List[Datachunk], None, None]:
        """Yield batches of datachunks."""
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            yield fetch_datachunks(batch_ids)

    def process_stream(
        datachunks: Iterable[Datachunk]
    ) -> Iterator[ProcessedDatachunk]:
        """Process datachunks one at a time."""
        for datachunk in datachunks:
            yield process_datachunk(datachunk)

Common Patterns
===============

Optional Values
---------------

Use ``Optional[T]`` for values that can be ``None``:

.. code-block:: python

    from typing import Optional

    def find_datachunk(datachunk_id: int) -> Optional[Datachunk]:
        """Return datachunk or None if not found."""
        return Datachunk.query.get(datachunk_id)

    # Usage with type narrowing
    datachunk = find_datachunk(123)
    if datachunk is not None:
        # Mypy knows datachunk is Datachunk here, not None
        print(datachunk.id)

Union Types
-----------

Use ``Union`` for multiple possible types:

.. code-block:: python

    from typing import Union
    from pathlib import Path

    PathLike = Union[str, Path]

    def read_file(filepath: PathLike) -> str:
        """Read file from string path or Path object."""
        path = Path(filepath)  # Handles both types
        return path.read_text()

TypedDict for Structured Data
------------------------------

Use ``TypedDict`` for dictionary structures:

.. code-block:: python

    from typing import TypedDict

    class ProcessingConfig(TypedDict):
        """Configuration for datachunk processing."""
        sampling_rate: float
        filter_low: float
        filter_high: float
        normalize: bool

    def configure_processing(config: ProcessingConfig) -> None:
        """Configure processing with typed config dict."""
        print(f"Sampling rate: {config['sampling_rate']}")

Protocols for Duck Typing
--------------------------

Use ``Protocol`` for structural subtyping:

.. code-block:: python

    from typing import Protocol

    class HasFilepath(Protocol):
        """Any object with a filepath attribute."""
        filepath: str

    def save_to_disk(obj: HasFilepath) -> None:
        """Save any object that has a filepath."""
        with open(obj.filepath, 'w') as f:
            f.write(str(obj))

    # Works with any class that has filepath attribute
    save_to_disk(datachunk_file)
    save_to_disk(ppsd_file)

Type Aliases
------------

Define reusable type aliases in ``src/noiz/models/type_aliases.py``:

.. code-block:: python

    from typing import TypeAlias, Union
    from pathlib import Path

    # Simple alias
    ComponentID: TypeAlias = int

    # Complex alias
    PathLike: TypeAlias = Union[str, Path]

    # Forward reference
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from noiz.models import Datachunk

    DatachunkID: TypeAlias = int

Handling Third-Party Libraries
===============================

Missing Type Stubs
------------------

Some libraries don't have type stubs. Handle them in configuration:

.. code-block:: toml

    [[tool.mypy.overrides]]
    module = ["obspy.*", "loguru.*"]
    ignore_missing_imports = true

Creating Stub Files
-------------------

For frequently-used untyped libraries, create stub files in ``stubs/``:

.. code-block:: python

    # stubs/obspy/core/trace.pyi
    from typing import Any
    import numpy as np

    class Trace:
        data: np.ndarray
        stats: Stats
        def filter(self, type: str, **kwargs: Any) -> None: ...
        def write(self, filename: str, format: str) -> None: ...

    class Stats:
        sampling_rate: float
        npts: int
        ...

Type Ignores
------------

Use ``# type: ignore`` sparingly and with comments:

.. code-block:: python

    # Bad - no explanation
    result = some_function()  # type: ignore

    # Good - explains why
    result = some_function()  # type: ignore[attr-defined]  # ObsPy Trace lacks stubs

    # Better - narrow the ignore
    result = some_function()  # type: ignore[no-untyped-call]

Common Mypy Errors
==================

Error: "Function is missing a return type annotation"
-----------------------------------------------------

**Problem:**

.. code-block:: python

    def process_data(data):  # Missing return type
        return data * 2

**Solution:**

.. code-block:: python

    def process_data(data: np.ndarray) -> np.ndarray:
        return data * 2

Error: "Argument has incompatible type"
---------------------------------------

**Problem:**

.. code-block:: python

    def process_int(value: int) -> int:
        return value * 2

    result = process_int("5")  # Error: str is not int

**Solution:**

.. code-block:: python

    # Option 1: Convert type
    result = process_int(int("5"))

    # Option 2: Change function signature
    def process_int(value: Union[int, str]) -> int:
        return int(value) * 2

Error: "Missing positional argument"
------------------------------------

**Problem:**

.. code-block:: python

    def fetch_data(id: int, limit: int) -> List[Data]:
        ...

    fetch_data(123)  # Error: Missing "limit"

**Solution:**

.. code-block:: python

    # Option 1: Provide argument
    fetch_data(123, limit=10)

    # Option 2: Make parameter optional
    def fetch_data(id: int, limit: int = 100) -> List[Data]:
        ...

Error: "Incompatible return value type"
---------------------------------------

**Problem:**

.. code-block:: python

    def get_count() -> int:
        return None  # Error: None is not int

**Solution:**

.. code-block:: python

    # Option 1: Return correct type
    def get_count() -> int:
        return 0

    # Option 2: Allow None
    def get_count() -> Optional[int]:
        return None

Gradual Typing
==============

Existing Codebase
-----------------

The Noiz codebase is being gradually typed:

* Models - Fully typed
* Processing - Partially typed
* API layer - Needs type hints
* CLI - Has ``# mypy: ignore-errors``

Type Coverage Goal
------------------

**Current status:**

.. code-block:: bash

    # Check type coverage
    mypy --html-report mypy-report src/noiz

**Goals:**

* **New code**: 100% type coverage (strict mode)
* **Existing code**: Gradual improvement to 90%+
* **No ``# type: ignore`` without comments**

Strict Mode for New Modules
----------------------------

All new modules must pass ``--strict`` mode:

.. code-block:: python

    # new_module.py
    """New module with strict typing."""

    from typing import List
    from noiz.models import Datachunk

    def fetch_recent_datachunks(limit: int = 10) -> List[Datachunk]:
        """Fetch recent datachunks."""
        return Datachunk.query.order_by(
            Datachunk.created_at.desc()
        ).limit(limit).all()

.. code-block:: bash

    # Must pass strict checks
    mypy --strict src/noiz/processing/new_module.py

Advanced Typing
===============

Generic Functions
-----------------

Use ``TypeVar`` for generic functions:

.. code-block:: python

    from typing import TypeVar, List, Callable

    T = TypeVar('T')

    def batch_process(
        items: List[T],
        processor: Callable[[T], T],
        batch_size: int = 100
    ) -> List[T]:
        """Process items in batches."""
        results = []
        for item in items:
            results.append(processor(item))
        return results

    # Type-safe usage
    datachunks: List[Datachunk] = [...]
    processed: List[ProcessedDatachunk] = batch_process(
        datachunks,
        process_datachunk,  # Type checked!
        batch_size=50
    )

Overloads
---------

Use ``@overload`` for functions with multiple signatures:

.. code-block:: python

    from typing import overload, Union

    @overload
    def fetch_by_id(id: int) -> Datachunk: ...

    @overload
    def fetch_by_id(id: List[int]) -> List[Datachunk]: ...

    def fetch_by_id(
        id: Union[int, List[int]]
    ) -> Union[Datachunk, List[Datachunk]]:
        """Fetch datachunk(s) by ID."""
        if isinstance(id, int):
            return Datachunk.query.get(id)
        else:
            return Datachunk.query.filter(
                Datachunk.id.in_(id)
            ).all()

    # Mypy knows the return type!
    single: Datachunk = fetch_by_id(123)
    multiple: List[Datachunk] = fetch_by_id([1, 2, 3])

Future: Pyre Type Checker
==========================

We are considering migrating to **Pyre** (Facebook's type checker) in the future
for stricter and faster type checking.

Why Pyre?
---------

* **Faster** - 10x faster than mypy on large codebases
* **Stricter** - Catches more type errors
* **Incremental** - Fast incremental checking
* **Better inference** - Smarter type inference
* **Facebook-backed** - Used in production at Meta

Pyre vs Mypy
------------

+-------------------+------------------+------------------+
| Feature           | Mypy             | Pyre             |
+===================+==================+==================+
| Speed             | Moderate         | Very Fast        |
+-------------------+------------------+------------------+
| Strictness        | Configurable     | Strict by default|
+-------------------+------------------+------------------+
| Ecosystem         | Mature           | Growing          |
+-------------------+------------------+------------------+
| IDE Support       | Excellent        | Good             |
+-------------------+------------------+------------------+
| Learning Curve    | Easy             | Moderate         |
+-------------------+------------------+------------------+

Migration Plan
--------------

When we migrate to Pyre:

1. Install Pyre alongside mypy
2. Run both checkers in parallel
3. Fix Pyre-specific errors
4. Switch CI to Pyre
5. Eventually remove mypy

This is a future consideration and not currently planned.

Best Practices Summary
======================

**Do:**

* Add type hints to all functions
* Use ``Optional[T]`` for nullable values
* Use ``Union`` for multiple types
* Define type aliases for complex types
* Use ``TypedDict`` for structured dicts
* Run mypy before committing
* Fix type errors, don't ignore them

**Don't:**

* Use ``Any`` unless absolutely necessary
* Ignore type errors without explanation
* Skip type hints on public APIs
* Use bare ``# type: ignore`` comments
* Disable mypy checks for entire files

Resources
=========

* Mypy documentation: https://mypy.readthedocs.io/
* Python typing docs: https://docs.python.org/3/library/typing.html
* Type hints cheat sheet: https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html
* PEP 484 (Type Hints): https://www.python.org/dev/peps/pep-0484/
* Pyre documentation: https://pyre-check.org/ (future reference)

See Also
========

* :doc:`coding_standards` - General coding standards
* :doc:`design_decisions/index` - Architecture decisions

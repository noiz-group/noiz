.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

==================
Pre-commit Hooks
==================

Noiz uses pre-commit hooks to automatically enforce code quality standards before commits.
This document describes the configured hooks and how to use them.

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

Pre-commit hooks run automatically when you commit code, catching issues before they
enter the repository. This ensures consistent code quality across all contributors.

**Benefits:**

* Catch issues early (before code review)
* Enforce consistent formatting
* Prevent common mistakes
* Reduce reviewer burden
* Maintain code quality standards

Installation
============

Install Pre-commit
------------------

.. code-block:: bash

    # Pre-commit is included in dev dependencies
    uv sync --all-groups

    # Or install separately
    pip install pre-commit

Install Hooks
-------------

After installing pre-commit, install the Git hooks:

.. code-block:: bash

    # Install hooks into .git/hooks/
    pre-commit install

    # Verify installation
    pre-commit --version

This creates a ``.git/hooks/pre-commit`` script that runs automatically on ``git commit``.

Configured Hooks
================

The hooks are configured in ``.pre-commit-config.yaml`` at the project root.

1. Trailing Whitespace
----------------------

**Hook**: ``trailing-whitespace``

**Purpose**: Removes trailing whitespace from all files

**When it runs**: On every commit

**What it does**:

* Strips whitespace at end of lines
* Prevents diff noise from whitespace changes
* Keeps repository clean

**Example:**

.. code-block:: python

    # Before
    def process_data():␣␣␣
        return result␣

    # After (automatically fixed)
    def process_data():
        return result

2. YAML Validation
------------------

**Hook**: ``check-yaml``

**Purpose**: Validates YAML syntax

**When it runs**: On YAML file commits

**What it does**:

* Checks for syntax errors in YAML files
* Ensures YAML can be parsed
* Prevents CI/CD failures from invalid YAML

**Files checked**:

* ``.gitlab-ci.yml``
* ``docker-compose.yml``
* ``.pre-commit-config.yaml``
* Any ``.yaml`` or ``.yml`` files

**Options**: ``--unsafe`` flag allows custom YAML tags

3. TOML Validation
------------------

**Hook**: ``check-toml``

**Purpose**: Validates TOML syntax

**When it runs**: On TOML file commits

**What it does**:

* Checks for syntax errors in TOML files
* Ensures TOML can be parsed
* Prevents packaging failures

**Files checked**:

* ``pyproject.toml``
* Any ``.toml`` files

4. End of File Fixer
--------------------

**Hook**: ``end-of-file-fixer``

**Purpose**: Ensures files end with a newline

**When it runs**: On every commit

**What it does**:

* Adds newline at end of files if missing
* Follows POSIX standard for text files
* Prevents issues with some tools

**Example:**

.. code-block:: python

    # Before
    def process_data():
        return result[NO NEWLINE]

    # After (automatically fixed)
    def process_data():
        return result
    [NEWLINE]

5. Ruff Linter
--------------

**Hook**: ``ruff-check``

**Purpose**: Lint Python code and auto-fix issues

**When it runs**: On Python file commits

**What it does**:

* Checks for code style violations
* Detects common bugs and anti-patterns
* Auto-fixes many issues with ``--fix`` flag
* Replaces flake8, isort, and pyupgrade

**Configuration**: ``ruff.toml`` in project root

**What it checks**:

* Code style (PEP 8)
* Import ordering
* Unused imports/variables
* Syntax errors
* Common bugs

**Example:**

.. code-block:: python

    # Before
    import sys
    import os
    from typing import List
    unused_variable = 5

    # After (automatically fixed)
    import os
    import sys
    from typing import List

6. Ruff Formatter
-----------------

**Hook**: ``ruff-format``

**Purpose**: Format Python code consistently

**When it runs**: On Python file commits

**What it does**:

* Formats code to consistent style
* Handles line length (119 chars)
* Manages quotes, spacing, indentation
* Replaces black and autopep8

**Configuration**: ``ruff.toml`` in project root

**Example:**

.. code-block:: python

    # Before
    def calculate_energy(  data,  sampling_rate ):
        result=np.sum(data**2)/sampling_rate
        return result

    # After (automatically formatted)
    def calculate_energy(data, sampling_rate):
        result = np.sum(data**2) / sampling_rate
        return result

7. Documentation Linter (doc8)
-------------------------------

**Hook**: ``doc8``

**Purpose**: Lint reStructuredText documentation

**When it runs**: On RST file commits

**What it does**:

* Checks RST syntax
* Validates documentation formatting
* Ensures docs build correctly
* Enforces line length in docs

**Files checked**:

* ``docs/**/*.rst``
* ``README.rst`` (if it exists)

**What it validates**:

* RST syntax correctness
* Line length (default 79, configurable)
* Trailing whitespace
* Blank lines
* Indentation

8. Mypy Type Checker (Disabled)
--------------------------------

**Status**: Currently disabled in configuration

**Hook**: ``mypy`` (commented out)

**Purpose**: Static type checking

**Why disabled**:

* Currently run separately in CI
* Can be slow on pre-commit
* Still being rolled out across codebase

**To enable**:

Uncomment in ``.pre-commit-config.yaml``:

.. code-block:: yaml

    -   repo: https://github.com/pre-commit/mirrors-mypy
        rev: v1.16.0
        hooks:
        -   id: mypy
            additional_dependencies: [sqlalchemy-stubs~=0.3, pydantic~=1.7.2]
            args:
                - --ignore-missing-imports
            exclude: tests/

**Note**: Mypy is still run manually and in CI:

.. code-block:: bash

    mypy src/noiz

Usage
=====

Automatic Usage
---------------

Hooks run automatically on ``git commit``:

.. code-block:: bash

    # Make changes
    vim src/noiz/processing/datachunk.py

    # Stage changes
    git add src/noiz/processing/datachunk.py

    # Commit (hooks run automatically)
    git commit -m "feat: add datachunk processing"

    # If hooks fail, commit is aborted
    # Fix issues and commit again

**Output example:**

.. code-block:: text

    Trim Trailing Whitespace...................Passed
    Check Yaml.................................Passed
    Check Toml.................................Passed
    Fix End of Files...........................Passed
    ruff-check.................................Failed
    - hook id: ruff-check
    - exit code: 1

    src/noiz/processing/datachunk.py:45:1: E402 Module level import not at top of file
    src/noiz/processing/datachunk.py:120:80: E501 Line too long (125 > 119 characters)

    2 files would be reformatted, 0 files already formatted.

Manual Usage
------------

Run hooks manually without committing:

.. code-block:: bash

    # Run on all files
    pre-commit run --all-files

    # Run on specific files
    pre-commit run --files src/noiz/processing/datachunk.py

    # Run specific hook
    pre-commit run ruff-check --all-files

    # Run with verbose output
    pre-commit run --all-files --verbose

Bypass Hooks (Not Recommended)
-------------------------------

To bypass hooks (use sparingly):

.. code-block:: bash

    # Skip all hooks
    git commit --no-verify -m "WIP: temporary commit"

    # Or use SKIP environment variable for specific hooks
    SKIP=ruff-check git commit -m "Skip ruff temporarily"

**Warning**: Only bypass hooks for WIP commits or emergencies.
CI will still run checks and may fail the pipeline.

Updating Hooks
==============

Update Hook Versions
--------------------

Hooks are versioned in ``.pre-commit-config.yaml``:

.. code-block:: yaml

    repos:
        - repo: https://github.com/astral-sh/ruff-pre-commit
          rev: v0.11.13  # Version pinned here

To update all hooks to latest versions:

.. code-block:: bash

    # Check for updates
    pre-commit autoupdate

    # This updates .pre-commit-config.yaml

    # Test with new versions
    pre-commit run --all-files

    # Commit the updated config
    git add .pre-commit-config.yaml
    git commit -m "chore: update pre-commit hook versions"

Auto-fix vs Manual Fix
=======================

Auto-fixable Issues
-------------------

These hooks automatically fix issues:

* ``trailing-whitespace`` - Removes trailing spaces
* ``end-of-file-fixer`` - Adds final newline
* ``ruff-check --fix`` - Fixes many code issues
* ``ruff-format`` - Reformats code

**Workflow:**

1. Commit triggers hooks
2. Hooks auto-fix issues
3. You need to stage fixed files
4. Commit again

.. code-block:: bash

    # First commit attempt
    $ git commit -m "feat: add function"
    Trim Trailing Whitespace...................Failed
    - files were modified by this hook

    # Stage the auto-fixed files
    $ git add -u

    # Commit again
    $ git commit -m "feat: add function"
    Trim Trailing Whitespace...................Passed
    # All hooks passed, commit successful!

Manual Fix Required
-------------------

These hooks only report errors:

* ``check-yaml`` - Reports YAML syntax errors
* ``check-toml`` - Reports TOML syntax errors
* ``doc8`` - Reports RST formatting issues

**Workflow:**

1. Hook reports error
2. Manually fix the issue
3. Stage changes
4. Commit again

Troubleshooting
===============

Hooks Not Running
-----------------

**Problem**: Hooks don't run on commit

**Solutions:**

.. code-block:: bash

    # Re-install hooks
    pre-commit install

    # Check if installed
    ls -la .git/hooks/pre-commit

    # Should see a pre-commit script

Hook Fails with "command not found"
------------------------------------

**Problem**: Hook can't find a command

**Solution:**

.. code-block:: bash

    # Clean hook cache
    pre-commit clean

    # Reinstall
    pre-commit install

    # Try again
    pre-commit run --all-files

Slow Hook Performance
---------------------

**Problem**: Hooks take too long to run

**Solutions:**

.. code-block:: bash

    # Run hooks only on changed files (default)
    git commit  # Fast

    # Skip slow hooks for WIP commits
    SKIP=mypy git commit -m "WIP"

    # Or configure specific files/paths to skip in config

Hook Conflicts with IDE
-----------------------

**Problem**: IDE auto-saves conflict with hooks

**Solution**:

* Configure IDE to format on save using same tools (ruff)
* Or disable IDE auto-format and rely on hooks
* See :doc:`environment_setup` for IDE configuration

CI vs Pre-commit
================

Pre-commit hooks are a **first line of defense**. CI runs additional checks:

+------------------+-------------------+------------------+
| Check            | Pre-commit        | CI Pipeline      |
+==================+===================+==================+
| ruff-check       | Yes               | Yes              |
+------------------+-------------------+------------------+
| ruff-format      | Yes               | Yes              |
+------------------+-------------------+------------------+
| mypy             | No (optional)     | Yes              |
+------------------+-------------------+------------------+
| pytest           | No                | Yes              |
+------------------+-------------------+------------------+
| doc8             | Yes               | Yes              |
+------------------+-------------------+------------------+
| Coverage         | No                | Yes              |
+------------------+-------------------+------------------+

**Strategy**: Pre-commit catches quick issues, CI runs comprehensive tests.

Best Practices
==============

**Do:**

* Keep hooks installed and updated
* Let hooks auto-fix when possible
* Fix reported issues before committing
* Run ``pre-commit run --all-files`` before pushing
* Update hook versions periodically

**Don't:**

* Bypass hooks without good reason
* Ignore hook failures
* Disable hooks permanently
* Commit ``--no-verify`` to main branch
* Fight the auto-formatter

Configuration
=============

Hook Configuration File
-----------------------

Location: ``.pre-commit-config.yaml``

**Example configuration:**

.. code-block:: yaml

    repos:
        - repo: https://github.com/astral-sh/ruff-pre-commit
          rev: v0.11.13
          hooks:
            - id: ruff-check
              args: [--fix]
            - id: ruff-format

**Customize**:

* Add new hooks
* Change hook versions
* Modify arguments
* Exclude files/paths

Adding New Hooks
----------------

To add a new hook:

1. Find the hook repository
2. Add to ``.pre-commit-config.yaml``
3. Install: ``pre-commit install``
4. Test: ``pre-commit run --all-files``
5. Commit the config change

**Example - adding prettier for JSON:**

.. code-block:: yaml

    repos:
        # ... existing hooks ...

        - repo: https://github.com/pre-commit/mirrors-prettier
          rev: v3.0.0
          hooks:
            - id: prettier
              types_or: [json, yaml]

Excluding Files
---------------

Exclude files from specific hooks:

.. code-block:: yaml

    repos:
        - repo: https://github.com/astral-sh/ruff-pre-commit
          rev: v0.11.13
          hooks:
            - id: ruff-check
              exclude: ^migrations/|^tests/fixtures/

**Patterns:**

* ``^migrations/`` - Exclude migrations directory
* ``.*_generated\.py$`` - Exclude generated files
* ``tests/fixtures/`` - Exclude test fixtures

Resources
=========

* Pre-commit documentation: https://pre-commit.com/
* Ruff documentation: https://docs.astral.sh/ruff/
* Available hooks: https://pre-commit.com/hooks.html
* Hook examples: https://github.com/pre-commit/pre-commit-hooks

See Also
========

* :doc:`coding_standards` - Coding conventions enforced by hooks
* :doc:`type_checking` - Type checking (can be added to hooks)
* :doc:`environment_setup` - Development environment setup

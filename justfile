timestamp := `date +%s`
system_tests_artifacts_dir := justfile_directory() / "tmp-system-tests-artifacts"

_default:
    just --list

[group('repo')]
submodule cmd="":
    #! /bin/bash
    set -euf -o pipefail
    if [[ "{{cmd}}" == "pull" ]]
    then
        git submodule foreach git pull origin main
    elif [[ "{{cmd}}" == "update" ]]
    then
        git submodule init
        git submodule update
    else
        echo "The command {{cmd}} does not exist."
    fi

# Cleanup artifacts after system tests
[group('testing')]
clean_after_tests:
    rm -rf {{system_tests_artifacts_dir}}

# Run system tests on sqlite without cleanup (use --parallel to enable parallel mode)
[group('testing')]
run_system_tests parallel="":
    #! /usr/bin/env bash
    set -e

    # Unset any PostgreSQL-related environment variables that might interfere
    unset DATABASE_URL
    unset POSTGRES_HOST
    unset POSTGRES_PORT
    unset POSTGRES_USER
    unset POSTGRES_PASSWORD
    unset POSTGRES_DB

    # Create main artifacts directory
    mkdir -p "{{system_tests_artifacts_dir}}"

    # Set SQLite configuration for system tests
    export NOIZ_DATABASE_BACKEND=sqlite
    export NOIZ_DATABASE_URL="sqlite:///{{system_tests_artifacts_dir}}/system_test_database_{{timestamp}}.db"
    export NOIZ_PROCESSED_DATA_DIR="{{system_tests_artifacts_dir}}/system_test_processed_data_dir_{{timestamp}}/"
    export NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex
    export SQLALCHEMY_WARN_20=1

    # Enable parallel mode if requested
    if [[ "{{parallel}}" == "--parallel" ]]; then
        export NOIZ_RUN_SYSTEM_TESTS_PARALLEL=True
        echo "Running system tests in PARALLEL mode"
    else
        echo "Running system tests in SEQUENTIAL mode"
    fi

    mkdir -p "$NOIZ_PROCESSED_DATA_DIR"

    # Run database migrations
    uv run noiz db upgrade

    # Run system tests with CLI marker
    uv run pytest --runcli

# Run unit tests
[group('testing')]
unit_tests:
    export SQLALCHEMY_WARN_20=1
    uv run pytest --cov=noiz

# Run system tests and cleanup afterwards (use --parallel to enable parallel mode)
[group('testing')]
system_tests parallel="": (run_system_tests parallel) clean_after_tests

# Sync dependencies with the local venv
[group('development')]
sync:
    uv sync --all-groups

# Run mypy type checking
[group('lint')]
mypy:
    uv run mypy --install-types --non-interactive src/noiz

# Check ruff linting and auto-fix issues
[group('lint')]
ruff_check:
    uv run ruff check --unsafe-fixes --fix .

# Check ruff linting without fixing (for CI)
[group('lint-ci')]
ruff_check_ci:
    uv run ruff check --output-format=gitlab > code-quality-report.json

# Check ruff format without modifying files (for CI)
[group('lint-ci')]
ruff_format_check:
    uv run ruff format --diff .

# Run ruff formatter on all files
[group('lint')]
ruff_format:
    uv run ruff format .

# Run all ruff checks on all files
[group('lint')]
ruff: ruff_check ruff_format

# Documentation build targets

# Build HTML documentation
[group('docs')]
docs:
    uv run sphinx-build -M html docs/ docs/_build

# Build documentation with specific target (html, latexpdf, pdf, etc.)
[group('docs')]
docs-build target="html":
    uv run sphinx-build -M {{target}} docs/ docs/_build

# Clean documentation build artifacts
[group('docs')]
docs-clean:
    rm -rf docs/_build

# Lint documentation with doc8
[group('lint')]
lint_docs:
    uv run doc8 docs/content

# Build and open documentation in browser (macOS)
[group('docs')]
docs-open: docs
    open docs/_build/html/index.html

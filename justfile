timestamp := `date +%s`
env_file_system_test := "env_system_tests"

_default:
    just --list

clean_after_tests:
    rm -rf system_test_processed_data_dir_*

prepare_dotenv:
    #! /usr/bin/env bash
    echo POSTGRES_HOST=localhost >> {{env_file_system_test}}
    echo POSTGRES_PORT="5432" >> {{env_file_system_test}}
    echo POSTGRES_USER=noiztest >> {{env_file_system_test}}
    echo POSTGRES_PASSWORD=noiztest >> {{env_file_system_test}}
    echo POSTGRES_DB=noiztest_{{timestamp}} >> {{env_file_system_test}}
    echo MSEEDINDEX_EXECUTABLE=../mseedindex/mseedindex >> {{env_file_system_test}}
    echo SQLALCHEMY_WARN_20=1 >> {{env_file_system_test}}

    export PROCESSED_DATA_DIR="system_test_processed_data_dir_{{timestamp}}/"
    echo PROCESSED_DATA_DIR=$PROCESSED_DATA_DIR >> {{env_file_system_test}}

    mkdir $PROCESSED_DATA_DIR

run_system_tests: prepare_dotenv
    #! /usr/bin/env bash
    export $(grep -v '^#' {{env_file_system_test}} | xargs)

    python tests/system_tests/create_new_db.py

    python -m noiz db migrate
    python -m noiz db upgrade
    python -m pytest --runcli

    rm {{env_file_system_test}}

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

unit_tests:
    SQLALCHEMY_WARN_20=1 uv run pytest --cov=noiz

sync:
    uv sync --all-groups

mypy:
    uv run mypy --install-types --non-interactive src/noiz

# Check ruff linting and auto-fix issues
ruff_check:
    uv run ruff check --unsafe-fixes --fix .

# Check ruff linting without fixing (for CI)
ruff_check_ci:
    uv run ruff check --output-format=gitlab > code-quality-report.json

# Check ruff format without modifying files (for CI)
ruff_format_check:
    uv run ruff format --diff .

ruff_format:
    uv run ruff format .

ruff: ruff_check ruff_format

# Documentation build targets

# Build HTML documentation
docs:
    uv run sphinx-build -M html docs/ docs/_build

# Build documentation with specific target (html, latexpdf, pdf, etc.)
docs-build target="html":
    uv run sphinx-build -M {{target}} docs/ docs/_build

# Clean documentation build artifacts
docs-clean:
    rm -rf docs/_build

# Lint documentation with doc8
lint_docs:
    uv run doc8 docs/content

# Build and open documentation in browser (macOS)
docs-open: docs
    open docs/_build/html/index.html

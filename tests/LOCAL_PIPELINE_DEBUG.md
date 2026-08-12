# Local Pipeline Debug Guide

This document provides instructions for running the GitLab CI pipeline locally for debugging purposes.
It can be used manually or with an AI agent for interactive debugging.

## Quick Reference

| Pipeline Stage | Local Command |
|---------------|---------------|
| Unit Tests | `./tests/local_pipeline.sh unit_tests` |
| Unit Tests (CI env) | `./tests/local_pipeline.sh ci_unit_tests` |
| Type Check | `./tests/local_pipeline.sh mypy` |
| Ruff Check | `./tests/local_pipeline.sh ruff_check` |
| Ruff Format | `./tests/local_pipeline.sh ruff_format` |
| All Pre-Docker | `./tests/local_pipeline.sh pre_docker` |
| System Tests | `./tests/local_pipeline.sh system_tests` |
| Full Pipeline | `./tests/local_pipeline.sh all` |

## Prerequisites

- Docker and Docker Compose installed
- `uv` package manager (will be installed if missing)
- `rust-just` task runner (will be installed if missing)
- Git submodules initialized

## Pipeline Stages Overview

The GitLab CI pipeline consists of these stages (from `.gitlab-ci.yml`):

```
stages:
  - testing        # Unit tests
  - linting        # mypy, ruff check, ruff format
  - documentation  # Sphinx docs
  - image-building # Docker image creation
  - system-testing # CLI system tests with PostgreSQL
```

### Part 1: Pre-Dockerization (Local Environment)

These stages run without Docker containers:
- **testing**: Unit tests (`just unit_tests`)
- **linting**: Type checking and code formatting (`just mypy`, `just ruff_check`, `just ruff_format_check`)

### Part 2: Post-Dockerization (Docker Environment)

These stages require Docker:
- **system-testing**: Full CLI system tests with PostgreSQL database

---

## Detailed Instructions

### Part 1: Pre-Dockerization Tests

#### 1.1 Setup Local Environment

```bash
# Install uv if not present
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install rust-just
uv tool install rust-just

# Sync dependencies
uv sync --all-groups
```

#### 1.2 Run Unit Tests

```bash
# From .gitlab-ci.yml: test-noiz job
just unit_tests
```

#### 1.3 Run Type Checking (mypy)

```bash
# From .gitlab/templates/linting.yml: type-check job
just mypy
```

#### 1.4 Run Ruff Linting

```bash
# From .gitlab/templates/linting.yml: ruff_check job
uv run ruff check .
```

#### 1.5 Run Ruff Format Check

```bash
# From .gitlab/templates/linting.yml: ruff_format job
uv run ruff format --diff .
```

#### 1.6 Fix Ruff Issues (Auto-fix)

```bash
# Auto-fix linting issues
uv run ruff check --fix .

# Auto-format code
uv run ruff format .
```

---

### Part 2: Post-Dockerization Tests (System Tests)

#### 2.1 Start PostgreSQL Container

```bash
# Start fresh database (removes existing data)
cd tests/system_tests
docker compose down -v
docker compose up -d
cd ../..

# Wait for PostgreSQL to be ready
sleep 3

# Enable required PostgreSQL extensions
docker exec system_tests-postgres-1 psql -U noiztest -d noiztest -c "CREATE EXTENSION IF NOT EXISTS hstore; CREATE EXTENSION IF NOT EXISTS btree_gist;"
```

#### 2.2 Initialize Git Submodule (Test Dataset)

```bash
git submodule init
git submodule update
```

#### 2.3 Run System Tests

```bash
# Create processed data directory
mkdir -p /tmp/processed-data-dir

# Run system tests in Docker container
# Variables from .gitlab-ci.yml: .system_test_parent
docker run --rm -it \
  --network system_tests_default \
  -v $(pwd):/app \
  -v /tmp/processed-data-dir:/processed-data-dir \
  -w /app \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_USER=noiztest \
  -e POSTGRES_PASSWORD=noiztest \
  -e POSTGRES_DB=noiztest \
  -e PROCESSED_DATA_DIR=/processed-data-dir \
  registry.gitlab.com/noiz-group/noiz:latest \
  bash -c "
    uv sync && \
    uv run flask db upgrade && \
    SQLALCHEMY_WARN_20=1 uv run pytest --runcli -v
  "
```

#### 2.4 Cleanup

```bash
# Stop and remove containers
cd tests/system_tests
docker compose down -v
cd ../..

# Clean processed data
rm -rf /tmp/processed-data-dir
```

---

## Automated Script

Save the following as `tests/local_pipeline.sh`:

```bash
#!/bin/bash
# Local Pipeline Runner for noiz
# Usage: ./scripts/local_pipeline.sh [stage]
# Stages: unit_tests, mypy, ruff_check, ruff_format, pre_docker, system_tests, all

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# ============================================================================
# Configuration from .gitlab-ci.yml
# ============================================================================
DOCKER_IMAGE="registry.gitlab.com/noiz-group/noiz:latest"
POSTGRES_IMAGE="registry.gitlab.com/noiz-group/noiz:postgres"
POSTGRES_HOST="postgres"
POSTGRES_PORT="5432"
POSTGRES_USER="noiztest"
POSTGRES_PASSWORD="noiztest"
POSTGRES_DB="noiztest"
PROCESSED_DATA_DIR="/processed-data-dir"
LOCAL_PROCESSED_DATA_DIR="/tmp/processed-data-dir"

# ============================================================================
# Setup Functions
# ============================================================================
setup_local_env() {
    log_info "Setting up local environment..."
    
    # Check if uv is installed
    if ! command -v uv &> /dev/null; then
        log_info "Installing uv..."
        curl -LsSf https://astral.sh/uv/install.sh | sh
        export PATH="$HOME/.cargo/bin:$PATH"
    fi
    
    # Install rust-just
    uv tool install rust-just 2>/dev/null || true
    
    # Sync dependencies
    log_info "Syncing dependencies..."
    uv sync --all-groups
    
    log_success "Local environment ready"
}

setup_docker_env() {
    log_info "Setting up Docker environment..."
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    # Initialize git submodule
    log_info "Initializing git submodules..."
    git submodule init
    git submodule update
    
    # Start PostgreSQL
    log_info "Starting PostgreSQL container..."
    cd tests/system_tests
    docker compose down -v 2>/dev/null || true
    docker compose up -d
    cd "$PROJECT_ROOT"
    
    # Wait for PostgreSQL to be ready
    log_info "Waiting for PostgreSQL to be ready..."
    sleep 5
    
    # Enable extensions
    docker exec system_tests-postgres-1 psql -U $POSTGRES_USER -d $POSTGRES_DB \
        -c "CREATE EXTENSION IF NOT EXISTS hstore; CREATE EXTENSION IF NOT EXISTS btree_gist;" \
        2>/dev/null || true
    
    # Create processed data directory
    mkdir -p "$LOCAL_PROCESSED_DATA_DIR"
    
    log_success "Docker environment ready"
}

cleanup_docker() {
    log_info "Cleaning up Docker environment..."
    cd tests/system_tests
    docker compose down -v 2>/dev/null || true
    cd "$PROJECT_ROOT"
    rm -rf "$LOCAL_PROCESSED_DATA_DIR"
    log_success "Cleanup complete"
}

# ============================================================================
# Test Functions (from .gitlab-ci.yml and justfile)
# ============================================================================
run_unit_tests() {
    log_info "Running unit tests (from .gitlab-ci.yml: test-noiz)"
    setup_local_env
    
    # From justfile: unit_tests
    SQLALCHEMY_WARN_20=1 uv run pytest --cov=noiz tests/ --ignore=tests/system_tests
    
    log_success "Unit tests passed"
}

run_mypy() {
    log_info "Running type check (from .gitlab/templates/linting.yml: type-check)"
    setup_local_env
    
    # From justfile: mypy
    uv run mypy --install-types --non-interactive src/noiz
    
    log_success "Type check passed"
}

run_ruff_check() {
    log_info "Running ruff check (from .gitlab/templates/linting.yml: ruff_check)"
    setup_local_env
    
    uv run ruff check .
    
    log_success "Ruff check passed"
}

run_ruff_format() {
    log_info "Running ruff format check (from .gitlab/templates/linting.yml: ruff_format)"
    setup_local_env
    
    # From justfile: ruff_format_check
    uv run ruff format --diff .
    
    log_success "Ruff format check passed"
}

run_pre_docker() {
    log_info "Running all pre-dockerization tests..."
    run_unit_tests
    run_mypy
    run_ruff_check
    run_ruff_format
    log_success "All pre-dockerization tests passed"
}

run_system_tests() {
    log_info "Running system tests (from .gitlab-ci.yml: cli_system_tests)"
    setup_docker_env
    
    # Run tests in Docker container
    # Variables and commands from .gitlab-ci.yml: .system_test_parent and cli_system_tests
    docker run --rm -it \
        --network system_tests_default \
        -v "$(pwd)":/app \
        -v "$LOCAL_PROCESSED_DATA_DIR":"$PROCESSED_DATA_DIR" \
        -w /app \
        -e POSTGRES_HOST="$POSTGRES_HOST" \
        -e POSTGRES_PORT="$POSTGRES_PORT" \
        -e POSTGRES_USER="$POSTGRES_USER" \
        -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
        -e POSTGRES_DB="$POSTGRES_DB" \
        -e PROCESSED_DATA_DIR="$PROCESSED_DATA_DIR" \
        "$DOCKER_IMAGE" \
        bash -c "
            uv sync && \
            uv run flask db upgrade && \
            SQLALCHEMY_WARN_20=1 uv run pytest --runcli -v
        "
    
    cleanup_docker
    log_success "System tests passed"
}

run_all() {
    log_info "Running full pipeline..."
    run_pre_docker
    run_system_tests
    log_success "Full pipeline passed"
}

# ============================================================================
# Interactive Debug Mode
# ============================================================================
run_interactive() {
    log_info "Starting interactive debug shell..."
    setup_docker_env
    
    log_info "Entering Docker container. Run tests manually:"
    echo "  uv sync"
    echo "  uv run flask db upgrade"
    echo "  SQLALCHEMY_WARN_20=1 uv run pytest --runcli -k 'test_name' -v"
    echo ""
    echo "Exit the container when done. Cleanup will run automatically."
    
    docker run --rm -it \
        --network system_tests_default \
        -v "$(pwd)":/app \
        -v "$LOCAL_PROCESSED_DATA_DIR":"$PROCESSED_DATA_DIR" \
        -w /app \
        -e POSTGRES_HOST="$POSTGRES_HOST" \
        -e POSTGRES_PORT="$POSTGRES_PORT" \
        -e POSTGRES_USER="$POSTGRES_USER" \
        -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
        -e POSTGRES_DB="$POSTGRES_DB" \
        -e PROCESSED_DATA_DIR="$PROCESSED_DATA_DIR" \
        "$DOCKER_IMAGE" \
        bash
    
    cleanup_docker
}

# ============================================================================
# Main
# ============================================================================
show_help() {
    echo "Usage: $0 [stage]"
    echo ""
    echo "Stages:"
    echo "  unit_tests    - Run unit tests (CI: test-noiz)"
    echo "  mypy          - Run type checking (CI: type-check)"
    echo "  ruff_check    - Run ruff linting (CI: ruff_check)"
    echo "  ruff_format   - Run ruff format check (CI: ruff_format)"
    echo "  pre_docker    - Run all pre-dockerization tests"
    echo "  system_tests  - Run system tests with Docker (CI: cli_system_tests)"
    echo "  all           - Run full pipeline"
    echo "  interactive   - Start interactive debug shell in Docker"
    echo "  cleanup       - Clean up Docker environment"
    echo ""
    echo "Examples:"
    echo "  $0 pre_docker      # Run linting and unit tests"
    echo "  $0 system_tests    # Run system tests in Docker"
    echo "  $0 all             # Run everything"
    echo "  $0 interactive     # Debug interactively in Docker"
}

case "${1:-help}" in
    unit_tests)   run_unit_tests ;;
    mypy)         run_mypy ;;
    ruff_check)   run_ruff_check ;;
    ruff_format)  run_ruff_format ;;
    pre_docker)   run_pre_docker ;;
    system_tests) run_system_tests ;;
    all)          run_all ;;
    interactive)  run_interactive ;;
    cleanup)      cleanup_docker ;;
    help|--help|-h) show_help ;;
    *)
        log_error "Unknown stage: $1"
        show_help
        exit 1
        ;;
esac
```

---

## AI Agent Instructions

When using an AI agent to debug the pipeline:

### For Pre-Dockerization Issues

1. Ask the agent to run: `./scripts/local_pipeline.sh pre_docker`
2. If a specific stage fails, run that stage individually:
   - `./scripts/local_pipeline.sh mypy` for type errors
   - `./scripts/local_pipeline.sh ruff_check` for linting errors
   - `./scripts/local_pipeline.sh ruff_format` for formatting issues
3. Ask the agent to fix the reported issues
4. Re-run the failing stage to verify

### For System Test Issues

1. Ask the agent to run: `./scripts/local_pipeline.sh system_tests`
2. If tests fail, use interactive mode: `./scripts/local_pipeline.sh interactive`
3. Inside the container, run specific tests:
   ```bash
   uv sync
   uv run flask db upgrade
   SQLALCHEMY_WARN_20=1 uv run pytest --runcli -k 'test_name' -v
   ```
4. Ask the agent to investigate and fix failures
5. Exit and re-run full system tests

### Common Debug Commands (Inside Docker Container)

```bash
# Run a specific test
SQLALCHEMY_WARN_20=1 uv run pytest --runcli -k 'test_function_name' -v

# Run tests with full traceback
SQLALCHEMY_WARN_20=1 uv run pytest --runcli -k 'test_name' -v --tb=long

# Run tests and stop on first failure
SQLALCHEMY_WARN_20=1 uv run pytest --runcli -x -v

# Check database connectivity
uv run flask db current

# Reset database
uv run flask db downgrade base
uv run flask db upgrade
```

---

## Mapping to GitLab CI Jobs

| Local Command | GitLab CI Job | Source File |
|--------------|---------------|-------------|
| `unit_tests` | `test-noiz` | `.gitlab-ci.yml` |
| `ci_unit_tests` | `test-noiz` (exact match) | `.gitlab-ci.yml` |
| `mypy` | `type-check` | `.gitlab/templates/linting.yml` |
| `ruff_check` | `ruff_check` | `.gitlab/templates/linting.yml` |
| `ruff_format` | `ruff_format` | `.gitlab/templates/linting.yml` |
| `system_tests` | `cli_system_tests` | `.gitlab-ci.yml` |

### Environment Differences

| Aspect | GitLab CI (`test-noiz`) | Local (`unit_tests`) | Local (`ci_unit_tests`) |
|--------|------------------------|----------------------|-------------------------|
| **Image** | `ghcr.io/astral-sh/uv:python3.10-bookworm` | Host Python | Same as CI |
| **venv** | Fresh each run | Cached `.venv` | Fresh each run |
| **Best for** | Production | Quick iteration | Debugging CI failures |

---

## Troubleshooting

> **Note:** The `local_pipeline.sh` script now handles most of these issues automatically.
> These manual commands are provided for reference if needed.

### ModuleNotFoundError: No module named 'pkg_resources'

This error occurs when `setuptools>=82` is installed. Newer versions of setuptools removed `pkg_resources`,
but `obspy` still requires it. The fix is already in `pyproject.toml`:

```toml
dependencies = [
    "setuptools <82",  # Required for pkg_resources used by obspy
    ...
]
```

If you see this error, ensure the `setuptools <82` constraint is in place and recreate the venv:

```bash
sudo rm -rf .venv
uv sync --all-groups
```

### "Permission denied" on .venv

This can occur when the virtual environment was created by a different user or has broken symlinks.
The script now automatically detects and fixes this, but if needed manually:

```bash
# Fix ownership
sudo chown -R $USER:$USER .venv

# Or remove and let it recreate
rm -rf .venv  # or: sudo rm -rf .venv
```

### "Permission denied" on /tmp/processed-data-dir

Docker containers create files with root ownership. The script now uses `sudo` for cleanup,
but if needed manually:

```bash
sudo rm -rf /tmp/processed-data-dir
```

### Database table does not exist

```bash
# Inside Docker container
uv run flask db upgrade
```

### Missing git submodule data

```bash
git submodule init
git submodule update
```

### PostgreSQL extensions missing

```bash
docker exec system_tests-postgres-1 psql -U noiztest -d noiztest \
  -c "CREATE EXTENSION IF NOT EXISTS hstore; CREATE EXTENSION IF NOT EXISTS btree_gist;"
```

### Docker network not found

```bash
cd tests/system_tests
docker compose down -v
docker compose up -d
cd ../..
```

### Clean start (reset everything)

If you encounter persistent issues, run a complete cleanup:

```bash
# Clean Docker environment
./tests/local_pipeline.sh cleanup

# Remove virtual environment
sudo rm -rf .venv

# Remove any cached files
sudo rm -rf /tmp/processed-data-dir

# Start fresh
./tests/local_pipeline.sh pre_docker
```

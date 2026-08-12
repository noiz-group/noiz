#!/bin/bash
# Local Pipeline Runner for noiz
# Usage: ./scripts/local_pipeline.sh [stage]
# Stages: unit_tests, mypy, ruff_check, ruff_format, pre_docker, system_tests, all
#
# This script mirrors the GitLab CI pipeline for local testing and debugging.
# Configuration is derived from .gitlab-ci.yml and .gitlab/templates/*.yml

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

DOCKER_RUN_FLAGS=()

init_docker_run_flags() {
    if [ -t 0 ] && [ -t 1 ]; then
        DOCKER_RUN_FLAGS=(-it)
    else
        DOCKER_RUN_FLAGS=(-i)
    fi
}

# ============================================================================
# Configuration from .gitlab-ci.yml
# These values are extracted from the CI configuration to stay in sync
# ============================================================================
# CI image used for test-noiz job (pre-docker tests)
CI_IMAGE="ghcr.io/astral-sh/uv:python3.10-bookworm"
# Image used for system tests
DOCKER_IMAGE="registry.gitlab.com/noiz-group/noiz:latest"
POSTGRES_IMAGE="registry.gitlab.com/noiz-group/noiz:postgres"

# From .gitlab-ci.yml: .system_test_parent.variables
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
fix_venv_permissions() {
    # Fix .venv permissions if it exists and has ownership issues
    # This can happen after running Docker or when switching users
    if [ -d ".venv" ]; then
        # Check if we can write to .venv
        if ! touch .venv/.write_test 2>/dev/null; then
            log_warn ".venv has permission issues, fixing with sudo..."
            sudo chown -R "$USER:$USER" .venv 2>/dev/null || true
        else
            rm -f .venv/.write_test
        fi
        
        # Check for broken symlinks in .venv/bin
        if [ -d ".venv/bin" ]; then
            local broken_links=$(find .venv/bin -type l ! -exec test -e {} \; -print 2>/dev/null | head -1)
            if [ -n "$broken_links" ]; then
                log_warn ".venv has broken symlinks, removing for fresh install..."
                rm -rf .venv 2>/dev/null || sudo rm -rf .venv 2>/dev/null || true
            fi
        fi
    fi
}

setup_local_env() {
    log_info "Setting up local environment..."
    
    # Fix any .venv permission issues first
    fix_venv_permissions
    
    # Check if uv is installed
    if ! command -v uv &> /dev/null; then
        log_info "Installing uv..."
        curl -LsSf https://astral.sh/uv/install.sh | sh
        export PATH="$HOME/.cargo/bin:$PATH"
    fi
    
    # Install rust-just (from .gitlab-ci.yml: before_script)
    uv tool install rust-just 2>/dev/null || true
    
    # Sync dependencies (from .gitlab-ci.yml: before_script)
    log_info "Syncing dependencies..."
    uv sync --all-groups
    
    log_success "Local environment ready"
}

setup_docker_env() {
    local services=("$@")
    if [ ${#services[@]} -eq 0 ]; then
        services=(postgres)
    fi

    log_info "Setting up Docker environment..."
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    # Initialize git submodule (from .gitlab-ci.yml: GIT_SUBMODULE_STRATEGY: recursive)
    log_info "Initializing git submodules..."
    git submodule init
    git submodule update
    
    # Start PostgreSQL (mirrors the postgres service in CI)
    log_info "Starting Docker services: ${services[*]}"
    cd tests/system_tests
    docker compose down -v 2>/dev/null || true
    docker compose up -d "${services[@]}"
    cd "$PROJECT_ROOT"
    
    # Wait for PostgreSQL to be ready
    log_info "Waiting for PostgreSQL to be ready..."
    sleep 5
    
    # Enable extensions (from db-init-scripts/init.sql)
    docker exec system_tests-postgres-1 psql -U $POSTGRES_USER -d $POSTGRES_DB \
        -c "CREATE EXTENSION IF NOT EXISTS hstore; CREATE EXTENSION IF NOT EXISTS btree_gist;" \
        2>/dev/null || true
    
    # Create processed data directory (from .gitlab-ci.yml: mkdir -p $PROCESSED_DATA_DIR)
    # First clean up any existing files with sudo (may have root ownership from Docker)
    sudo rm -rf "$LOCAL_PROCESSED_DATA_DIR" 2>/dev/null || rm -rf "$LOCAL_PROCESSED_DATA_DIR" 2>/dev/null || true
    mkdir -p "$LOCAL_PROCESSED_DATA_DIR"
    
    log_success "Docker environment ready"
}

cleanup_docker() {
    log_info "Cleaning up Docker environment..."
    cd tests/system_tests
    docker compose down -v 2>/dev/null || true
    cd "$PROJECT_ROOT"
    # Use sudo to clean files created by Docker containers (may have root ownership)
    sudo rm -rf "$LOCAL_PROCESSED_DATA_DIR" 2>/dev/null || rm -rf "$LOCAL_PROCESSED_DATA_DIR" 2>/dev/null || true
    log_success "Cleanup complete"
}

# ============================================================================
# Test Functions (mapped to .gitlab-ci.yml jobs)
# ============================================================================

# Maps to: .gitlab-ci.yml -> test-noiz
run_unit_tests() {
    log_info "Running unit tests (CI job: test-noiz)"
    setup_local_env
    
    # From justfile: unit_tests
    SQLALCHEMY_WARN_20=1 uv run pytest --cov=noiz tests/ --ignore=tests/system_tests
    
    log_success "Unit tests passed"
}

# Maps to: .gitlab-ci.yml -> test-noiz (exact CI environment)
# Use this to debug CI failures that don't reproduce locally
run_ci_unit_tests() {
    log_info "Running unit tests in CI environment (CI job: test-noiz)"
    log_info "Using image: $CI_IMAGE"
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    # Run tests in the same Docker image used by GitLab CI
    docker run --rm "${DOCKER_RUN_FLAGS[@]}" \
        -v "$(pwd)":/app \
        -w /app \
        "$CI_IMAGE" \
        bash -c "
            uv tool install rust-just && \
            uv sync --all-groups && \
            just unit_tests
        "
    
    log_success "CI unit tests passed"
}

# Maps to: .gitlab/templates/linting.yml -> type-check
run_mypy() {
    log_info "Running type check (CI job: type-check)"
    setup_local_env
    
    # From justfile: mypy
    uv run mypy src/noiz
    
    log_success "Type check passed"
}

# Maps to: .gitlab/templates/linting.yml -> ruff_check
run_ruff_check() {
    log_info "Running ruff check (CI job: ruff_check)"
    setup_local_env
    
    uv run ruff check .
    
    log_success "Ruff check passed"
}

# Maps to: .gitlab/templates/linting.yml -> ruff_format
run_ruff_format() {
    log_info "Running ruff format check (CI job: ruff_format)"
    setup_local_env
    
    # From justfile: ruff_format_check
    uv run ruff format --diff .
    
    log_success "Ruff format check passed"
}

# Runs all pre-dockerization tests (stages: testing, linting)
run_pre_docker() {
    log_info "Running all pre-dockerization tests..."
    run_unit_tests
    run_mypy
    run_ruff_check
    run_ruff_format
    log_success "All pre-dockerization tests passed"
}

# Maps to: .gitlab-ci.yml -> cli_system_tests
run_system_tests() {
    log_info "Running system tests (CI job: cli_system_tests)"
    setup_docker_env postgres
    
    # Run tests in Docker container
    # Configuration from .gitlab-ci.yml: .system_test_parent and cli_system_tests
    docker run --rm "${DOCKER_RUN_FLAGS[@]}" \
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

# Runs full pipeline (all stages)
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
    setup_docker_env postgres adminer
    
    log_info "Entering Docker container. Run tests manually:"
    echo ""
    echo "  # Sync dependencies and apply migrations"
    echo "  uv sync"
    echo "  uv run flask db upgrade"
    echo ""
    echo "  # Run all system tests"
    echo "  SQLALCHEMY_WARN_20=1 uv run pytest --runcli -v"
    echo ""
    echo "  # Run specific test"
    echo "  SQLALCHEMY_WARN_20=1 uv run pytest --runcli -k 'test_name' -v"
    echo ""
    echo "  # Run with full traceback"
    echo "  SQLALCHEMY_WARN_20=1 uv run pytest --runcli -k 'test_name' -v --tb=long"
    echo ""
    echo "Exit the container when done. Cleanup will run automatically."
    echo ""
    
    docker run --rm "${DOCKER_RUN_FLAGS[@]}" \
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

# Run a specific test by name (for debugging)
run_single_test() {
    local test_name="$1"
    if [ -z "$test_name" ]; then
        log_error "Usage: $0 single_test <test_name>"
        exit 1
    fi
    
    log_info "Running single test: $test_name"
    setup_docker_env postgres
    
    docker run --rm "${DOCKER_RUN_FLAGS[@]}" \
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
            SQLALCHEMY_WARN_20=1 uv run pytest --runcli -k '$test_name' -v --tb=long
        "
    
    cleanup_docker
}

# ============================================================================
# Auto-fix Commands
# ============================================================================
run_fix() {
    log_info "Running auto-fix for linting issues..."
    setup_local_env
    
    log_info "Fixing ruff issues..."
    uv run ruff check --fix .
    
    log_info "Formatting code..."
    uv run ruff format .
    
    log_success "Auto-fix complete"
}

# ============================================================================
# Main
# ============================================================================
show_help() {
    echo "Local Pipeline Runner for noiz"
    echo ""
    echo "Usage: $0 [stage] [options]"
    echo ""
    echo "Pre-Dockerization Stages (run locally):"
    echo "  unit_tests    - Run unit tests (CI: test-noiz)"
    echo "  ci_unit_tests - Run unit tests in CI Docker image (exact CI match)"
    echo "  mypy          - Run type checking (CI: type-check)"
    echo "  ruff_check    - Run ruff linting (CI: ruff_check)"
    echo "  ruff_format   - Run ruff format check (CI: ruff_format)"
    echo "  pre_docker    - Run all pre-dockerization tests"
    echo "  fix           - Auto-fix ruff issues and format code"
    echo ""
    echo "Post-Dockerization Stages (run in Docker):"
    echo "  system_tests  - Run system tests with Docker (CI: cli_system_tests)"
    echo "  interactive   - Start interactive debug shell in Docker"
    echo "  single_test   - Run a single test: $0 single_test <test_name>"
    echo ""
    echo "Combined:"
    echo "  all           - Run full pipeline (pre_docker + system_tests)"
    echo ""
    echo "Utilities:"
    echo "  cleanup       - Clean up Docker environment"
    echo "  help          - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 pre_docker                    # Run linting and unit tests"
    echo "  $0 system_tests                  # Run system tests in Docker"
    echo "  $0 all                           # Run everything"
    echo "  $0 interactive                   # Debug interactively in Docker"
    echo "  $0 single_test test_run_stacking # Run specific test"
    echo "  $0 fix                           # Auto-fix linting issues"
    echo ""
    echo "Configuration sources:"
    echo "  - .gitlab-ci.yml"
    echo "  - .gitlab/templates/linting.yml"
    echo "  - justfile"
}

init_docker_run_flags

case "${1:-help}" in
    unit_tests)   run_unit_tests ;;
    ci_unit_tests) run_ci_unit_tests ;;
    mypy)         run_mypy ;;
    ruff_check)   run_ruff_check ;;
    ruff_format)  run_ruff_format ;;
    pre_docker)   run_pre_docker ;;
    system_tests) run_system_tests ;;
    all)          run_all ;;
    interactive)  run_interactive ;;
    single_test)  run_single_test "$2" ;;
    fix)          run_fix ;;
    cleanup)      cleanup_docker ;;
    help|--help|-h) show_help ;;
    *)
        log_error "Unknown stage: $1"
        show_help
        exit 1
        ;;
esac

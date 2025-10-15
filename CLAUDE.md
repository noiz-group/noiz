# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation Standards

**IMPORTANT: Do not use emojis in any documentation.** All documentation should be professional and emoji-free. This includes:
- RST files in `docs/`
- Code comments
- Docstrings
- Commit messages
- Design documents

### RST Formatting: One Sentence Per Line

**All RST documentation must follow "one sentence per line" formatting.**

This means:
- Each sentence starts on a new line
- Long sentences can be broken across multiple lines if needed
- Blank lines separate paragraphs
- Makes diffs cleaner and easier to review

Example:
```rst
This is the first sentence.
This is a very long sentence that needs to be broken across multiple lines
because it exceeds reasonable line length limits for readability.
This is the third sentence.

This is a new paragraph.
```

**Why**: This approach makes version control diffs much cleaner, easier to review changes, and simplifies collaborative editing.

### Design Documents and Implementation Plans

**ALL future implementation plans, design documents, and architectural proposals MUST be created as RST documentation** in `docs/content/development/design_documents/`.

**Location**: `docs/content/development/design_documents/`

**Existing design documents**:
- `refactoring_roadmap.rst` - Complete refactoring plan (13-17 weeks, Phases 0-5)
- `architecture.rst` - Code quality and architecture analysis (18-26 weeks roadmap)
- `s3_storage.rst` - S3-compatible storage implementation (4-week plan)
- `config_system.rst` - Transferrable configuration system (8-10 weeks)
- `plugin_architecture.rst` - Pluggable module system (16-20 weeks)
- `semantic_versioning.rst` - Conventional commits and automated changelog (1-2 weeks)
- `index.rst` - Design documents index with integration roadmap

**When creating new plans or proposals**:
1. Create a new `.rst` file in `docs/content/development/design_documents/`
2. Follow the existing document structure (problem statement, proposed solution, implementation plan, etc.)
3. Add the document to `design_documents/index.rst` toctree
4. Cross-reference with related documents using `:doc:` directives
5. Ensure it builds correctly with Sphinx

**Do NOT**:
- Create markdown files (`.md`) in the repository root for plans
- Create separate documentation outside the Sphinx system
- Duplicate information across multiple locations

**Why**: All plans must be in the official documentation so they can be:
- Properly versioned and tracked
- Cross-referenced between documents
- Built and validated by Sphinx
- Easily accessible to all contributors

## Project Overview

Noiz is an ambient seismic noise processing application built with Python, Flask, and PostgreSQL. It processes seismic data through a pipeline involving data ingestion, quality control, cross-correlation, stacking, beamforming, and Power Spectral Density (PPSD) calculations. The project uses SQLAlchemy for database operations and Dask for parallel processing.

## Development Setup

### Environment Variables

Required environment variables (see `src/noiz/settings.py`):

- **Database Connection** - Either provide:
  - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
  - OR `DATABASE_URL` (PostgreSQL connection URI)
- `PROCESSED_DATA_DIR` - Directory for processed data output (required)
- `MSEEDINDEX_EXECUTABLE` - Path to mseedindex executable (required)
- `FLASK_ENV` - Set to "development" or "production" (defaults to "development")

### Installing Dependencies

```bash
# Install dependencies using uv
uv sync --all-groups

# Or use just
just sync
```

### Database Migrations

```bash
# Run migrations using Flask-Migrate
uv run flask db upgrade
```

## Just Command Runner

**This project uses [just](https://github.com/casey/just) as a command runner** to provide consistent commands across local development and CI environments.

### Available Commands

Run `just --list` to see all available commands:

```bash
just --list
```

### Common Just Recipes

**Development tasks:**
```bash
just sync                # Install/sync dependencies
just unit_tests          # Run unit tests with coverage
just mypy                # Run type checking
```

**Code quality:**
```bash
just ruff                # Run ruff check and format (with fixes)
just ruff_check          # Run ruff linting with auto-fix
just ruff_format         # Format code with ruff
```

**CI-specific recipes:**
```bash
just ruff_check_ci       # Check linting without fixing (for CI)
just ruff_format_check   # Check formatting without modifying (for CI)
```

**Documentation:**
```bash
just docs                # Build HTML documentation
just docs-clean          # Clean documentation build artifacts
just docs-open           # Build and open docs in browser
just lint_docs           # Run doc8 linting on docs
```

**System tests:**
```bash
just run_system_tests    # Run full system test suite
```

### Why Just?

The project uses `just` to:
- Ensure consistent commands between local development and CI
- Simplify complex command invocations
- Provide discoverable, self-documenting commands
- Reduce "works on my machine" issues

### CI Integration

The GitLab CI pipeline uses the same `just` commands as local development. The CI configuration automatically installs `just` using `uv tool install rust-just` and then executes the same recipes you use locally.

## Running Tests

### Basic Test Commands

```bash
# Using just (recommended)
just unit_tests          # Run all tests with coverage

# Or using uv directly
uv run pytest --cov=noiz

# Run specific test file
uv run pytest tests/processing/test_configs.py

# Run tests with specific markers
uv run pytest --runcli  # Run CLI system tests
uv run pytest --runapi  # Run API system tests

# Run full system tests
just run_system_tests
```

### Test Structure

- Tests use pytest with custom markers (`@pytest.mark.cli`, `@pytest.mark.api`)
- System tests are skipped by default unless `--runcli` or `--runapi` flags are provided
- Fixtures defined in `conftest.py` at repository root

## Linting and Code Quality

```bash
# Using just (recommended)
just ruff                # Run check and format with fixes
just ruff_check          # Run linting with auto-fix
just ruff_format         # Format code
just mypy                # Run type checking
just lint_docs           # Lint documentation

# Or using uv directly
uv run ruff check .
uv run ruff check --fix .
uv run mypy src/noiz
uv run doc8 docs/content
```

Configuration in `ruff.toml` - line length is 119 characters.

## Building Documentation

```bash
# Using just (recommended)
just docs                # Build HTML documentation
just docs-clean          # Clean build artifacts
just docs-open           # Build and open in browser

# Or using uv directly
uv run sphinx-build -M html docs/ docs/_build
```

## CLI Usage

The main CLI is accessed via the `noiz` command (defined in `src/noiz/cli.py`). The CLI is organized into subgroups:

- `noiz configs` - Manage processing configurations (add datachunk params, crosscorrelation params, etc.)
- `noiz data` - Ingest raw data (seismic data, inventories, timespans, SOH data)
- `noiz processing` - Run processing tasks (datachunks, cross-correlations, beamforming, PPSD, QC, stacking)
- `noiz plot` - Generate plots (datachunk availability, beamforming, spectrograms, PSD, SOH data)
- `noiz export` - Export data (GPS SOH, cross-correlations)

### Common CLI Patterns

All processing commands support:
- `-sd/--startdate` and `-ed/--enddate` for time ranges (use ISO format or parseable strings)
- `-v/--verbose` for increased logging (can be repeated: `-vv`, `-vvv`)
- `--quiet` for minimal logging
- `--parallel/--no_parallel` for parallel processing
- `-b/--batch_size` for controlling batch sizes

Example:
```bash
noiz processing prepare_datachunks -sd 2023-01-01 -ed 2023-01-31 -p 1 -b 1000 --parallel
```

## Architecture

### Source Structure (`src/noiz/`)

- **`models/`** - SQLAlchemy ORM models for all database tables
  - Core entities: Component, ComponentPair, Timespan, Datachunk
  - Processing results: CrosscorrelationCartesian, BeamformingResult, PPSDResult
  - Configuration: ProcessingParams (various types for different processing stages)
  - QC: QCOneResults, QCTwoResults

- **`processing/`** - Core processing algorithms and utilities
  - Signal processing, cross-correlation, beamforming implementations
  - `obspy_derived/` - Methods derived from ObsPy
  - File I/O, time utilities, path helpers

- **`api/`** - High-level API functions that orchestrate processing workflows
  - Bridges CLI commands to processing logic
  - Handles database queries and parallel execution

- **`cli.py`** - Click-based CLI with command groups
- **`app.py`** - Flask application factory
- **`database.py`** - SQLAlchemy database setup and utilities
- **`settings.py`** - Environment-based configuration

### Processing Pipeline Flow

1. **Data Ingestion** (`noiz data add_seismic_data`, `add_inventory`)
2. **Timespan Generation** (`noiz data add_timespans`) - Creates time windows for processing
3. **Datachunk Preparation** (`noiz processing prepare_datachunks`) - Segments raw data
4. **QC One** (`noiz processing run_qcone`) - Quality control on individual components
5. **Datachunk Processing** (`noiz processing process_datachunks`) - Preprocessing/filtering
6. **Cross-correlation** (`noiz processing run_crosscorrelations_cartesian` or `run_crosscorrelations_cylindrical`)
7. **QC Two** (`noiz processing run_qctwo`) - Quality control on cross-correlations
8. **Stacking** (`noiz processing run_stacking`) - Time-domain stacking of cross-correlations
9. **Additional Analyses**: Beamforming, PPSD calculations

### Configuration System

Processing parameters are stored as TOML files and ingested into the database via CLI:
- Examples in `config_examples/` directory
- Each processing stage has its own params type (DatachunkParams, CrosscorrelationCartesianParams, etc.)
- Configurations are versioned in the database and referenced by ID

### Database Schema

- Flask-SQLAlchemy with PostgreSQL backend
- Migrations managed via Flask-Migrate (Alembic under the hood) in `migrations/` directory
- Models use mixins from `src/noiz/models/mixins.py` for common patterns
- Heavy use of relationships and foreign keys for data lineage tracking

## Docker Support

Three Docker images defined in `docker/`:
- `noiz-image/` - Main application image
- `postgres-image/` - PostgreSQL with PostGIS extensions
- `unittest-image/` - Image for running tests

## CI/CD

GitLab CI configuration in `.gitlab-ci.yml`:
- Stages: testing, linting, documentation, image-building, system-testing
- Uses `ghcr.io/astral-sh/uv:python3.10-bookworm` as base image
- Docker images built for linux/amd64 platform
- Coverage reports generated via pytest-cov
- **Uses `just` command runner** - CI automatically installs `just` via `uv tool install rust-just` and executes the same recipes used in local development

### CI Command Mapping

The CI uses the following `just` commands:
- `just unit_tests` - Run unit tests with coverage
- `just mypy` - Run type checking
- `just ruff_check_ci` - Check linting (produces GitLab code quality report)
- `just ruff_format_check` - Check formatting without modifying files
- `just lint_docs` - Lint documentation with doc8
- `just docs` - Build HTML documentation
- `just sync` - Install dependencies

## Important Notes

- Python version: 3.10 (strictly: >=3.10, <3.11)
- The codebase uses `# mypy: ignore-errors` in CLI due to complex type interactions
- ObsPy is a core dependency - methods in `src/noiz/processing/obspy_derived/` are derived from ObsPy
- Parallel processing uses Dask with configurable workers
- File paths in processing use a structured hierarchy based on processing parameters and timespans

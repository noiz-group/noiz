<!--
Sync Impact Report:
Version: 0.0.0 → 1.0.0
Rationale: Initial constitution ratification for Noiz project
Added sections: All core principles and governance structure
Modified principles: N/A (initial version)
Removed sections: None
Templates requiring updates:
  ✅ .specify/templates/plan-template.md - Will align on first use
  ✅ .specify/templates/spec-template.md - Will align on first use
  ✅ .specify/templates/tasks-template.md - Will align on first use
Follow-up TODOs: None
-->

# Noiz Project Constitution

## Core Principles

### I. Scientific Correctness First

Every feature MUST preserve the scientific validity of seismic noise processing results.

- Algorithm implementations MUST be traceable to published methods or ObsPy derivations
- Data lineage MUST be tracked through the database for reproducibility
- Processing parameter changes MUST be versioned and recorded
- Quality control steps are mandatory, not optional

**Rationale**: Noiz processes scientific data where incorrect results can lead to flawed research conclusions. Correctness trumps convenience.

### II. Database-Driven Configuration

All processing configurations MUST be stored in the database, not in code or config files.

- TOML files are input format only, ingested via CLI to database
- Processing parameters are versioned database entities
- Configuration changes are tracked with IDs and timestamps
- No hardcoded processing parameters in application code

**Rationale**: Enables reproducibility, parameter comparison, and historical analysis of processing workflows.

### III. CLI-First Interface

All functionality MUST be accessible via the `noiz` CLI before any other interface.

- Every processing operation has a CLI command
- Commands follow consistent patterns: time ranges (`-sd/-ed`), verbosity (`-v`), parallelization (`--parallel`)
- Support both interactive and scripted usage
- Batch operations support `--batch_size` for memory management

**Rationale**: Scientific workflows require scripting and automation. CLI ensures composability and reproducibility.

### IV. Parallel Processing by Default

Processing tasks MUST support parallel execution where scientifically valid.

- Use Dask for parallel orchestration
- Respect batch sizes to prevent memory exhaustion
- Maintain data integrity across parallel workers
- Provide `--no_parallel` escape hatch for debugging

**Rationale**: Seismic datasets are large. Sequential processing is impractical for production workflows.

### V. Type Safety and Quality Checks

Code MUST pass type checking and linting before merge.

- `mypy` type checking required (exceptions documented with `# mypy: ignore-errors` and justification)
- `ruff` linting and formatting enforced (line length: 119 characters)
- `doc8` for documentation quality
- All checks automated via `just` commands and CI

**Rationale**: Python's dynamic nature requires discipline. Type hints catch errors before production.

### VI. Test Coverage for Scientific Code

Processing algorithms MUST have test coverage.

- Unit tests for signal processing functions
- Integration tests marked with `@pytest.mark.cli` or `@pytest.mark.api`
- System tests optional by default (require `--runcli`/`--runapi` flags)
- Coverage reports generated in CI

**Rationale**: Untested scientific code is untrustworthy code. Tests document expected behavior.

### VII. Documentation as Code

Documentation MUST be maintained in Sphinx and built in CI.

- Use reStructuredText (RST) format exclusively
- Design documents in `docs/content/development/design_documents/`
- Cross-reference with `:doc:` directives
- Validate builds with `just docs`

**Rationale**: Scattered documentation becomes stale. Sphinx enforces structure and enables navigation.

## Development Workflow

### Code Quality Gates

All changes MUST pass these gates before merge:

1. `just unit_tests` - Tests pass with coverage
2. `just mypy` - Type checking passes
3. `just ruff_check_ci` - Linting passes without fixes needed
4. `just ruff_format_check` - Formatting is correct
5. `just lint_docs` - Documentation lints cleanly
6. `just docs` - Documentation builds without errors

These run automatically in GitLab CI. Use `just` locally for consistency with CI.

### Database Migration Discipline

Schema changes MUST follow Flask-Migrate workflow:

1. Create migration: `uv run flask db revision --autogenerate -m "description"`
2. Review generated migration for correctness
3. Test migration: `uv run flask db upgrade` on development database
4. Test rollback: `uv run flask db downgrade` to verify reversibility
5. Commit migration file with code changes

**Never** modify committed migrations. Create new migrations to fix issues.

### Dependency Management

Dependencies MUST be managed via `uv`:

- Add dependencies: `uv add package-name`
- Add dev dependencies: `uv add --dev package-name`
- Sync environment: `just sync` or `uv sync --all-groups`
- Python version: `>=3.10, <3.11` (strictly enforced)

### Commit Discipline

Commits MUST be small, incremental, and well-documented:

- **One logical change per commit** - Single concept, feature, or fix
- **Descriptive commit messages** - Clear subject line (50 chars), detailed body explaining why
- **Incremental progress** - Commit after each completed task or logical unit
- **Reviewable diffs** - Small commits are easier to review and debug
- **Atomic changes** - Each commit should leave codebase in working state

**Good commit examples**:
- "feat(models): Add ulid field to DatachunkFile"
- "fix(api): Generate ULID before object creation in workers"
- "test(parallel): Add integration test for resume capability"

**Bad commit examples**:
- "WIP" (no description)
- "fixes" (too vague)
- "Add ULID to everything" (too broad, should be split)

**Rationale**: Small, well-named commits enable bisect debugging, easier code review, and clear project history. Scientific code requires traceable changes for reproducibility.

## Constraints

### Technology Stack

- **Language**: Python 3.10 (no 3.11+ until ObsPy supports it)
- **Web Framework**: Flask (for future API)
- **Database**: PostgreSQL with PostGIS extensions
- **ORM**: SQLAlchemy via Flask-SQLAlchemy
- **CLI**: Click framework
- **Parallel Processing**: Dask
- **Scientific Computing**: ObsPy, NumPy, SciPy
- **Testing**: pytest with coverage
- **Documentation**: Sphinx with RST

Stack changes require architecture review and documented rationale.

### Performance Standards

- Batch processing MUST support configurable batch sizes for memory management
- Database queries MUST use SQLAlchemy relationships to avoid N+1 queries
- Large datasets MUST stream rather than load entirely into memory
- Parallel processing MUST be opt-out (`--no_parallel`), not opt-in

## Governance

### Amendment Process

This constitution supersedes informal practices.

To amend:

1. Propose changes via merge request to `.specify/memory/constitution.md`
2. Document rationale and impact analysis
3. Update version following semantic versioning:
   - **MAJOR**: Principle removals or backward-incompatible governance changes
   - **MINOR**: New principles or materially expanded guidance
   - **PATCH**: Clarifications, wording fixes, non-semantic refinements
4. Update `LAST_AMENDED_DATE` to merge date
5. Propagate changes to dependent templates and documentation

### Compliance Review

- All spec-kit plans MUST pass constitution compliance checks
- Pull request reviews SHOULD verify adherence to principles
- Deviations MUST be documented and justified in commit messages
- Chronic non-compliance indicates principle needs revision, not elimination

### Version History

**Version**: 1.0.0 | **Ratified**: 2025-10-16 | **Last Amended**: 2025-10-16

# Implementation Plan: Configuration System Refactor

**Branch**: `007-config-system-refactor` | **Date**: 2025-10-25 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/007-config-system-refactor/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Consolidate scattered environment variable loading across `noiz.settings` and `noiz.database` into a single, type-safe configuration system using pydantic-settings. All environment variables will use `NOIZ_` prefix for namespace isolation. The system must preserve Flask app factory pattern compatibility, support both CLI and notebook usage patterns, validate configuration at startup with clear error messages, mask sensitive values in logs, and maintain backward compatibility during migration. Comprehensive user documentation with `.env` file guidance will be provided, and CI pipeline will be updated to use new configuration variables.

## Technical Context

**Language/Version**: Python 3.10 (>=3.10, <3.11 per project constraints)
**Primary Dependencies**: pydantic-settings (>=2.0,<3.0), Flask, Flask-SQLAlchemy, Flask-Migrate, loguru
**Storage**: Configuration stored as environment variables; application data in PostgreSQL/SQLite
**Testing**: pytest with coverage, system tests with `@pytest.mark.cli` and `@pytest.mark.api`
**Target Platform**: Linux/macOS (CLI tool and library)
**Project Type**: Single project (CLI + library with Flask integration)
**Performance Goals**: Configuration loading adds <100ms to startup time
**Constraints**: Must support dynamic config reload for Flask `create_app()` pattern; must preserve pytest fixture pattern for test isolation; must mask sensitive values in all output
**Scale/Scope**: Single configuration module replacing 2 current modules (noiz.settings, noiz.database config logic); ~10-15 environment variables; CI updates across 6 pipeline stages

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Core Principles Compliance

| Principle | Status | Notes |
|-----------|--------|-------|
| **I. Scientific Correctness First** | ✅ PASS | Config refactor does not affect algorithm implementations; data lineage tracking preserved; no changes to processing parameters or QC steps |
| **II. Database-Driven Configuration** | ✅ PASS | Refactor targets application-level config (env vars), NOT processing parameters. Processing configs remain in database as mandated |
| **III. CLI-First Interface** | ✅ PASS | All CLI commands will continue to work; config validation happens at CLI invocation; no new CLI patterns required |
| **IV. Parallel Processing by Default** | ✅ PASS | Config system does not affect Dask parallelization; all parallel processing preserved |
| **V. Type Safety and Quality Checks** | ✅ PASS | New config module will use Pydantic for type safety; mypy type checking enforced; ruff linting applies |
| **VI. Test Coverage for Scientific Code** | ✅ PASS | Config module will have unit tests; no scientific algorithms affected; pytest patterns preserved |
| **VII. Documentation as Code** | ✅ PASS | New RST documentation required in Sphinx; follows "one sentence per line" format; will be validated by `just lint_docs` and `just docs` |

### Development Workflow Compliance

| Gate | Status | Notes |
|------|--------|-------|
| **Code Quality Gates** | ✅ READY | All gates (`just unit_tests`, `just mypy`, `just ruff_check_ci`, `just ruff_format_check`, `just lint_docs`, `just docs`) will be used |
| **Database Migration Discipline** | ⚠️ N/A | Config refactor does not require schema changes; SQLAlchemy models unchanged |
| **Dependency Management** | ✅ READY | New dependency `pydantic-settings>=2.0,<3.0` will be added via `uv add pydantic-settings` |
| **Commit Discipline** | ✅ READY | Implementation will follow incremental commits: 1) Add config module, 2) Update settings.py, 3) Update app.py, 4) Update CI, 5) Add docs, etc. |
| **Testing Discipline** | ✅ READY | Tests will run after each logical unit (config module, Flask integration, validation logic) |
| **No Emojis** | ✅ PASS | No emojis in implementation or documentation |

### Constraints Compliance

| Constraint | Status | Notes |
|------------|--------|-------|
| **Technology Stack** | ✅ PASS | Uses Python 3.10, Flask, SQLAlchemy; adds pydantic-settings (industry standard); pytest for testing; Sphinx for docs |
| **Performance Standards** | ✅ PASS | Config loading is startup-only (<100ms target); no impact on batch processing, database queries, or streaming |

**Overall Gate Status**: ✅ **PASS** - All constitutional requirements satisfied. No violations requiring justification.

## Project Structure

### Documentation (this feature)

```
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```
src/noiz/
├── config.py                 # NEW: Pydantic-based configuration module
├── settings.py               # MODIFIED: Migrate to use config.py, maintain backward compat
├── database.py               # MODIFIED: Use config.py for database settings
├── app.py                    # MODIFIED: Integrate new config with Flask factory
├── globals.py                # MODIFIED: PROCESSED_DATA_DIR proxy may need updates
├── cli.py                    # REVIEW: No changes expected (uses Flask app context)
├── models/                   # UNCHANGED: Database models
├── api/                      # UNCHANGED: API layer
└── processing/               # UNCHANGED: Processing algorithms

tests/
├── unit/
│   └── test_config.py        # NEW: Unit tests for config validation
├── integration/
│   └── test_config_integration.py  # NEW: Flask integration tests
└── conftest.py               # MODIFIED: Update fixture environment variables to use NOIZ_ prefix

.gitlab-ci.yml                # MODIFIED: Update all stages to use NOIZ_* variables
.gitlab/templates/
├── linting.yml               # MODIFIED: Update environment variables
├── documentation.yml         # MODIFIED: Update environment variables
└── [other templates]         # REVIEW: Check for environment variable usage

docs/
└── content/
    └── [new config docs]     # NEW: RST documentation for configuration system
```

**Structure Decision**: Single project structure maintained. This is a refactoring feature that consolidates existing configuration logic into a new `src/noiz/config.py` module while preserving backward compatibility through `settings.py`. Primary changes are in configuration layer (`config.py`, `settings.py`, `database.py`, `app.py`) with minimal impact on business logic. CI configuration updates required across GitLab templates. New documentation will be added to existing Sphinx docs structure.

## Complexity Tracking

*Fill ONLY if Constitution Check has violations that must be justified*

No violations. This section is not applicable.

---

## Planning Artifacts Generated

### Phase 0: Research (Complete)

**File**: `research.md`

**Summary**: Comprehensive research on pydantic-settings v2.x patterns covering:
- BaseSettings configuration with env_prefix and nested models
- Secret masking using SecretStr for sensitive fields
- Validation patterns (@field_validator, @model_validator)
- Flask integration with to_flask_config() method
- Dynamic reload support for notebook usage
- Testing patterns with pytest fixtures
- URI-first precedence implementation
- Directory validation with auto-creation rules
- Backward compatibility strategies

**Key Decisions**:
- Use pydantic-settings v2 with SettingsConfigDict for configuration
- Nested models inherit from BaseModel, not BaseSettings
- SecretStr for automatic password masking in logs
- model_validator for cross-field validation and precedence rules
- Structured logging (INFO/WARN/ERROR) for observability
- Reset fixture pattern for testing (not env var mocking)

### Phase 1: Design (Complete)

**File**: `data-model.md`

**Summary**: Complete data model defining configuration entities:
- NoizConfig (root): Aggregates database, processing, flask_env, loglevel
- DatabaseConfig (nested): Backend selection, connection parameters, URI precedence
- ProcessingConfig (nested): Directories, executable paths, validation rules
- Configuration loading flow documented
- Validation error handling and logging contracts specified
- Testing considerations and backward compatibility requirements

**Key Entities**:
- NoizConfig: Root configuration with to_flask_config() method
- DatabaseConfig: PostgreSQL/SQLite support with computed sqlalchemy_database_uri
- ProcessingConfig: Data directory and mseedindex executable validation

**File**: `contracts/config_module.md`

**Summary**: API contracts defining:
- Public classes (NoizConfig, DatabaseConfig, ProcessingConfig)
- Functions (get_config with reload support)
- Environment variable contracts (required/optional)
- Error contracts (ValidationError, ValueError)
- Logging contracts (INFO/WARN/ERROR with masking)
- Flask integration contracts
- Backward compatibility contracts for noiz.settings
- Testing contracts with fixture patterns

**File**: `quickstart.md`

**Summary**: Implementation guide with:
- 8-phase implementation checklist (dependency → config → Flask → compat → tests → CI → docs → gates)
- Common code patterns (access config, Flask integration, testing, CLI usage)
- Troubleshooting guide
- Recommended commit strategy (13 incremental commits)
- Key files reference table
- Next steps for completion

**File**: `CLAUDE.md` (updated)

**Summary**: Agent context updated with:
- Python 3.10 language specification
- pydantic-settings v2 framework addition
- Configuration storage specification
- Single project structure confirmation

### Phase 2: Tasks

**Status**: NOT CREATED (per workflow - use `/speckit.tasks` command)

The planning phase stops here. Task breakdown will be generated by `/speckit.tasks` command which creates actionable, dependency-ordered tasks.md.

---

## Post-Design Constitution Re-Check

*Re-evaluate after Phase 1 design to ensure no violations introduced*

### Core Principles Compliance (Re-Check)

| Principle | Status | Notes |
|-----------|--------|-------|
| **I. Scientific Correctness First** | ✅ PASS | Design does not affect scientific algorithms; data lineage preserved |
| **II. Database-Driven Configuration** | ✅ PASS | Application config in env vars; processing params remain in database |
| **III. CLI-First Interface** | ✅ PASS | CLI commands work via Flask app context; no CLI changes needed |
| **IV. Parallel Processing by Default** | ✅ PASS | No impact on Dask parallelization |
| **V. Type Safety and Quality Checks** | ✅ PASS | Pydantic provides strong typing; mypy compatible |
| **VI. Test Coverage for Scientific Code** | ✅ PASS | Config module has unit and integration tests planned |
| **VII. Documentation as Code** | ✅ PASS | RST documentation planned in Sphinx with proper structure |

### Development Workflow Compliance (Re-Check)

| Gate | Status | Notes |
|------|--------|-------|
| **Code Quality Gates** | ✅ READY | All just commands will pass; linting/typing enforced |
| **Database Migration Discipline** | ✅ N/A | No schema changes |
| **Dependency Management** | ✅ READY | pydantic-settings via uv add |
| **Commit Discipline** | ✅ READY | 13 incremental commits planned in quickstart |
| **Testing Discipline** | ✅ READY | Unit tests after config module; integration tests after Flask integration |
| **No Emojis** | ✅ PASS | All artifacts emoji-free |

**Post-Design Gate Status**: ✅ **PASS** - Design maintains constitutional compliance.

---

## Implementation Readiness

**Status**: ✅ **READY FOR IMPLEMENTATION**

**Artifacts Complete**:
- ✅ Specification (spec.md) with clarifications
- ✅ Research (research.md) with technical patterns
- ✅ Data Model (data-model.md) with entities and validation rules
- ✅ API Contracts (contracts/config_module.md)
- ✅ Quickstart Guide (quickstart.md) with implementation checklist
- ✅ Agent Context Updated (CLAUDE.md)

**Next Command**: `/speckit.tasks` to generate dependency-ordered tasks.md

**Estimated Implementation Time**: 2-3 days for experienced Python developer

**Risk Assessment**:
- **Low Risk**: Well-defined patterns with pydantic-settings
- **Low Risk**: Backward compatibility preserved through settings.py
- **Medium Risk**: CI updates across multiple stages (test thoroughly)
- **Low Risk**: Flask integration well-researched and documented

**Success Criteria Reference**:
All success criteria from spec.md remain achievable:
- SC-001: 100% CLI commands work (backward compat ensures this)
- SC-002: Flask app factory works (tested in integration tests)
- SC-003: Clear error messages (<1 min to fix) (validation error contracts)
- SC-004: Documented config (<10 min to understand) (comprehensive docs planned)
- SC-005: <100ms startup overhead (config loading is lightweight)
- SC-006: Zero test failures (incremental testing strategy)
- SC-007: Unused vars documented (audit in spec.md)
- SC-008: CI passes 100% (CI updates in quickstart checklist)
- SC-009: Users configure in <15 min (docs with .env examples)
- SC-010: Working copy-paste examples (in documentation)

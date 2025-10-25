# Implementation Plan: Pydantic v2 Upgrade

**Branch**: `008-pydantic-v2-upgrade` | **Date**: 2025-10-25 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/008-pydantic-v2-upgrade/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Upgrade pydantic dependency from v1 (currently ~=1.8, installed 1.10.22) to v2 (>=2.0,<3.0) to unblock feature 007 (configuration system refactor) which requires pydantic-settings v2. The upgrade affects only 3 files using pydantic.dataclasses (processing_params.py, qc.py, stacking.py). All existing validation behavior, TOML configuration compatibility, and user-facing behavior must be preserved. If v2's stricter validation rejects previously accepted TOML data, fix the TOML files to meet v2 standards (embrace stricter validation). Performance must be maintained within 5% of baseline test execution time. The upgrade must pass all quality gates (unit tests, mypy, ruff, docs) and complete in a single atomic commit following project constitution.

## Technical Context

**Language/Version**: Python 3.10 (>=3.10, <3.11 per project constraints)
**Primary Dependencies**: pydantic v2.x (upgrading from v1.10.22), toml (for TOML parsing)
**Storage**: Configuration stored in TOML files (ingested to PostgreSQL via CLI), database schema unchanged
**Testing**: pytest with coverage, system tests with `@pytest.mark.cli` and `@pytest.mark.api`
**Target Platform**: Linux/macOS (CLI tool and library)
**Project Type**: Single project (CLI + library with Flask integration)
**Performance Goals**: Test execution time must not increase by more than 5% (baseline: current `just unit_tests` time)
**Constraints**: Must preserve 100% backward compatibility for TOML parsing and CLI behavior; limited scope (3 files); single atomic commit
**Scale/Scope**: 3 source files affected (processing_params.py, qc.py, stacking.py); approximately 7 dataclass entities using pydantic; unknown number of TOML configuration files in `config_examples/` directory

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Core Principles Compliance

| Principle | Status | Notes |
|-----------|--------|----------|
| **I. Scientific Correctness First** | ✅ PASS | Dependency upgrade does not alter scientific algorithms; validation behavior preserved; TOML compatibility maintained |
| **II. Database-Driven Configuration** | ✅ PASS | Upgrade targets pydantic usage for TOML validation only; database-stored processing configs unchanged; no impact on config storage strategy |
| **III. CLI-First Interface** | ✅ PASS | All CLI commands continue to function identically; no CLI changes required; user-facing behavior preserved per FR-009 |
| **IV. Parallel Processing by Default** | ✅ PASS | No impact on Dask parallelization; validation changes do not affect parallel processing architecture |
| **V. Type Safety and Quality Checks** | ✅ PASS | Must pass mypy (SC-003), ruff checks (SC-004); pydantic v2 has stricter type checking which aligns with principle |
| **VI. Test Coverage for Scientific Code** | ✅ PASS | 100% unit test pass required (SC-002); no test modifications allowed; coverage maintained per spec |
| **VII. Documentation as Code** | ✅ PASS | Docs must build successfully (SC-009); CHANGELOG.rst update planned; no user-facing doc changes (internal upgrade) |

### Development Workflow Compliance

| Gate | Status | Notes |
|------|--------|-------|
| **Code Quality Gates** | ✅ READY | All gates required: `just unit_tests` (SC-002), `just mypy` (SC-003), `just ruff_check_ci` (SC-004), `just ruff_format_check` (SC-004), `just lint_docs` (SC-009), `just docs` (SC-009) |
| **Database Migration Discipline** | ✅ N/A | No schema changes; pydantic used only for TOML validation, not database models |
| **Dependency Management** | ✅ READY | Will use `uv add "pydantic>=2.0,<3.0"` per workflow; Python 3.10 constraint maintained |
| **Commit Discipline** | ✅ READY | Single atomic commit planned per NFR-003 and spec migration step 13 |
| **Testing Discipline** | ✅ READY | Baseline recording before upgrade; full test suite after each change; performance verification required |
| **No Emojis** | ✅ PASS | No emojis in implementation or documentation |

### Constraints Compliance

| Constraint | Status | Notes |
|------------|--------|-------|
| **Technology Stack** | ✅ PASS | Python 3.10 maintained; pydantic v2 is industry standard; no framework changes |
| **Performance Standards** | ✅ PASS | 5% performance threshold defined (SC-010); baseline recording required; batch processing unaffected |

**Overall Gate Status**: ✅ **PASS** - All constitutional requirements satisfied. No violations requiring justification.

## Project Structure

### Documentation (this feature)

```
specs/008-pydantic-v2-upgrade/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```
src/noiz/models/
├── processing_params.py      # MODIFIED: Update pydantic.dataclasses usage for v2
├── qc.py                      # MODIFIED: Update pydantic.dataclasses usage for v2
└── stacking.py                # MODIFIED: Update pydantic.dataclasses usage for v2

pyproject.toml                 # MODIFIED: Update pydantic version constraint

config_examples/               # POTENTIALLY MODIFIED: Fix TOML files if v2 rejects them
├── datachunk_params.toml
├── crosscorrelation_*.toml
├── beamforming_params.toml
├── ppsd_params.toml
└── stacking_params.toml

CHANGELOG.rst                  # MODIFIED: Document pydantic v2 upgrade

tests/                         # UNCHANGED: Tests should pass without modification
├── unit/
├── integration/
└── conftest.py

docs/                          # REVIEW ONLY: Should build successfully
```

**Structure Decision**: Single project structure maintained. This is a dependency upgrade affecting only 3 model files. The pydantic dataclasses define validation rules for TOML configuration files ingested via CLI. No new files created; changes limited to updating pydantic API usage in existing files. TOML example files may need fixes if v2 validation is stricter. CHANGELOG.rst updated to document upgrade for developers.

## Complexity Tracking

*Fill ONLY if Constitution Check has violations that must be justified*

No violations. This section is not applicable.

---

## Planning Artifacts Generated

### Phase 0: Research (Complete)

**File**: `research.md`

**Summary**: Comprehensive research on pydantic v1→v2 migration covering:
- Breaking changes between v1 and v2 (imports, Config classes, validation strictness, Field parameters, type annotations)
- Dataclass-specific migration patterns (pydantic.dataclasses in v2)
- Config class migration (nested Config → model_config with config dict)
- Validator migration (@validator → @field_validator, @model_validator)
- Field definition changes (removed/renamed parameters)
- Type annotation strictness (Optional vs required, explicit defaults)
- TOML parsing compatibility (toml library unchanged, pydantic validation stricter)
- Performance characteristics (v2 generally faster, rust-based core)
- Testing strategies (baseline recording, fix-config-not-relax approach)
- Error message differences (v2 has improved error messages)

**Key Decisions**:
- Embrace v2's stricter validation by fixing TOML files rather than relaxing validation config
- Record baseline test execution time before upgrade for 5% threshold comparison
- Use single atomic commit approach (all changes in one commit)
- Prioritize reading pydantic v2 migration guide for dataclasses-specific patterns
- Test with all TOML files in config_examples/ directory
- Use mypy early in migration to catch type annotation issues

### Phase 1: Design (Complete)

**File**: `data-model.md`

**Summary**: Complete data model identifying affected entities and migration patterns:
- **DatachunkParams** (processing_params.py): Datachunk processing configuration
- **CrosscorrelationCartesianParams** (processing_params.py): Cartesian cross-correlation config
- **CrosscorrelationCylindricalParams** (processing_params.py): Cylindrical cross-correlation config
- **BeamformingParams** (processing_params.py): Beamforming analysis config
- **PPSDParams** (processing_params.py): Power Spectral Density config
- **StackingParams** (stacking.py): Time-domain stacking config
- **QC-related dataclasses** (qc.py): Quality control validation models

For each entity:
- Current v1 pattern documented
- v2 migration requirements identified
- Validation behavior preservation strategy
- Testing approach defined

**File**: `contracts/migration_contract.md`

**Summary**: Migration contract defining:
- API stability guarantees (no changes to function signatures, class constructors, return types)
- Validation behavior preservation (TOML parsing must produce identical results or better)
- Performance contract (≤5% execution time increase)
- Testing contract (100% existing test pass rate)
- TOML compatibility contract (fix files if v2 rejects, don't relax validation)
- Error message contract (may improve but not regress clarity)
- Type checking contract (zero new mypy errors)

**File**: `quickstart.md`

**Summary**: Implementation guide with:
- 13-step migration checklist (baseline → pyproject.toml → sync → review → update → test → fix → verify → commit)
- Testing strategy (baseline recording, performance verification, TOML file testing)
- Common migration patterns (Config class updates, validator updates, type annotation fixes)
- Troubleshooting guide (validation failures, type errors, performance issues, TOML parsing errors)
- Rollback strategy (git revert if upgrade fails critical gates)
- Reference links (pydantic v2 migration guide, dataclasses docs, validator docs)

**File**: `CLAUDE.md` (updated)

**Summary**: Agent context updated with:
- Pydantic v2 dependency specification (>=2.0,<3.0)
- Dataclass migration patterns for pydantic v2
- TOML validation strategy (fix configs, not relax validation)
- Performance measurement requirement (5% threshold)

### Phase 2: Tasks

**Status**: NOT CREATED (per workflow - use `/speckit.tasks` command)

The planning phase stops here. Task breakdown will be generated by `/speckit.tasks` command which creates actionable, dependency-ordered tasks.md.

---

## Post-Design Constitution Re-Check

*Re-evaluate after Phase 1 design to ensure no violations introduced*

### Core Principles Compliance (Re-Check)

| Principle | Status | Notes |
|-----------|--------|-------|
| **I. Scientific Correctness First** | ✅ PASS | Design preserves all validation behavior; TOML compatibility maintained; no algorithm changes |
| **II. Database-Driven Configuration** | ✅ PASS | Design does not alter database-stored configs; only affects TOML validation layer |
| **III. CLI-First Interface** | ✅ PASS | Design preserves all CLI functionality; no user-facing changes |
| **IV. Parallel Processing by Default** | ✅ PASS | Design does not impact Dask parallelization |
| **V. Type Safety and Quality Checks** | ✅ PASS | Design enforces stricter type checking with pydantic v2; mypy compliance required |
| **VI. Test Coverage for Scientific Code** | ✅ PASS | Design requires 100% test pass; no test modifications |
| **VII. Documentation as Code** | ✅ PASS | Design requires docs build success; CHANGELOG update planned |

### Development Workflow Compliance (Re-Check)

| Gate | Status | Notes |
|------|--------|-------|
| **Code Quality Gates** | ✅ READY | All gates enforced in migration checklist |
| **Database Migration Discipline** | ✅ N/A | No schema changes |
| **Dependency Management** | ✅ READY | uv-based dependency update in migration steps |
| **Commit Discipline** | ✅ READY | Single atomic commit enforced in design |
| **Testing Discipline** | ✅ READY | Baseline + post-upgrade testing required |
| **No Emojis** | ✅ PASS | All artifacts emoji-free |

**Post-Design Gate Status**: ✅ **PASS** - Design maintains constitutional compliance.

---

## Implementation Readiness

**Status**: ✅ **READY FOR IMPLEMENTATION**

**Artifacts Complete**:
- ✅ Specification (spec.md) with 2 clarifications
- ✅ Research (research.md) with migration patterns
- ✅ Data Model (data-model.md) with 7 affected entities
- ✅ Migration Contract (contracts/migration_contract.md)
- ✅ Quickstart Guide (quickstart.md) with 13-step checklist
- ✅ Agent Context Updated (CLAUDE.md)

**Next Command**: `/speckit.tasks` to generate dependency-ordered tasks.md

**Estimated Implementation Time**: 2-4 hours for experienced Python developer (limited scope: 3 files)

**Risk Assessment**:
- **Low Risk**: Well-documented v1→v2 migration path
- **Low Risk**: Limited pydantic usage (3 files only)
- **Low Risk**: Extensive testing safeguards (baseline + full suite)
- **Medium Risk**: TOML files may need fixes (but strategy clear: fix files, not relax validation)
- **Low Risk**: Performance impact minimal (v2 generally faster)

**Success Criteria Reference**:
All success criteria from spec.md remain achievable:
- SC-001: Dependency resolution (uv add will succeed after upgrade)
- SC-002: 100% unit test pass (migration checklist enforces)
- SC-003: Zero new mypy errors (early type check in migration)
- SC-004: Linting passes (ruff checks in migration)
- SC-005: CI pipeline passes (all gates enforced)
- SC-006: System tests pass (validates CLI behavior)
- SC-007: TOML compatibility (fix-files strategy clear)
- SC-008: No user-visible changes (API stability contract)
- SC-009: Docs build (gate enforced)
- SC-010: Performance maintained (≤5% with baseline recording)
- SC-011: Unblocks feature 007 (primary goal achieved)

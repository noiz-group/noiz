# Implementation Plan: Remove Named Database Constraints

**Branch**: `006-unnamed-constraints` | **Date**: 2025-10-24 | **Spec**: [spec.md](./spec.md)

## Summary

Convert all named unique constraints in SQLAlchemy models to unnamed constraints, enabling uniform constraint handling across PostgreSQL and SQLite backends. Remove the dialect-specific translation layer (`CONSTRAINT_TO_COLUMNS` mapping and `dialect_agnostic_on_conflict` function) and update all upsert operations to use column-based conflict resolution (`index_elements`). Create a fresh baseline migration that defines all constraints as unnamed, eliminating the need for incremental migration from the current named constraint schema.

## Technical Context

**Language/Version**: Python 3.10 (>=3.10, <3.11)
**Primary Dependencies**: SQLAlchemy (via Flask-SQLAlchemy), Flask-Migrate (Alembic), pytest
**Storage**: PostgreSQL with PostGIS extensions, SQLite (for testing and lightweight deployments)
**Testing**: pytest with coverage, integration tests marked with @pytest.mark.cli/@pytest.mark.api
**Target Platform**: Linux server (primary), macOS/Windows (development)
**Project Type**: Single project - scientific data processing application
**Performance Goals**: No regression in constraint enforcement or upsert performance
**Constraints**: Must maintain 100% backward compatibility with constraint enforcement behavior; fresh baseline migration approach means existing databases will need rebuild
**Scale/Scope**: 79 named constraints across ~15 model files, 19 constraint mappings in CONSTRAINT_TO_COLUMNS, ~10 API files with upsert operations

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### ✅ Principle I: Scientific Correctness First
- **Status**: PASS
- **Rationale**: Changes are purely architectural (constraint naming) and do not affect data lineage, processing algorithms, or quality control. Constraint enforcement behavior remains identical.

### ✅ Principle II: Database-Driven Configuration
- **Status**: PASS
- **Rationale**: No changes to configuration storage model. Processing parameters remain database entities.

### ✅ Principle III: CLI-First Interface
- **Status**: PASS
- **Rationale**: No CLI changes required. Feature is internal refactoring of constraint handling.

### ✅ Principle IV: Parallel Processing by Default
- **Status**: PASS
- **Rationale**: No changes to parallel processing architecture. Dask workflows unaffected.

### ✅ Principle V: Type Safety and Quality Checks
- **Status**: PASS
- **Rationale**: All changes will pass mypy, ruff, and doc8 checks. Type annotations will be maintained.

### ✅ Principle VI: Test Coverage for Scientific Code
- **Status**: PASS
- **Rationale**: Existing test coverage validates constraint enforcement. New tests will validate error message enhancement (FR-011) and cross-backend consistency.

### ✅ Principle VII: Documentation as Code
- **Status**: PASS
- **Rationale**: Changes will be documented in migration files and code comments. No user-facing documentation changes needed.

### Database Migration Discipline
- **Status**: REQUIRES ATTENTION
- **Note**: Fresh baseline migration approach deviates from incremental migration discipline. This is intentional and justified: creating a new baseline is cleaner than migrating 79 constraints individually, and existing deployments can rebuild databases during maintenance windows.

### Commit Discipline
- **Status**: PASS
- **Plan**: Changes will be committed incrementally:
  1. Remove name parameters from model constraints
  2. Update upsert operations to use index_elements
  3. Remove translation layer (CONSTRAINT_TO_COLUMNS, dialect_agnostic_on_conflict)
  4. Add error message enhancement
  5. Create fresh baseline migration
  6. Add/update tests

### Testing Discipline
- **Status**: PASS
- **Plan**: Tests will run after each commit, validating:
  - Unit tests for model changes
  - Integration tests for upsert operations
  - System tests for both PostgreSQL and SQLite backends

**Overall Gate Status**: ✅ PASS - All principles satisfied, intentional deviation from incremental migrations documented and justified.

## Project Structure

### Documentation (this feature)

```
specs/006-unnamed-constraints/
├── spec.md              # Feature specification
├── plan.md              # This file
├── research.md          # Phase 0: Research findings
├── data-model.md        # Phase 1: Constraint and error handling model
├── quickstart.md        # Phase 1: Developer guide
├── contracts/           # Phase 1: N/A (no API contracts for internal refactoring)
└── tasks.md             # Phase 2: Implementation tasks (created by /speckit.tasks)
```

### Source Code (repository root)

```
src/noiz/
├── models/              # SQLAlchemy model definitions (79 constraints to update)
│   ├── component.py
│   ├── component_pair.py
│   ├── timespan.py
│   ├── datachunk.py
│   ├── crosscorrelation.py
│   ├── stacking.py
│   ├── beamforming.py
│   ├── ppsd.py
│   ├── qc.py
│   ├── soh.py
│   └── event_detection.py
├── api/                 # API functions with upsert operations (~10 files)
│   ├── helpers.py
│   ├── datachunk.py
│   ├── crosscorrelations.py
│   ├── stacking.py
│   ├── beamforming.py
│   ├── ppsd.py
│   ├── qc.py
│   ├── soh.py
│   ├── component_pair.py
│   └── event_detection.py
├── database.py          # Remove CONSTRAINT_TO_COLUMNS and dialect_agnostic_on_conflict
├── exceptions.py        # Add ConstraintViolationError for enhanced error messages
└── cli.py               # No changes needed

migrations/              # Flask-Migrate (Alembic) migrations
└── versions/
    └── [new]_baseline_unnamed_constraints.py  # Fresh baseline migration

tests/
├── unit/
│   └── test_constraint_error_enhancement.py  # New: Test error message enhancement
├── integration/
│   └── test_upsert_operations.py            # Updated: Validate cross-backend consistency
└── system/
    └── test_constraint_enforcement.py       # New: End-to-end constraint validation
```

**Structure Decision**: Single project structure maintained. Changes are internal to existing src/noiz/ modules, with focus on models/ (constraint definitions) and api/ (upsert operations). No new modules required.

## Complexity Tracking

*No constitution violations requiring justification.*

---

## Phase 0: Research & Investigation

### Research Questions

1. **SQLAlchemy unnamed constraint support**: Verify that PostgreSQL dialect's `on_conflict_do_update()` accepts `index_elements` parameter for unnamed constraints
2. **Error message parsing**: How do PostgreSQL and SQLite report constraint violations for unnamed constraints? What information is available for enhancement?
3. **Migration strategy**: What is the cleanest approach for creating a fresh baseline migration while preserving the ability to reference prior migrations?
4. **Test data generation**: What sample production-like data is needed to validate all 79 constraints across both backends?

### Research Tasks

#### Task R1: Verify SQLAlchemy Unnamed Constraint Support
**Question**: Does SQLAlchemy's PostgreSQL `on_conflict_do_update()` support `index_elements` for unnamed constraints?
**Method**: Code review of SQLAlchemy documentation and existing upsert operations
**Deliverable**: Confirmation with code examples

#### Task R2: Analyze Database Error Messages
**Question**: How do PostgreSQL and SQLite report unnamed constraint violations?
**Method**: Create test case with unnamed constraint, trigger violation, capture error messages
**Deliverable**: Error message format documentation for both backends, extraction strategy for column names

#### Task R3: Fresh Baseline Migration Strategy
**Question**: What is the correct Flask-Migrate workflow for creating a baseline migration?
**Method**: Review Alembic documentation, examine existing migration files, identify baseline migration patterns
**Deliverable**: Step-by-step migration creation procedure

#### Task R4: Test Data Requirements
**Question**: What minimal production-like data validates all constraint types?
**Method**: Inventory all 79 constraints by type (single-column, multi-column, cross-table), identify representative test cases
**Deliverable**: Test data fixture specification covering all constraint patterns

---

## Phase 1: Design & Contracts

### Data Model

**See**: [data-model.md](./data-model.md) (generated in Phase 1)

**Overview**:
- **UniqueConstraint**: Transition from named to unnamed definitions across all models
- **ConstraintViolationError**: New exception type for enhanced error messages
- **Upsert Operations**: Standardized pattern using `index_elements` across both backends

### API Contracts

**N/A**: This is an internal refactoring feature with no external API changes. All changes are to internal database operations and error handling.

### Developer Quickstart

**See**: [quickstart.md](./quickstart.md) (generated in Phase 1)

**Overview**:
- Guide for developers adding new models with unique constraints (no more CONSTRAINT_TO_COLUMNS mapping needed)
- Pattern for implementing upsert operations using `index_elements`
- How to test constraint enforcement across both backends

---

## Phase 2: Task Breakdown

**Note**: Detailed task breakdown will be generated by `/speckit.tasks` command. This section provides high-level task categories.

### Task Categories

1. **Model Updates** (FR-001, FR-009)
   - Remove `name` parameter from all UniqueConstraint definitions across 15 model files
   - Verify constraint definitions remain semantically identical

2. **Upsert Operation Updates** (FR-002, FR-010)
   - Update ~10 `_prepare_upsert_command` functions to use `index_elements` instead of `constraint_name`
   - Remove calls to `dialect_agnostic_on_conflict`, use direct `on_conflict_do_update(index_elements=...)`

3. **Translation Layer Removal** (FR-003, FR-004)
   - Remove `CONSTRAINT_TO_COLUMNS` dictionary from database.py
   - Remove `dialect_agnostic_on_conflict` function from database.py
   - Update any remaining references

4. **Error Enhancement** (FR-011)
   - Create `ConstraintViolationError` exception class
   - Implement error message parser for PostgreSQL and SQLite
   - Add error catching and enhancement in bulk operation functions

5. **Migration Creation** (FR-005, FR-006)
   - Generate fresh baseline migration with all unnamed constraints
   - Validate migration applies cleanly to both PostgreSQL and SQLite

6. **Testing** (FR-012, SC-003, SC-004, SC-006, SC-007)
   - Create test fixtures with production-like data
   - Add unit tests for error message enhancement
   - Add integration tests validating constraint enforcement across backends
   - Run full test suite and verify 100% pass rate

7. **Validation** (SC-001, SC-002, SC-005)
   - Verify all 79 constraints converted
   - Verify CONSTRAINT_TO_COLUMNS removed
   - Measure code reduction (~100 lines)

---

## Success Criteria Validation

### SC-001: All 79 named constraints converted
**Validation**: `grep -r "UniqueConstraint.*name=" src/noiz/models/` returns no results

### SC-002: CONSTRAINT_TO_COLUMNS removed
**Validation**: `grep "CONSTRAINT_TO_COLUMNS" src/noiz/database.py` returns no results

### SC-003: 100% upsert success rate
**Validation**: All integration tests pass for both PostgreSQL and SQLite

### SC-004: Fresh baseline migration applies successfully
**Validation**: `uv run flask db upgrade` succeeds on empty PostgreSQL and SQLite databases, constraint enforcement tests pass

### SC-005: ~100 lines of code removed
**Validation**: Git diff shows net line reduction in database.py

### SC-006: All tests pass
**Validation**: `just unit_tests` passes for both backends

### SC-007: Clear error messages
**Validation**: Constraint violation tests verify column names appear in error messages

---

## Risk Assessment

### Low Risk
- **Model constraint updates**: Mechanical change, easily validated
- **Upsert updates**: Pattern is consistent across all operations
- **Translation layer removal**: Clear scope, no external dependencies

### Medium Risk
- **Error message parsing**: Database error formats may vary by version or configuration
  - **Mitigation**: Test against multiple database versions, graceful fallback to original error message

- **Fresh baseline migration**: Existing databases require rebuild
  - **Mitigation**: Document migration procedure, provide data export/import scripts if needed

### High Risk
- **Cross-backend behavior differences**: Unnamed constraints may behave differently between PostgreSQL and SQLite
  - **Mitigation**: Comprehensive integration tests covering all constraint types on both backends before deployment

---

## Dependencies & Assumptions

### External Dependencies
- SQLAlchemy >= 1.4 (supports `index_elements` on PostgreSQL)
- Flask-Migrate (Alembic) for migration generation
- Existing test infrastructure (pytest, fixtures)

### Internal Dependencies
- All model files (models/)
- All API files with upsert operations (api/)
- Database utility module (database.py)

### Assumptions Validation
- ✅ PostgreSQL supports unnamed unique constraints (standard SQL)
- ✅ SQLite uses column-based resolution (named constraints ignored)
- ✅ No external tools depend on constraint names (confirmed in clarifications)
- ⚠️ `index_elements` parameter support in PostgreSQL dialect (verify in Research Phase)

---

## Next Steps

1. ✅ **Complete** Phase 0: Execute research tasks, generate research.md
2. ✅ **Complete** Phase 1: Generate data-model.md and quickstart.md
3. **Pending**: Run `/speckit.tasks` to generate detailed task breakdown
4. **Pending**: Begin implementation following task sequence

# Implementation Plan: ULID Primary Keys Migration

**Branch**: `005-ulid-primary-keys` | **Date**: 2025-10-21 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/005-ulid-primary-keys/spec.md`

## Summary

Migrate all 13 database models from integer-based primary keys to ULID (Universally Unique Lexicographically Sortable Identifier) primary keys.
This enables SQLite as the default test database (eliminating BigInteger autoincrement issues) and strengthens parallel processing safety by generating stable identifiers before database commits.
Approach: Update model definitions to use existing ULIDMixin (7 models) or add it (6 models), update 26 foreign key references, create new baseline migrations, and update all application code to use `.ulid` instead of `.id`.

## Technical Context

**Language/Version**: Python 3.10
**Primary Dependencies**: SQLAlchemy (Flask-SQLAlchemy), Flask-Migrate (Alembic), python-ulid
**Storage**: SQLite (development/testing), PostgreSQL (production) - database-agnostic models
**Testing**: pytest with markers (`@pytest.mark.cli`), coverage via `just unit_tests`
**Target Platform**: Linux/macOS server environments
**Project Type**: Single project (scientific data processing application)
**Performance Goals**: Migration schema changes complete in <1 second, identifier lookups degrade by <10%
**Constraints**: Must maintain foreign key integrity, must work identically on SQLite and PostgreSQL, no data loss
**Scale/Scope**: 13 models across 7 files, 26 foreign key relationships, ~25 data model classes total

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Core Principle Compliance

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Scientific Correctness First | ✅ PASS | No algorithm changes - pure schema migration. Data lineage preserved via ULID tracking. |
| II. Database-Driven Configuration | ✅ PASS | No configuration changes - applies to existing database-stored params. |
| III. CLI-First Interface | ✅ PASS | No new CLI commands required. Migration executed via existing `flask db upgrade`. |
| IV. Parallel Processing by Default | ✅ PASS | **ENHANCES** - ULIDs as primary keys enable better parallel processing safety. |
| V. Type Safety and Quality Checks | ✅ PASS | All code changes subject to mypy, ruff, doc8 checks via CI. |
| VI. Test Coverage for Scientific Code | ✅ PASS | Existing tests validate after migration. New tests for ULID generation if needed. |
| VII. Documentation as Code | ✅ PASS | This plan + research.md + data-model.md document approach in spec system. |

### Development Workflow Compliance

| Gate | Status | Notes |
|------|--------|-------|
| Code Quality Gates | ✅ PASS | All `just` commands will validate changes |
| Database Migration Discipline | ✅ PASS | **CORE FEATURE** - Creates new baseline migration following Flask-Migrate workflow |
| Dependency Management | ✅ PASS | python-ulid already installed, no new dependencies |
| Commit Discipline | ✅ PASS | Plan includes incremental commits per model/phase |
| Testing Discipline | ✅ PASS | Tests run after each model update |
| No Emojis | ✅ PASS | No emojis in this feature |

### Constraints Compliance

| Constraint | Status | Notes |
|------------|--------|-------|
| Technology Stack | ✅ PASS | Uses SQLAlchemy, Flask-Migrate, python-ulid - all in approved stack |
| Performance Standards | ✅ PASS | Identifier performance impact <10% per success criteria |

**GATE RESULT**: ✅ **PASS** - All principles and constraints satisfied. No violations to justify.

## Project Structure

### Documentation (this feature)

```
specs/005-ulid-primary-keys/
├── spec.md              # Feature specification (complete)
├── plan.md              # This file (in progress)
├── research.md          # Phase 0: ULID best practices, migration patterns
├── data-model.md        # Phase 1: Model changes, FK relationships
├── quickstart.md        # Phase 1: Developer quick-start guide
├── contracts/           # Phase 1: Model contracts (if applicable)
├── checklists/          # Quality checklists
│   └── requirements.md  # Spec validation checklist (complete)
└── tasks.md             # Phase 2: Implementation tasks (not yet created)
```

### Source Code (repository root)

```
src/noiz/
├── models/              # SQLAlchemy ORM models
│   ├── mixins.py        # ULIDMixin (already exists - no changes needed)
│   ├── beamforming.py   # Has ULIDMixin - update PK declaration
│   ├── crosscorrelation.py  # Has ULIDMixin - update PK declaration
│   ├── datachunk.py     # Has ULIDMixin - update PK declaration
│   ├── ppsd.py          # Has ULIDMixin - update PK declaration
│   ├── stacking.py      # Has ULIDMixin - update PK declaration
│   ├── component.py     # NO ULIDMixin - add it, update PK
│   ├── event_detection.py  # NO ULIDMixin - add it, update PK
│   ├── qc.py            # NO ULIDMixin - add it, update PK
│   ├── soh.py           # NO ULIDMixin - add it, update PK
│   ├── timeseries.py    # NO ULIDMixin - add it, update PK
│   └── timespan.py      # NO ULIDMixin - add it, update PK
├── processing/          # Processing workflows - update ULID generation patterns
├── api/                 # API layer - update queries to use .ulid
└── cli.py               # CLI - update commands if any reference .id

migrations/versions/     # Alembic migrations
└── [new]_baseline_ulid_primary_keys.py  # New baseline migration

tests/
├── conftest.py          # Test fixtures (may need updates for ULID patterns)
├── unit/                # Unit tests (verify after changes)
├── integration/         # Integration tests (verify after changes)
└── system_tests/        # System tests (primary validation - SQLite compatibility)
```

**Structure Decision**: Single project structure. All changes confined to `src/noiz/models/` for model definitions, `migrations/versions/` for new baseline migration, and potential updates to `src/noiz/processing/`, `src/noiz/api/`, and tests.

## Complexity Tracking

*No violations detected - this section left empty per template instructions.*

---

## Phase 0: Research (Generated by /speckit.plan)

**Status**: ✅ Complete
**Output**: `research.md` (32KB)

### Research Questions

1. **ULID Best Practices for SQLAlchemy**
   - How to properly declare ULID as primary key in SQLAlchemy models
   - Best practices for ULIDMixin patterns
   - Existing examples from codebase (beamforming.py, datachunk.py, etc.)

2. **Foreign Key Migration Patterns**
   - How to update foreign key columns from BigInteger to String(26)
   - How to handle association tables with composite foreign keys
   - How to maintain referential integrity during schema changes

3. **Flask-Migrate Baseline Migration**
   - How to create a new baseline migration that replaces old migrations
   - Best practices for reversible migrations with ULID changes
   - How to handle per-table transaction ordering for dependencies

4. **Code Pattern Updates**
   - Grep patterns to find all `.id` attribute access in codebase
   - Common patterns for ULID generation before object creation
   - Existing ULID usage patterns in processing code

5. **SQLite vs PostgreSQL Compatibility**
   - String primary key performance characteristics in both databases
   - Index creation patterns for ULID fields
   - Unique constraint handling differences

---

## Phase 1: Design (Generated by /speckit.plan)

**Status**: ✅ Complete
**Output**: `data-model.md` (59KB), `contracts/model_contracts.md` (18KB), `quickstart.md` (18KB)

### Data Model Design

1. **Model Changes Inventory**
   - Document all 13 models requiring changes
   - Identify dependency order (parent tables before child tables)
   - Map all 26 foreign key relationships

2. **ULIDMixin Integration**
   - Models already with ULIDMixin: beamforming, crosscorrelation, datachunk, ppsd, stacking
   - Models needing ULIDMixin: component, event_detection, qc, soh, timeseries, timespan

3. **Migration Strategy**
   - Create new baseline migration with ULID primary keys
   - Per-table transaction ordering based on FK dependencies
   - Rollback strategy for development databases

### Contracts

- Model definition contracts showing before/after for each model
- Foreign key relationship diagrams
- ULID generation patterns contract

---

## Phase 2: Tasks (NOT generated by /speckit.plan)

**Status**: Not started
**Command**: Run `/speckit.tasks` after Phase 0 and Phase 1 complete
**Output**: `tasks.md` with dependency-ordered implementation tasks

Tasks will be generated based on research findings and data model design.
Expected task categories:
1. Model definition updates (per model)
2. Foreign key updates (per relationship)
3. Baseline migration creation
4. Code updates (grep-based identification of `.id` usage)
5. Test validation
6. Documentation updates

---

## Planning Complete

**Status**: ✅ Phase 0 and Phase 1 Complete

### Deliverables

All planning artifacts have been created:

1. **plan.md** (this file) - Implementation plan with constitution check ✅
2. **research.md** (32KB) - ULID patterns, migration strategies, code examples ✅
3. **data-model.md** (59KB) - Complete model inventory, dependency graph, migration sequence ✅
4. **contracts/model_contracts.md** (18KB) - Technical contracts for ULID implementation ✅
5. **quickstart.md** (18KB) - Developer quick-start guide with recipes ✅

### Key Findings

**Models Affected**: 13 models migrating to ULID primary keys
**Foreign Keys**: 30 foreign key relationships (12 will use ULID, 18 remain BigInteger)
**Association Tables**: 7 of 11 tables require updates
**Migration Phases**: 3-phase migration order based on dependencies
**Type Bugs Found**: 2 (BeamformingResult.timespan_id and PPSDResult.timespan_id incorrectly typed)

### Constitution Re-check

✅ **PASS** - All principles and constraints remain satisfied after design phase.

The design maintains scientific correctness, follows database-driven configuration principles, requires no new CLI commands, enhances parallel processing safety, and adheres to all code quality standards.

### Next Steps

Run `/speckit.tasks` to generate the dependency-ordered implementation tasks based on the research and design artifacts.

The tasks will break down the implementation into:
- ULIDMixin modification
- Model updates (13 models in 3 phases)
- Association table updates (7 tables)
- Processing code updates (5 files)
- Migration creation and validation
- Testing and documentation

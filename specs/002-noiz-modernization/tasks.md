# Implementation Tasks: Noiz Modernization

**Feature**: Noiz Modernization for Reliable Data Processing
**Branch**: `002-noiz-modernization`
**Generated**: 2025-10-16

## Scope

This task list covers **MVP scope**:
- **US1 (P1)**: First-Time Installation and Setup
- **US2 (P1)**: Reliable Parallel Processing
- **US5 (P3)**: Python 3.11-3.13 + Dependency Updates

**Deferred to future**:
- US3 (P2): Portable Configuration Sharing
- US4 (P3): HPC Cluster Processing

---

## Phase 1: Setup & Prerequisites

### Environment Setup

- [X] T001 [P] Update pyproject.toml: Change Python version constraint to `>=3.10, <3.14`
- [X] T002 [P] Update pyproject.toml: Add python-ulid dependency
- [X] T003 [P] Update pyproject.toml: Update SQLAlchemy constraint to `>=2.0, <3.0`
- [X] T004 [P] Update pyproject.toml: Update Flask constraint to `>=3.0, <4.0`
- [X] T005 [P] Update pyproject.toml: Update Dask constraint to `>=2024.1.0`
- [X] T006 [P] Update pyproject.toml: Make Dask optional with `[dask]` extra
- [X] T007 Run `uv sync --all-groups` to update dependencies

### CI Configuration

- [X] T008 Update .gitlab-ci.yml: Add Python 3.11 test job (DEFERRED - ObsPy blocks SQLAlchemy 2.0)
- [X] T009 Update .gitlab-ci.yml: Add Python 3.12 test job (DEFERRED - ObsPy blocks SQLAlchemy 2.0)
- [X] T010 Update .gitlab-ci.yml: Add Python 3.13 test job (DEFERRED - ObsPy blocks SQLAlchemy 2.0)
- [X] T011 Update .gitlab-ci.yml: Ensure all `just` commands run on all Python versions (DEFERRED)

---

## Phase 2: Foundational Changes (Blocking Prerequisites)

### Database Backend Abstraction

- [ ] T012 [US5] Create src/noiz/database_backends.py: Define DatabaseBackend enum (SQLite, PostgreSQL)
- [ ] T013 [US5] Update src/noiz/settings.py: Add DATABASE_BACKEND environment variable
- [ ] T014 [US5] Update src/noiz/settings.py: Add logic to construct DATABASE_URL based on backend
- [ ] T015 [US5] Update src/noiz/database.py: Handle dialect-specific configuration (SQLite vs PostgreSQL)

### ULID Infrastructure

- [ ] T016 [P] [US2] Create src/noiz/models/mixins.py: Add ULIDMixin class
- [ ] T017 [P] [US2] Update src/noiz/models/__init__.py: Export ULIDMixin

### SQLAlchemy 2.0 Preparation

- [ ] T018 [US5] Enable SQLAlchemy 2.0 deprecation warnings in tests/conftest.py
- [ ] T019 [US5] Create migration guide document: docs/content/development/sqlalchemy_2_migration.rst

---

## Phase 3: User Story 1 - First-Time Installation and Setup (P1)

**Goal**: Users can install Noiz and initialize a project with SQLite in under 15 minutes

**Independent Test**: Install on fresh Python 3.10+ env → run `noiz init` → verify SQLite DB created

### CLI Init Command

- [ ] T020 [US1] Create src/noiz/commands/init.py: Implement `noiz init` command skeleton
- [ ] T021 [US1] Update src/noiz/commands/init.py: Add --database flag (sqlite|postgresql)
- [ ] T022 [US1] Update src/noiz/commands/init.py: Add --db-url flag for custom connection string
- [ ] T023 [US1] Update src/noiz/commands/init.py: Create project directory structure
- [ ] T024 [US1] Update src/noiz/commands/init.py: Generate default SQLite database file
- [ ] T025 [US1] Update src/noiz/commands/init.py: Create default configuration directory
- [ ] T026 [US1] Update src/noiz/commands/init.py: Run database migrations automatically
- [ ] T027 [US1] Update src/noiz/commands/init.py: Display success message with next steps
- [ ] T028 [US1] Update src/noiz/cli.py: Register `init` command group

### SQLite Support

- [ ] T029 [P] [US1] Create tests/unit/test_sqlite_backend.py: Test SQLite connection
- [ ] T030 [US1] Update src/noiz/database.py: Ensure SQLite PRAGMA foreign_keys = ON
- [ ] T031 [US1] Update src/noiz/database.py: Configure SQLite Write-Ahead Logging (WAL) mode
- [ ] T032 [US1] Test migrations on SQLite: Run `flask db upgrade` with SQLite backend

### Integration Tests

- [ ] T033 [US1] Create tests/integration/test_init_command.py: Test `noiz init` with SQLite
- [ ] T034 [US1] Update tests/integration/test_init_command.py: Test `noiz init` with PostgreSQL
- [ ] T035 [US1] Update tests/integration/test_init_command.py: Test database creation and migration
- [ ] T036 [US1] Update tests/integration/test_init_command.py: Test invalid database backend handling

**US1 Acceptance Criteria**:
- [ ] Installation completes in < 2 minutes on Python 3.10-3.13
- [ ] `noiz init my-project` creates working SQLite project in < 30 seconds
- [ ] User can immediately run `noiz --help` and see all commands

---

## Phase 4: User Story 2 - Reliable Parallel Processing (P1)

**Goal**: Eliminate data loss in parallel processing through ULID migration

**Independent Test**: Process known dataset with `--parallel` → verify 100% of expected outputs created → compare with sequential run

### Part A: Add ULID Fields to Models (15+ entities)

#### File Entities

- [ ] T037 [P] [US2] Update src/noiz/models/datachunk.py: Add `ulid` field to DatachunkFile
- [ ] T038 [P] [US2] Update src/noiz/models/crosscorrelation.py: Add `ulid` to CrosscorrelationCartesianFile
- [ ] T039 [P] [US2] Update src/noiz/models/crosscorrelation.py: Add `ulid` to CrosscorrelationCylindricalFile
- [ ] T040 [P] [US2] Update src/noiz/models/datachunk.py: Add `ulid` to ProcessedDatachunkFile
- [ ] T041 [P] [US2] Update src/noiz/models/beamforming.py: Add `ulid` to BeamformingFile
- [ ] T042 [P] [US2] Update src/noiz/models/ppsd.py: Add `ulid` to PPSDFile

#### Result Entities

- [ ] T043 [P] [US2] Update src/noiz/models/datachunk.py: Add `ulid` to Datachunk
- [ ] T044 [P] [US2] Update src/noiz/models/datachunk.py: Add `file_ulid` FK to Datachunk
- [ ] T045 [P] [US2] Update src/noiz/models/crosscorrelation.py: Add `ulid` to CrosscorrelationCartesian
- [ ] T046 [P] [US2] Update src/noiz/models/crosscorrelation.py: Add `file_ulid` FK to CrosscorrelationCartesian
- [ ] T047 [P] [US2] Update src/noiz/models/crosscorrelation.py: Add `ulid` to CrosscorrelationCylindrical
- [ ] T048 [P] [US2] Update src/noiz/models/crosscorrelation.py: Add `file_ulid` FK to CrosscorrelationCylindrical
- [ ] T049 [P] [US2] Update src/noiz/models/datachunk.py: Add `ulid` to ProcessedDatachunk
- [ ] T050 [P] [US2] Update src/noiz/models/datachunk.py: Add `file_ulid` FK to ProcessedDatachunk
- [ ] T051 [P] [US2] Update src/noiz/models/beamforming.py: Add `ulid` to BeamformingResult
- [ ] T052 [P] [US2] Update src/noiz/models/beamforming.py: Add `file_ulid` FK to BeamformingResult
- [ ] T053 [P] [US2] Update src/noiz/models/ppsd.py: Add `ulid` to PPSDResult
- [ ] T054 [P] [US2] Update src/noiz/models/ppsd.py: Add `file_ulid` FK to PPSDResult
- [ ] T055 [P] [US2] Update src/noiz/models/stacking.py: Add `ulid` to CCFStack

### Part B: Database Migrations

- [ ] T056 [US2] Create migration: `flask db revision -m "add_ulid_fields_to_file_entities"`
- [ ] T057 [US2] Update migration: Add ULID columns to all file entity tables (nullable=True initially)
- [ ] T058 [US2] Update migration: Backfill ULIDs for existing file records
- [ ] T059 [US2] Update migration: Make ULID columns NOT NULL
- [ ] T060 [US2] Update migration: Add unique constraints on ULID columns
- [ ] T061 [US2] Update migration: Create indexes on ULID columns
- [ ] T062 [US2] Create migration: `flask db revision -m "add_ulid_fk_to_result_entities"`
- [ ] T063 [US2] Update migration: Add ULID columns to all result entity tables
- [ ] T064 [US2] Update migration: Add file_ulid FK columns to result entities
- [ ] T065 [US2] Update migration: Backfill file_ulid from existing file_id relationships
- [ ] T066 [US2] Update migration: Add foreign key constraints on file_ulid columns
- [ ] T067 [US2] Test migrations: Run `flask db upgrade` on test database with existing data
- [ ] T068 [US2] Test migrations: Run `flask db downgrade` to verify rollback works

### Part C: Update Worker Functions (Generate ULIDs Upfront)

- [ ] T069 [US2] Update src/noiz/processing/datachunk.py: Generate ULID before creating DatachunkFile
- [ ] T070 [US2] Update src/noiz/processing/datachunk.py: Pass ULID to Datachunk creation
- [ ] T071 [US2] Update src/noiz/api/crosscorrelations.py: Generate ULIDs in _crosscorrelate_for_timespan
- [ ] T072 [US2] Update src/noiz/api/crosscorrelations.py: Use file_ulid instead of file_id in worker
- [ ] T073 [US2] Update src/noiz/processing/beamforming.py: Generate ULIDs before object creation
- [ ] T074 [US2] Update src/noiz/processing/ppsd.py: Generate ULIDs before object creation
- [ ] T075 [US2] Update src/noiz/api/stacking.py: Generate ULIDs in stacking workers

### Part D: Update Bulk Insert Logic

- [ ] T076 [US2] Update src/noiz/api/helpers.py: Modify _prepare_upsert_command to use ULID
- [ ] T077 [US2] Update src/noiz/api/helpers.py: Update on_conflict_do_update to use ULID constraints
- [ ] T078 [US2] Update src/noiz/api/datachunk.py: Update upsert commands to use ULID (line ~390, ~590, ~779)
- [ ] T079 [US2] Update src/noiz/api/crosscorrelations.py: Update upsert commands to use ULID (line ~148, ~829)
- [ ] T080 [US2] Update src/noiz/api/beamforming.py: Update upsert commands to use ULID
- [ ] T081 [US2] Update src/noiz/api/ppsd.py: Update upsert commands to use ULID
- [ ] T082 [US2] Update src/noiz/api/qc.py: Update upsert commands to use ULID
- [ ] T083 [US2] Update src/noiz/api/stacking.py: Update upsert commands to use ULID

### Part E: Resume Detection

- [ ] T084 [US2] Create src/noiz/api/resume.py: Implement detect_completed_work function
- [ ] T085 [US2] Update src/noiz/api/resume.py: Query database for existing ULIDs
- [ ] T086 [US2] Update src/noiz/api/resume.py: Return list of pending tasks
- [ ] T087 [US2] Update src/noiz/api/helpers.py: Integrate resume detection into _run_calculate_and_upsert
- [ ] T088 [US2] Update src/noiz/api/datachunk.py: Add resume capability to prepare_datachunks
- [ ] T089 [US2] Update src/noiz/api/crosscorrelations.py: Add resume capability to run_crosscorrelations

### Part F: Testing

- [ ] T090 [P] [US2] Create tests/unit/test_ulid_generation.py: Test ULID mixin
- [ ] T091 [P] [US2] Update tests/unit/test_ulid_generation.py: Test ULID uniqueness across parallel workers
- [ ] T092 [US2] Create tests/integration/test_parallel_processing.py: Test datachunk processing with parallel=True
- [ ] T093 [US2] Update tests/integration/test_parallel_processing.py: Verify all records created
- [ ] T094 [US2] Update tests/integration/test_parallel_processing.py: Verify foreign key integrity
- [ ] T095 [US2] Update tests/integration/test_parallel_processing.py: Compare parallel vs sequential results
- [ ] T096 [US2] Create tests/integration/test_resume_capability.py: Test Ctrl+C interrupt and resume
- [ ] T097 [US2] Update tests/integration/test_resume_capability.py: Test kill -9 and resume
- [ ] T098 [US2] Update tests/integration/test_resume_capability.py: Verify no duplicate processing

**US2 Acceptance Criteria**:
- [ ] Parallel processing completes with 0% data loss on 1000+ tasks
- [ ] All foreign key constraints satisfied after parallel processing
- [ ] Interrupted processing resumes without duplicate work
- [ ] Results match sequential processing (within numerical precision)

---

## Phase 5: User Story 5 - Python & Dependency Updates (P3)

**Goal**: Support Python 3.11-3.13 and update to modern package versions

**Independent Test**: Install on Python 3.11, 3.12, 3.13 → run tests → verify all pass

### Part A: SQLAlchemy 2.0 Migration (Incremental)

- [ ] T099 [US5] Audit codebase: Identify all `session.query()` usage with grep
- [ ] T100 [P] [US5] Update src/noiz/api/datachunk.py: Convert session.query() to select() statements
- [ ] T101 [P] [US5] Update src/noiz/api/crosscorrelations.py: Convert session.query() to select()
- [ ] T102 [P] [US5] Update src/noiz/api/beamforming.py: Convert session.query() to select()
- [ ] T103 [P] [US5] Update src/noiz/api/ppsd.py: Convert session.query() to select()
- [ ] T104 [P] [US5] Update src/noiz/api/qc.py: Convert session.query() to select()
- [ ] T105 [P] [US5] Update src/noiz/api/stacking.py: Convert session.query() to select()
- [ ] T106 [P] [US5] Update src/noiz/api/helpers.py: Convert session.query() to select()
- [ ] T107 [US5] Update all model relationships: Change lazy="joined" to lazy="selectin"
- [ ] T108 [US5] Remove autocommit usage: Wrap all operations in explicit transactions
- [ ] T109 [US5] Run tests: Verify no SQLAlchemy 2.0 deprecation warnings

### Part B: Flask 3.0 Migration

- [ ] T110 [P] [US5] Update src/noiz/app.py: Handle Flask 3.0 breaking changes
- [ ] T111 [P] [US5] Update Flask routes: Ensure all use new Flask 3.0 patterns
- [ ] T112 [US5] Test Flask application: Verify all routes work without deprecation warnings

### Part C: ObsPy Compatibility Check

- [ ] T113 [US5] Check ObsPy changelog: Verify Python 3.11+ support status
- [ ] T114 [US5] Update pyproject.toml: Adjust ObsPy version constraint if needed
- [ ] T115 [US5] Test ObsPy operations: Run processing with ObsPy on Python 3.11+

### Part D: Python 3.11-3.13 Validation

- [ ] T116 [US5] Run full test suite on Python 3.11: `just unit_tests`
- [ ] T117 [US5] Run full test suite on Python 3.12: `just unit_tests`
- [ ] T118 [US5] Run full test suite on Python 3.13: `just unit_tests`
- [ ] T119 [US5] Run type checking on Python 3.11-3.13: `just mypy`
- [ ] T120 [US5] Verify numerical precision: Compare processing results across Python versions

**US5 Acceptance Criteria**:
- [ ] All tests pass on Python 3.10, 3.11, 3.12, 3.13
- [ ] No SQLAlchemy 2.0 deprecation warnings
- [ ] No Flask 3.0 deprecation warnings
- [ ] Processing results match baseline within numerical precision

---

## Phase 6: Integration & Polish

### Cross-Story Integration

- [ ] T121 Run full system test: Install → init → process sample data (3 months, 10 stations)
- [ ] T122 Verify processing time: 3 months/10 stations completes in ~1 hour (8 cores)
- [ ] T123 Verify database size: SQLite database under 10GB for 1-year dataset
- [ ] T124 Verify memory usage: Parallel processing uses < 8GB with 8 workers

### Documentation Updates

- [ ] T125 [P] Update docs/content/user_guide/installation.rst: Document new `noiz init` command
- [ ] T126 [P] Update docs/content/user_guide/installation.rst: Document SQLite vs PostgreSQL choice
- [ ] T127 [P] Update docs/content/development/architecture.rst: Document ULID usage
- [ ] T128 [P] Update docs/content/development/architecture.rst: Document resume capability
- [ ] T129 [P] Create docs/content/development/python_versions.rst: Document Python 3.10-3.13 support
- [ ] T130 [P] Update README.rst: Update installation instructions
- [ ] T131 Run `just docs` to verify documentation builds

### Final Validation

- [ ] T132 Run all quality gates: `just unit_tests && just mypy && just ruff_check_ci && just ruff_format_check && just lint_docs && just docs`
- [ ] T133 Verify CI passes on all Python versions (3.10, 3.11, 3.12, 3.13)
- [ ] T134 Performance benchmark: Compare parallel speedup (should be linear up to 8 cores)
- [ ] T135 Create release notes: Document breaking changes and migration path

---

## Task Summary

**Total Tasks**: 135
**Parallelizable**: 47 tasks marked with [P]

### Tasks by User Story

- **Setup (Phase 1)**: 11 tasks
- **Foundation (Phase 2)**: 8 tasks
- **US1 (Installation & Setup)**: 17 tasks
- **US2 (Parallel Processing)**: 62 tasks
  - Models: 19 tasks
  - Migrations: 13 tasks
  - Workers: 7 tasks
  - Bulk Insert: 8 tasks
  - Resume: 6 tasks
  - Testing: 9 tasks
- **US5 (Python/Dependencies)**: 22 tasks
  - SQLAlchemy 2.0: 11 tasks
  - Flask 3.0: 3 tasks
  - ObsPy: 3 tasks
  - Validation: 5 tasks
- **Integration & Polish (Phase 6)**: 15 tasks

### Dependencies

**Blocking Dependencies** (must complete before others):
1. Phase 1 (Setup) → ALL other phases
2. Phase 2 (Foundation) → US1, US2, US5
3. US2 Part A (Add ULID fields) → US2 Part B (Migrations)
4. US2 Part B (Migrations) → US2 Parts C, D, E (Worker updates)
5. US5 Part A (SQLAlchemy 2.0) → US5 Part D (Validation)

**Parallel Opportunities**:
- Phase 1: All tasks can run in parallel (T001-T006)
- US2 Part A: All model updates can run in parallel (T037-T055)
- US5 Part A: API file conversions can run in parallel (T100-T106)
- Phase 6 Documentation: All doc updates can run in parallel (T125-T130)

### MVP Scope (Minimum Viable Product)

**For first release, complete**:
1. Phase 1 (Setup)
2. Phase 2 (Foundation)
3. US1 (Installation & Setup) - enables easy adoption
4. US2 (Parallel Processing) - fixes critical data loss bug

**Defer to v2**:
- US5 (Python 3.11-3.13) - nice to have, not blocking

**Estimated MVP Effort**: 6-8 weeks

---

## Implementation Strategy

### Week 1-2: Foundation
- Complete Phase 1 (Setup) and Phase 2 (Foundation)
- Establish SQLite backend support
- Create ULID mixin infrastructure

### Week 3-4: Installation (US1)
- Implement `noiz init` command
- Test SQLite database creation
- Validate installation flow

### Week 5-8: Parallel Processing Fix (US2)
- Week 5: Add ULID fields to all models
- Week 6: Create and test database migrations
- Week 7: Update worker functions and bulk insert logic
- Week 8: Implement resume detection and comprehensive testing

### Week 9-10: Integration & Polish
- System-wide integration testing
- Documentation updates
- Performance validation
- Release preparation

### Future (v2): Python Version Expansion (US5)
- SQLAlchemy 2.0 migration (incremental, can start in parallel)
- Flask 3.0 upgrade
- Python 3.11-3.13 validation

---

## Testing Strategy

### Per-Task Testing
Each task should be tested individually before moving to next task:
- Model changes: Unit tests for ULID generation
- Migrations: Test upgrade/downgrade on sample database
- Worker updates: Integration tests with small datasets
- API changes: Verify queries return correct results

### Story-Level Testing
After completing each user story:
- **US1**: Fresh install test on clean system
- **US2**: Parallel processing test with 1000+ tasks
- **US5**: Test suite on all Python versions

### Release Testing
Before merging to main:
- Full pipeline test: 3 months data, 10 stations
- Performance benchmark: Verify linear speedup
- Memory profiling: Confirm < 8GB usage
- Database validation: Check all foreign keys intact

---

## Risk Mitigation

### High Risk: SQLAlchemy 2.0 Migration (US5)
- Start early, run in parallel with other work
- Enable deprecation warnings immediately
- Test incrementally after each file conversion
- Maintain backward compatibility during transition

### Medium Risk: ULID Migration Complexity (US2)
- Test migrations on copy of production database
- Validate data integrity after migration
- Keep integer IDs during transition for safety
- Comprehensive rollback plan documented

### Medium Risk: ObsPy Python 3.11+ Support (US5)
- Monitor ObsPy releases weekly
- Maintain Python 3.10 support as fallback
- Test scientific accuracy on each version
- Document any version-specific behaviors

---

## Success Metrics

Upon completion of MVP (US1 + US2), verify:
- [ ] Installation success rate: 95%+ on first attempt
- [ ] Setup time: < 15 minutes from pip install to first processing
- [ ] Data loss rate: 0% in parallel processing (1000+ tasks tested)
- [ ] Resume capability: 100% success rate after interruption
- [ ] Parallel speedup: Linear up to 8 cores (7-8x faster than sequential)
- [ ] Memory usage: < 8GB for 8 parallel workers
- [ ] Database size: < 10GB for typical 1-year dataset
- [ ] All quality gates pass: tests, mypy, ruff, docs

---

**Next Steps**: Begin with Phase 1 (Setup) tasks T001-T011

# Tasks: ULID Primary Keys Migration

**Input**: Design documents from `/specs/005-ulid-primary-keys/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/model_contracts.md

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and documentation review

- [X] T001 Review research.md findings on ULID patterns and migration strategy
- [X] T002 Review data-model.md model inventory and dependency graph
- [X] T003 Review contracts/model_contracts.md for implementation contracts
- [X] T004 Review quickstart.md for common patterns and pitfalls

**Checkpoint**: Documentation reviewed - ready for foundational changes

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core ULIDMixin modification that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T005 Modify ULIDMixin to declare `id` as ULID primary key in src/noiz/models/mixins.py
- [X] T006 Remove explicit `id` column declaration from ULIDMixin (now provided by declared_attr)
- [X] T007 Update ULIDMixin __init__ to accept `id` parameter for explicit ULID generation
- [X] T008 Run mypy type check after ULIDMixin changes: `just mypy`
- [X] T009 Run ruff linting after ULIDMixin changes: `just ruff_check`

**Checkpoint**: ULIDMixin modified - all models can now be migrated

---

## Phase 3: User Story 1 - Developer Runs System Tests with SQLite (Priority: P1) 🎯 MVP

**Goal**: Enable SQLite as default test database by migrating all models to ULID primary keys, eliminating BigInteger autoincrement issues

**Independent Test**: Run `just run_system_tests_sqlite` and verify all tests pass without IntegrityError exceptions

### File Models Migration (No dependencies on other ULID models)

- [X] T010 [P] [US1] Update DatachunkFile model in src/noiz/models/datachunk.py (remove explicit id, verify ULIDMixin inheritance)
- [X] T011 [P] [US1] Update ProcessedDatachunkFile model in src/noiz/models/datachunk.py (remove explicit id)
- [X] T012 [P] [US1] Update CrosscorrelationCartesianFile model in src/noiz/models/crosscorrelation.py (remove explicit id)
- [X] T013 [P] [US1] Update CrosscorrelationCylindricalFile model in src/noiz/models/crosscorrelation.py (remove explicit id)
- [X] T014 [P] [US1] Update BeamformingFile model in src/noiz/models/beamforming.py (remove explicit id, verify FileModelMixin compatibility)
- [X] T015 [P] [US1] Update PPSDFile model in src/noiz/models/ppsd.py (remove explicit id, verify FileModelMixin compatibility)

### Result Models Migration - First Level (Depend on File models)

- [ ] T016 [US1] Update Datachunk model in src/noiz/models/datachunk.py (remove explicit id, consolidate dual FKs: rename file_ulid to datachunk_file_id, remove datachunk_file_id BigInteger)
- [ ] T017 [US1] Fix type bug: Change BeamformingResult.timespan_id from Integer to BigInteger in src/noiz/models/beamforming.py:99
- [ ] T018 [US1] Update BeamformingResult model in src/noiz/models/beamforming.py (remove explicit id, consolidate dual FKs: rename file_ulid to beamforming_file_id)
- [ ] T019 [US1] Update CrosscorrelationCartesian model in src/noiz/models/crosscorrelation.py (remove explicit id, consolidate dual FKs: rename file_ulid to crosscorrelation_cartesian_file_id)
- [ ] T020 [US1] Update CrosscorrelationCylindrical model in src/noiz/models/crosscorrelation.py (remove explicit id, consolidate dual FKs: rename file_ulid to crosscorrelation_cylindrical_file_id)
- [ ] T021 [US1] Fix type bug: Change PPSDResult.timespan_id from Integer to BigInteger in src/noiz/models/ppsd.py:38
- [ ] T022 [US1] Fix type bug: Change PPSDResult.datachunk_id from Integer to String(26) ULID FK in src/noiz/models/ppsd.py:39
- [ ] T023 [US1] Update PPSDResult model in src/noiz/models/ppsd.py (remove explicit id, consolidate dual FKs: rename file_ulid to ppsd_file_id, update datachunk FK)
- [ ] T024 [US1] Update CCFStack model in src/noiz/models/stacking.py (remove explicit id)

### Result Models Migration - Second Level (Depend on First Level)

- [ ] T025 [US1] Update ProcessedDatachunk model in src/noiz/models/datachunk.py (remove explicit id, consolidate dual FKs: rename file_ulid to processed_datachunk_file_id, update datachunk_id FK to ULID)

### Association Tables Migration

- [ ] T026 [P] [US1] Update crosscorrelation_association_table in src/noiz/models/crosscorrelation.py (change crosscorrelation_cartesian_id FK to String(26) ULID)
- [ ] T027 [P] [US1] Update crosscorrelation_datachunk_association_table in src/noiz/models/crosscorrelation.py (change crosscorrelation_cartesian_id and datachunk_id FKs to String(26) ULID)
- [ ] T028 [P] [US1] Update crosscorrelation_cylindrical_association_table in src/noiz/models/crosscorrelation.py (change crosscorrelation_cylindrical_id FK to String(26) ULID)
- [ ] T029 [P] [US1] Update crosscorrelation_cylindrical_datachunk_association_table in src/noiz/models/crosscorrelation.py (change crosscorrelation_cylindrical_id and datachunk_id FKs to String(26) ULID)
- [ ] T030 [P] [US1] Update beamforming_association_table in src/noiz/models/beamforming.py (change beamforming_result_id FK to String(26) ULID)
- [ ] T031 [P] [US1] Update ppsd_association_table in src/noiz/models/ppsd.py (change ppsd_result_id FK to String(26) ULID)
- [ ] T032 [P] [US1] Update stacking_association table in src/noiz/models/stacking.py (change crosscorrelation_cartesian_id and ccfstack_id FKs to String(26) ULID)

### Model Validation

- [ ] T033 [US1] Run mypy type check: `just mypy`
- [ ] T034 [US1] Run ruff linting: `just ruff_check`
- [ ] T035 [US1] Commit model changes: "refactor(models): Migrate all models to ULID primary keys"

### Migration Creation

- [ ] T036 [US1] Delete all existing migration files in migrations/versions/ (creating new baseline)
- [ ] T037 [US1] Generate new baseline migration: `uv run flask db migrate -m "Baseline migration with ULID primary keys"`
- [ ] T038 [US1] Review generated migration for correctness (all 13 models use String(26) id, no BigInteger primary keys)
- [ ] T039 [US1] Test migration on fresh SQLite database: `uv run flask db upgrade`
- [ ] T040 [US1] Test migration on fresh PostgreSQL database: `uv run flask db upgrade`
- [ ] T041 [US1] Test migration rollback: `uv run flask db downgrade`
- [ ] T042 [US1] Commit baseline migration: "feat(migrations): Create baseline migration with ULID primary keys"

### System Test Validation

- [ ] T043 [US1] Run system tests with SQLite: `just run_system_tests_sqlite`
- [ ] T044 [US1] Verify no IntegrityError exceptions related to autoincrement
- [ ] T045 [US1] Verify all ComponentFile inserts succeed with ULID primary keys
- [ ] T046 [US1] Verify parallel record creation works without conflicts
- [ ] T047 [US1] Run system tests with PostgreSQL: `DATABASE_URL=postgresql://... just run_system_tests`
- [ ] T048 [US1] Verify identical behavior between SQLite and PostgreSQL

**Checkpoint**: User Story 1 complete - SQLite is now functional as test database

---

## Phase 4: User Story 2 - Processing Pipeline Creates Records with Stable Identifiers (Priority: P1)

**Goal**: Update processing code to generate ULIDs explicitly and use renamed foreign key columns, ensuring parallel processing safety

**Independent Test**: Run `noiz processing prepare_datachunks --parallel` with 10 workers and verify all foreign key relationships are correctly established

### Processing Code Updates

- [ ] T049 [P] [US2] Update datachunk processing in src/noiz/processing/datachunk.py (change parameter name from ulid to id, update FK references: file_ulid → datachunk_file_id)
- [ ] T050 [P] [US2] Update beamforming processing in src/noiz/processing/beamforming.py (change parameter name from ulid to id, update FK references: file_ulid → beamforming_file_id)
- [ ] T051 [P] [US2] Update crosscorrelation processing in src/noiz/processing/crosscorrelations_cartesian.py (change parameter name from ulid to id, update FK references: file_ulid → crosscorrelation_cartesian_file_id)
- [ ] T052 [P] [US2] Update crosscorrelation cylindrical processing in src/noiz/processing/crosscorrelations_cylindrical.py (change parameter name from ulid to id, update FK references: file_ulid → crosscorrelation_cylindrical_file_id)
- [ ] T053 [P] [US2] Update PPSD processing in src/noiz/processing/ppsd.py (change parameter name from ulid to id, update FK references: file_ulid → ppsd_file_id)
- [ ] T054 [P] [US2] Update stacking processing in src/noiz/processing/stacking.py (update any ULID references if present)
- [ ] T055 [P] [US2] Update processed datachunk processing in src/noiz/processing/datachunk.py (update FK references for second-level processing)

### Code Pattern Validation

- [ ] T056 [US2] Search for remaining `.ulid` attribute access: `grep -r "\.ulid" src/noiz/processing/`
- [ ] T057 [US2] Search for old FK column names: `grep -r "file_ulid" src/noiz/processing/`
- [ ] T058 [US2] Verify all ULID generation uses `id=str(ULID())` pattern
- [ ] T059 [US2] Run mypy type check: `just mypy`
- [ ] T060 [US2] Run ruff linting: `just ruff_check`
- [ ] T061 [US2] Commit processing code updates: "refactor(processing): Update to use ULID primary keys and renamed FKs"

### API/CLI Updates (if needed)

- [ ] T062 [US2] Search for `.id` attribute access in API layer: `grep -r "\.id" src/noiz/api/`
- [ ] T063 [US2] Update any API queries that reference integer IDs to use ULID
- [ ] T064 [US2] Search for `.id` attribute access in CLI: `grep -r "\.id" src/noiz/cli.py`
- [ ] T065 [US2] Update any CLI commands that reference integer IDs to use ULID
- [ ] T066 [US2] Commit API/CLI updates if changes made: "refactor(api,cli): Update queries to use ULID identifiers"

### Parallel Processing Test Validation

- [ ] T067 [US2] Run datachunk preparation with 10 parallel workers: `noiz processing prepare_datachunks --parallel -b 100`
- [ ] T068 [US2] Verify all datachunk_file_id foreign keys established correctly
- [ ] T069 [US2] Verify no foreign key constraint violations during parallel inserts
- [ ] T070 [US2] Run beamforming workflow with parallel workers
- [ ] T071 [US2] Verify beamforming_file_id references work before file commit
- [ ] T072 [US2] Run cross-correlation workflow with parallel workers
- [ ] T073 [US2] Verify no primary key conflicts occur
- [ ] T074 [US2] Verify all relationships preserved in concurrent operations

**Checkpoint**: User Story 2 complete - Parallel processing uses ULID primary keys safely

---

## Phase 5: User Story 3 - Fresh Database Schema with ULID Primary Keys (Priority: P2)

**Goal**: Validate that fresh database creation works correctly with new baseline migration

**Independent Test**: Create empty database, run migrations, verify all tables have ULID primary keys

### Fresh Database Creation Tests

- [ ] T075 [US3] Create fresh SQLite database: `rm test_fresh.db && uv run flask db upgrade`
- [ ] T076 [US3] Inspect SQLite schema: `sqlite3 test_fresh.db ".schema"` and verify 13 tables have String(26) id columns
- [ ] T077 [US3] Verify no integer-based primary key columns exist in SQLite schema
- [ ] T078 [US3] Test insert operations on fresh SQLite database
- [ ] T079 [US3] Verify foreign key relationships work with ULID references in SQLite
- [ ] T080 [US3] Create fresh PostgreSQL database and run migrations
- [ ] T081 [US3] Inspect PostgreSQL schema and verify 13 tables have VARCHAR(26) id columns
- [ ] T082 [US3] Verify no integer-based primary key columns exist in PostgreSQL schema
- [ ] T083 [US3] Test insert operations on fresh PostgreSQL database
- [ ] T084 [US3] Verify foreign key relationships work with ULID references in PostgreSQL

### Schema Validation

- [ ] T085 [US3] Verify all ULID columns have unique constraints
- [ ] T086 [US3] Verify all ULID foreign keys have proper constraint names
- [ ] T087 [US3] Verify association tables have correct composite foreign keys
- [ ] T088 [US3] Test migration rollback on fresh database: `uv run flask db downgrade`
- [ ] T089 [US3] Verify database is empty after rollback
- [ ] T090 [US3] Re-apply migration: `uv run flask db upgrade`

**Checkpoint**: User Story 3 complete - Fresh databases work correctly with ULID schema

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final validation and documentation

### Comprehensive Testing

- [ ] T091 [P] Run full unit test suite: `just unit_tests`
- [ ] T092 [P] Run full system test suite on SQLite: `just run_system_tests_sqlite`
- [ ] T093 [P] Run full system test suite on PostgreSQL (if available): `DATABASE_URL=postgresql://... just run_system_tests`
- [ ] T094 Verify test coverage meets project standards
- [ ] T095 Verify no regression in existing functionality

### Code Quality

- [ ] T096 [P] Final mypy type check: `just mypy`
- [ ] T097 [P] Final ruff check: `just ruff_check`
- [ ] T098 [P] Final ruff format check: `just ruff_format_check`
- [ ] T099 Run doc8 documentation linting: `just lint_docs`

### Performance Validation

- [ ] T100 Measure identifier lookup performance in SQLite vs old schema
- [ ] T101 Measure identifier lookup performance in PostgreSQL vs old schema
- [ ] T102 Verify performance degradation is <10% per success criteria
- [ ] T103 Run parallel processing benchmark with 10 workers
- [ ] T104 Verify no performance regression in parallel workflows

### Documentation Updates

- [ ] T105 [P] Update any documentation that references integer IDs
- [ ] T106 [P] Update CLAUDE.md if needed with ULID migration context
- [ ] T107 [P] Add migration notes to project documentation
- [ ] T108 Review and finalize specs/005-ulid-primary-keys/quickstart.md

### Final Validation

- [ ] T109 Run quickstart.md validation checklist
- [ ] T110 Verify all success criteria from spec.md are met
- [ ] T111 Create summary of changes for project team
- [ ] T112 Final commit: "docs: Update documentation for ULID primary keys migration"

**Checkpoint**: All tasks complete - ULID migration fully implemented and validated

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational phase - Can start after T009
- **User Story 2 (Phase 4)**: Depends on User Story 1 completion - Needs model changes from US1
- **User Story 3 (Phase 5)**: Depends on User Story 1 completion - Needs baseline migration from US1
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Foundation → Models → Migrations → Tests (MUST complete first)
- **User Story 2 (P1)**: Depends on US1 (needs model changes and migrations)
- **User Story 3 (P2)**: Depends on US1 (needs baseline migration)

### Critical Path

```
T001-T004 (Setup) → T005-T009 (ULIDMixin) →
T010-T025 (Models) → T026-T032 (Associations) → T033-T042 (Migration) → T043-T048 (US1 Tests) →
T049-T066 (US2 Processing) → T067-T074 (US2 Tests) →
T075-T090 (US3 Fresh DB) →
T091-T112 (Polish)
```

### Parallel Opportunities

**Within User Story 1**:
- T010-T015: All file models can be updated in parallel
- T026-T032: All association tables can be updated in parallel
- T043-T046 and T047-T048: SQLite and PostgreSQL tests can run in parallel

**Within User Story 2**:
- T049-T055: All processing files can be updated in parallel
- T062-T065: API and CLI searches can run in parallel

**Within Phase 6**:
- T091-T093: Different test suites can run in parallel
- T096-T098: Code quality checks can run in parallel
- T105-T108: Documentation updates can run in parallel

---

## Parallel Example: User Story 1 Models

```bash
# Launch all file model updates together:
Task: "Update DatachunkFile model in src/noiz/models/datachunk.py"
Task: "Update ProcessedDatachunkFile model in src/noiz/models/datachunk.py"
Task: "Update CrosscorrelationCartesianFile model in src/noiz/models/crosscorrelation.py"
Task: "Update CrosscorrelationCylindricalFile model in src/noiz/models/crosscorrelation.py"
Task: "Update BeamformingFile model in src/noiz/models/beamforming.py"
Task: "Update PPSDFile model in src/noiz/models/ppsd.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T004)
2. Complete Phase 2: Foundational (T005-T009) - CRITICAL
3. Complete Phase 3: User Story 1 (T010-T048)
4. **STOP and VALIDATE**: Run `just run_system_tests_sqlite` and verify SQLite works
5. MVP complete: SQLite is now usable as test database

### Full Implementation

1. Complete MVP (Phases 1-3)
2. Add User Story 2 (T049-T074): Processing code updates
3. Add User Story 3 (T075-T090): Fresh database validation
4. Complete Polish (T091-T112): Final validation and documentation

### Incremental Commits

- Commit after T009: ULIDMixin modification
- Commit after T035: Model changes
- Commit after T042: Baseline migration
- Commit after T061: Processing code updates
- Commit after T066: API/CLI updates (if any)
- Commit after T112: Documentation updates

---

## Notes

- [P] tasks = different files, no dependencies, can run in parallel
- [Story] label maps task to specific user story for traceability
- All model updates in US1 follow dependency order: File models → First-level results → Second-level results
- Type bugs discovered (BeamformingResult.timespan_id, PPSDResult.timespan_id, PPSDResult.datachunk_id) will be fixed during migration
- Association tables updated after all models to ensure correct foreign key types
- US2 cannot start until US1 model changes and migration are complete
- US3 can run in parallel with US2 since both only depend on US1
- Performance validation (T100-T104) confirms <10% degradation requirement

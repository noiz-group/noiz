# Tasks: Remove Named Database Constraints

**Input**: Design documents from `/specs/006-unnamed-constraints/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md

**Organization**: Tasks are grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions
- **Single project**: `src/noiz/`, `tests/` at repository root
- Paths follow existing project structure

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Backup existing implementation before making changes

- [x] T001 Create feature branch if not already on it: `git checkout 006-unnamed-constraints`
- [x] T002 [P] Document current CONSTRAINT_TO_COLUMNS mappings for reference in specs/006-unnamed-constraints/constraint-mappings-backup.md
- [x] T003 [P] Run existing test suite to establish baseline: `just unit_tests`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core changes that enable all user stories

**⚠️ CRITICAL**: All user stories depend on these foundational changes

- [x] T004 Add ConstraintViolationError exception class in src/noiz/exceptions.py
- [x] T005 Add parse_constraint_violation function for PostgreSQL error parsing in src/noiz/database.py
- [x] T006 Add parse_constraint_violation function for SQLite error parsing in src/noiz/database.py
- [x] T007 Add enhance_constraint_error function in src/noiz/database.py

**Checkpoint**: Error handling foundation ready - user story implementation can now begin

---

## Phase 3: User Story 1 - Simplified Database Operations (Priority: P1) 🎯 MVP

**Goal**: Enable developers to perform database operations without dialect-specific constraint handling. Remove translation layer and update models/upserts to use column-based conflict resolution.

**Independent Test**: Run existing database operations against both PostgreSQL and SQLite backends and verify all upsert operations succeed without the translation layer.

### Step 1: Update Model Constraints (Remove name= parameters)

**Component Models** (3 constraints):

- [x] T008 [P] [US1] Remove name="unique_device_per_station" from Device model in src/noiz/models/component.py
- [x] T009 [P] [US1] Remove name="unique_component_per_station" from Component model in src/noiz/models/component.py
- [x] T010 [P] [US1] Remove name="single_component_pair" from ComponentPairCartesian model in src/noiz/models/component_pair.py

**Timespan Models** (4 constraints):

- [x] T011 [P] [US1] Remove name="unique_starttime" from Timespan model in src/noiz/models/timespan.py
- [x] T012 [P] [US1] Remove name="unique_midtime" from Timespan model in src/noiz/models/timespan.py
- [x] T013 [P] [US1] Remove name="unique_endtime" from Timespan model in src/noiz/models/timespan.py
- [x] T014 [P] [US1] Remove name="unique_times" from Timespan model in src/noiz/models/timespan.py

**Processing Results Models** (6 constraints):

- [x] T015 [P] [US1] Remove name="unique_ppsd_per_config_per_datachunk" from PPSDResult model in src/noiz/models/ppsd.py
- [x] T016 [P] [US1] Remove name="unique_beam_per_config_per_timespan" from BeamformingResult model in src/noiz/models/beamforming.py
- [x] T017 [P] [US1] Remove name="unique_qcone_results_per_config_per_datachunk" from QCOneResults model in src/noiz/models/qc.py
- [x] T018 [P] [US1] Remove name="unique_qctwo_results_per_config_per_ccf" from QCTwoResults model in src/noiz/models/qc.py
- [x] T019 [P] [US1] Remove name="unique_ccfn_per_timespan_per_componentpair_per_config" from CrosscorrelationCartesian model in src/noiz/models/crosscorrelation.py
- [x] T020 [P] [US1] Remove name="unique_ccfcylindrical_per_timespan_cylindrical_per_config" from CrosscorrelationCylindrical model in src/noiz/models/crosscorrelation.py

**SOH Models** (3 constraints):

- [x] T021 [P] [US1] Remove name="unique_timestamp_per_station_in_sohinstrument" from SOHInstrument model in src/noiz/models/soh.py
- [x] T022 [P] [US1] Remove name="unique_timestamp_per_station_in_sohgps" from SOHGPS model in src/noiz/models/soh.py
- [x] T023 [P] [US1] Remove name="unique_tispan_per_station_in_avgsohgps" from AveragedSOHGPS model in src/noiz/models/soh.py

### Step 2: Update Upsert Operations (Use index_elements instead of constraint_name)

**Datachunk API** (3 upsert commands):

- [ ] T024 [P] [US1] Update _prepare_upsert_command_datachunk to use index_elements=["timespan_id","component_id","datachunk_params_id"] in src/noiz/api/datachunk.py
- [ ] T025 [P] [US1] Update _prepare_upsert_command_datachunk_stats to use index_elements in src/noiz/api/datachunk.py
- [ ] T026 [P] [US1] Update _prepare_upsert_command_processed_datachunk to use index_elements in src/noiz/api/datachunk.py

**Other API Files** (3 upsert commands):

- [ ] T027 [P] [US1] Update _prepare_upsert_command_ppsd to use index_elements=["datachunk_id","ppsd_params_id"] in src/noiz/api/ppsd.py
- [ ] T028 [P] [US1] Update _prepare_upsert_command_event_detection (2 calls) to use index_elements in src/noiz/api/event_detection.py

### Step 3: Validation for User Story 1

- [ ] T029 [US1] Run mypy type checking: `just mypy`
- [ ] T030 [US1] Run ruff linting: `just ruff_check`
- [ ] T031 [US1] Verify no named constraints remain: `grep -r "UniqueConstraint.*name=" src/noiz/models/` (should return no results)
- [ ] T032 [US1] Run unit tests for both backends: `just unit_tests`

**Checkpoint**: At this point, all models use unnamed constraints and all upsert operations use index_elements. The translation layer is still present but unused.

---

## Phase 4: User Story 3 - Code Cleanup (Priority: P3)

**Goal**: Remove dialect-specific constraint translation logic to simplify codebase.

**Independent Test**: Code review showing removal of CONSTRAINT_TO_COLUMNS mapping and dialect_agnostic_on_conflict function, with all tests passing.

**Note**: This is prioritized after US1 because US1 must work first. US2 (Migration) is independent and can proceed in parallel.

### Remove Translation Layer

- [ ] T033 [US3] Remove CONSTRAINT_TO_COLUMNS dictionary (19 mappings) from src/noiz/database.py
- [ ] T034 [US3] Remove dialect_agnostic_on_conflict function from src/noiz/database.py
- [ ] T035 [US3] Update imports in src/noiz/api/datachunk.py (remove dialect_agnostic_on_conflict)
- [ ] T036 [US3] Update imports in src/noiz/api/ppsd.py (remove dialect_agnostic_on_conflict)
- [ ] T037 [US3] Update imports in src/noiz/api/event_detection.py (remove dialect_agnostic_on_conflict)

### Enhance Bulk Operations with Error Handling

- [ ] T038 [US3] Update bulk_add_objects to catch and enhance IntegrityError in src/noiz/api/helpers.py
- [ ] T039 [US3] Update bulk_merge_objects to catch and enhance IntegrityError in src/noiz/api/helpers.py
- [ ] T040 [US3] Update _run_upsert_commands to log enhanced errors in src/noiz/api/helpers.py

### Validation for User Story 3

- [ ] T041 [US3] Verify CONSTRAINT_TO_COLUMNS removed: `grep "CONSTRAINT_TO_COLUMNS" src/noiz/database.py` (should return no results)
- [ ] T042 [US3] Verify dialect_agnostic_on_conflict removed: `grep "dialect_agnostic_on_conflict" src/noiz/` (should return no results)
- [ ] T043 [US3] Measure code reduction in src/noiz/database.py: `git diff --stat`
- [ ] T044 [US3] Run full test suite for both backends: `just unit_tests`
- [ ] T045 [US3] Run ruff code quality checks: `just ruff`

**Checkpoint**: Translation layer completely removed, error enhancement in place, all tests passing.

---

## Phase 5: User Story 2 - Migration Compatibility (Priority: P2)

**Goal**: Create fresh baseline migration that defines all constraints as unnamed, ensuring clean schema generation for both PostgreSQL and SQLite.

**Independent Test**: Apply fresh baseline migration to empty databases and load sample production-like data to verify all constraints enforce uniqueness correctly in both PostgreSQL and SQLite.

**Note**: This can proceed in parallel with US1/US3 or after US1 is complete.

### Create Test Fixtures

- [ ] T046 [P] [US2] Create test fixture for simple uniqueness (Device, Component, ComponentPair) in tests/fixtures/constraint_test_data.py
- [ ] T047 [P] [US2] Create test fixture for temporal uniqueness (Timespan, SOH records) in tests/fixtures/constraint_test_data.py
- [ ] T048 [P] [US2] Create test fixture for result uniqueness (Datachunk, CrossCorrelation, QC, PPSD) in tests/fixtures/constraint_test_data.py

### Generate Fresh Baseline Migration

- [ ] T049 [US2] Delete existing baseline migration: `rm migrations/versions/5333b2f172f3_baseline_mixed_ids_ulids_for_data_.py`
- [ ] T050 [US2] Generate new baseline migration: `uv run flask db revision --autogenerate -m "baseline unnamed constraints"`
- [ ] T051 [US2] Review generated migration for correctness in migrations/versions/[new]_baseline_unnamed_constraints.py

### Test Migration on Both Backends

- [ ] T052 [US2] Test PostgreSQL migration on fresh database: `DATABASE_BACKEND=postgresql uv run flask db upgrade`
- [ ] T053 [US2] Test SQLite migration on fresh database: `DATABASE_BACKEND=sqlite uv run flask db upgrade`
- [ ] T054 [US2] Load production-like test data into PostgreSQL test database
- [ ] T055 [US2] Load production-like test data into SQLite test database
- [ ] T056 [US2] Verify constraint enforcement in PostgreSQL: attempt duplicate inserts, confirm rejections
- [ ] T057 [US2] Verify constraint enforcement in SQLite: attempt duplicate inserts, confirm rejections

### Create Integration Tests

- [ ] T058 [P] [US2] Create test_constraint_enforcement_postgresql.py in tests/integration/ for PostgreSQL-specific constraint tests
- [ ] T059 [P] [US2] Create test_constraint_enforcement_sqlite.py in tests/integration/ for SQLite-specific constraint tests
- [ ] T060 [P] [US2] Create test_error_message_enhancement.py in tests/unit/ for error parsing tests
- [ ] T061 [US2] Create parametrized cross-backend test in tests/integration/test_constraint_consistency.py

### Validation for User Story 2

- [ ] T062 [US2] Verify migration applies cleanly to PostgreSQL: `DATABASE_BACKEND=postgresql uv run flask db upgrade`
- [ ] T063 [US2] Verify migration applies cleanly to SQLite: `DATABASE_BACKEND=sqlite uv run flask db upgrade`
- [ ] T064 [US2] Run constraint enforcement tests: `pytest tests/integration/test_constraint_*`
- [ ] T065 [US2] Run error message enhancement tests: `pytest tests/unit/test_error_message_enhancement.py`
- [ ] T066 [US2] Verify all system tests pass: `just run_system_tests`

**Checkpoint**: Fresh baseline migration created and validated on both backends with production-like data.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, documentation, and cleanup

### Documentation

- [ ] T067 [P] Create migration guide for existing developers in specs/006-unnamed-constraints/MIGRATION.md
- [ ] T068 [P] Update CLAUDE.md if needed with any new patterns or conventions
- [ ] T069 [P] Verify quickstart.md examples match implemented code

### Final Validation

- [ ] T070 Verify all success criteria from spec.md:
  - SC-001: `grep -r "UniqueConstraint.*name=" src/noiz/models/` returns no results (16 constraints converted)
  - SC-002: `grep "CONSTRAINT_TO_COLUMNS" src/noiz/database.py` returns no results
  - SC-003: All integration tests pass for both backends
  - SC-004: Fresh baseline migration applies successfully
  - SC-005: Code complexity reduced (~100 lines)
  - SC-006: All tests pass for both backends
  - SC-007: Constraint violation error messages include column names

- [ ] T071 Run complete test suite on both backends: `just unit_tests` (PostgreSQL and SQLite)
- [ ] T072 Run full system tests: `just run_system_tests`
- [ ] T073 Run all code quality checks: `just mypy && just ruff && just lint_docs`
- [ ] T074 Verify documentation builds: `just docs`

### Code Review & Cleanup

- [ ] T075 Review all changes for code quality and consistency
- [ ] T076 Ensure all commits follow project conventions (small, incremental, well-documented)
- [ ] T077 Update specs/006-unnamed-constraints/tasks.md to mark all tasks complete

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational (Phase 2) - No dependencies on other stories
- **User Story 3 (Phase 4)**: Depends on User Story 1 (Phase 3) - US1 must work before removing translation layer
- **User Story 2 (Phase 5)**: Depends on Foundational (Phase 2) - Can proceed in parallel with US1/US3 or after US1
- **Polish (Phase 6)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1) - Simplified Operations**: Foundation → US1
  - Converts all constraints to unnamed
  - Updates all upsert operations to use index_elements
  - Translation layer becomes unused but remains

- **User Story 3 (P3) - Code Cleanup**: US1 → US3
  - Removes translation layer (only safe after US1 works)
  - Adds error enhancement to bulk operations
  - Must come after US1 to avoid breaking active code

- **User Story 2 (P2) - Migration**: Foundation → US2 (independent of US1/US3)
  - Can proceed in parallel with US1/US3 if team capacity allows
  - Or can proceed after US1 is validated
  - Creates fresh baseline with unnamed constraints
  - Tests enforcement on both backends

### Within Each User Story

**User Story 1**:
1. Update all models (T008-T023) in parallel
2. Then update upsert operations (T024-T028) in parallel
3. Then run validation (T029-T032) sequentially

**User Story 3**:
1. Remove translation layer (T033-T037) sequentially
2. Add error enhancement (T038-T040) sequentially
3. Then run validation (T041-T045) sequentially

**User Story 2**:
1. Create test fixtures (T046-T048) in parallel
2. Then generate migration (T049-T051) sequentially
3. Then test on both backends (T052-T057) sequentially
4. Create integration tests (T058-T061) in parallel
5. Then run validation (T062-T066) sequentially

### Parallel Opportunities

**Phase 1** (Setup): T002 and T003 can run in parallel

**Phase 2** (Foundational): T005 and T006 can run in parallel (different functions)

**Phase 3** (US1):
- All model updates (T008-T023) can run in parallel - different files
- All upsert updates (T024-T028) can run in parallel - different files

**Phase 4** (US3):
- Import updates (T035-T037) can run in parallel - different files
- Bulk operation updates (T038-T040) must be sequential - same file

**Phase 5** (US2):
- Test fixture creation (T046-T048) can run in parallel - same file, different functions
- Integration test creation (T058-T061) can run in parallel - different files

**Phase 6** (Polish):
- Documentation tasks (T067-T069) can run in parallel - different files

---

## Parallel Example: User Story 1 (Model Updates)

```bash
# Launch all model constraint updates together:
# Component models (3 files):
Task: "Remove name= from Device in src/noiz/models/component.py"
Task: "Remove name= from Component in src/noiz/models/component.py"
Task: "Remove name= from ComponentPairCartesian in src/noiz/models/component_pair.py"

# Timespan models (1 file, 4 constraints):
Task: "Remove 4 name= parameters from Timespan in src/noiz/models/timespan.py"

# Processing results (4 files, 6 constraints):
Task: "Remove name= from PPSDResult in src/noiz/models/ppsd.py"
Task: "Remove name= from BeamformingResult in src/noiz/models/beamforming.py"
Task: "Remove 2 name= from QC models in src/noiz/models/qc.py"
Task: "Remove 2 name= from CrossCorrelation models in src/noiz/models/crosscorrelation.py"

# SOH models (1 file, 3 constraints):
Task: "Remove 3 name= parameters from SOH models in src/noiz/models/soh.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (error handling infrastructure)
3. Complete Phase 3: User Story 1 (simplified operations)
4. **STOP and VALIDATE**: Run all tests, verify upserts work without translation layer
5. Commit with message: "feat(constraints): Convert to unnamed constraints with index_elements"

### Incremental Delivery

1. **Checkpoint 1**: Setup + Foundational → Error handling ready
2. **Checkpoint 2**: Add User Story 1 → Test independently → Models and upserts updated
3. **Checkpoint 3**: Add User Story 3 → Test independently → Translation layer removed
4. **Checkpoint 4**: Add User Story 2 → Test independently → Migration validated
5. **Final**: Polish → All validation passes → Ready for PR

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - **Developer A**: User Story 1 (model updates + upsert updates)
   - **Developer B**: User Story 2 (migration + test fixtures) - can proceed in parallel
   - Once Developer A completes US1:
     - **Developer C**: User Story 3 (remove translation layer) - depends on US1
3. All converge for Polish phase

### Single Developer Strategy

1. Setup (T001-T003)
2. Foundational (T004-T007)
3. User Story 1 in batches:
   - Batch 1: Update component models (T008-T010), run tests
   - Batch 2: Update timespan models (T011-T014), run tests
   - Batch 3: Update processing models (T015-T020), run tests
   - Batch 4: Update SOH models (T021-T023), run tests
   - Batch 5: Update upsert operations (T024-T028), run tests
   - Validate US1 (T029-T032)
   - **Commit**: "feat(constraints): Convert to unnamed constraints"
4. User Story 3:
   - Remove translation layer (T033-T037)
   - Add error enhancement (T038-T040)
   - Validate US3 (T041-T045)
   - **Commit**: "refactor(database): Remove constraint translation layer"
5. User Story 2:
   - Create fixtures (T046-T048)
   - Generate migration (T049-T051)
   - Test migration (T052-T066)
   - **Commit**: "feat(migration): Add baseline migration with unnamed constraints"
6. Polish (T067-T077)
   - **Commit**: "docs: Add migration guide and update documentation"

---

## Task Summary

**Total Tasks**: 77

**By Phase**:
- Phase 1 (Setup): 3 tasks
- Phase 2 (Foundational): 4 tasks
- Phase 3 (User Story 1): 25 tasks (16 model updates + 5 upsert updates + 4 validation)
- Phase 4 (User Story 3): 13 tasks (5 removal + 3 enhancement + 5 validation)
- Phase 5 (User Story 2): 21 tasks (3 fixtures + 3 migration + 6 testing + 4 integration tests + 5 validation)
- Phase 6 (Polish): 11 tasks

**Parallel Opportunities**: 35 tasks marked [P] can run in parallel within their phases

**Independent Test Criteria**:
- US1: All upsert operations succeed without translation layer on both backends
- US2: Fresh migration applies cleanly and enforces all constraints on both backends
- US3: Translation layer removed, error enhancement working, all tests pass

**Suggested MVP Scope**: Phases 1-3 (Setup + Foundational + User Story 1)

---

## Notes

- [P] tasks = different files, no dependencies within their phase
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Commit after logical groups of tasks (see Implementation Strategy)
- Stop at any checkpoint to validate story independently
- Run tests frequently (after each batch of changes)
- Follow project constitution: small commits, type safety, test coverage

**Format Validation**: ✅ All tasks follow checklist format with checkbox, ID, optional [P]/[Story] labels, description, and file paths

# Implementation Tasks: Configuration System Refactor

**Feature**: Configuration System Refactor
**Branch**: `007-config-system-refactor`
**Date**: 2025-10-25

## Overview

This document breaks down the configuration system refactor into actionable, dependency-ordered tasks organized by user story. Each user story represents an independently testable increment of functionality.

**Task Format**: `- [ ] [TaskID] [P?] [Story?] Description with file path`
- **[P]**: Parallelizable (can run concurrently with other [P] tasks)
- **[Story]**: User story label (US1, US2, etc.) for traceability

**Total Tasks**: 45
**Parallel Opportunities**: 18 parallelizable tasks identified

## Implementation Strategy

**MVP Scope**: User Stories 1 & 2 (P1) - Core configuration and Flask integration
**Incremental Delivery**: Each user story can be independently developed, tested, and delivered
**Parallel Execution**: Tasks marked [P] can run concurrently within their phase

---

## Phase 1: Setup & Dependencies

**Goal**: Initialize project with required dependencies and foundational infrastructure

**Tasks**:

- [X] T001 Add pydantic-settings dependency via uv add "pydantic-settings>=2.0,<3.0"
- [X] T002 Run uv sync --all-groups to install all dependencies
- [X] T003 Verify pydantic-settings installation with python -c "import pydantic_settings"

**Completion Criteria**: Dependencies installed and importable

---

## Phase 2: Foundational - Core Configuration Module

**Goal**: Create the core configuration module that all user stories depend on

**Tasks**:

- [X] T004 [P] Create src/noiz/config.py with module structure and imports
- [X] T005 [P] Implement DatabaseConfig(BaseModel) class in src/noiz/config.py
- [X] T006 [P] Implement ProcessingConfig(BaseModel) class in src/noiz/config.py
- [X] T007 Implement NoizConfig(BaseSettings) root class in src/noiz/config.py
- [X] T008 Add get_config(reload=False) singleton function in src/noiz/config.py
- [X] T009 [P] Implement DatabaseConfig validation logic (URI precedence, backend requirements) in src/noiz/config.py
- [X] T010 [P] Implement ProcessingConfig validation logic (directory auto-create, file/non-empty checks) in src/noiz/config.py
- [X] T011 [P] Add SecretStr masking for passwords in DatabaseConfig in src/noiz/config.py
- [X] T012 [P] Add structured logging (INFO/WARN/ERROR) throughout validation in src/noiz/config.py
- [X] T013 Implement to_flask_config() method in NoizConfig in src/noiz/config.py
- [X] T014 Add custom __repr__() with sensitive field masking in NoizConfig in src/noiz/config.py
- [X] T015 [P] Create tests/unit/test_config.py with test structure
- [X] T016 [P] Add test for valid PostgreSQL config with DATABASE_URL in tests/unit/test_config.py
- [X] T017 [P] Add test for valid PostgreSQL config with POSTGRES_* vars in tests/unit/test_config.py
- [X] T018 [P] Add test for valid SQLite config in tests/unit/test_config.py
- [X] T019 [P] Add test for URI precedence behavior in tests/unit/test_config.py
- [X] T020 [P] Add test for directory validation (auto-create) in tests/unit/test_config.py
- [X] T021 [P] Add test for directory validation (file fail) in tests/unit/test_config.py
- [X] T022 [P] Add test for directory validation (non-empty dir fail) in tests/unit/test_config.py
- [X] T023 [P] Add test for directory validation (empty dir pass) in tests/unit/test_config.py
- [X] T024 [P] Add test for missing required fields in tests/unit/test_config.py
- [X] T025 [P] Add test for secret masking in repr/logs in tests/unit/test_config.py
- [X] T026 Run just unit_tests to verify config module tests pass

**Completion Criteria**: Config module fully implemented and all unit tests passing

---

## Phase 3: User Story 1 - CLI User Configures Application (P1)

**Story Goal**: Enable CLI users to configure Noiz via NOIZ_* environment variables with validation and clear error messages

**Independent Test**: Run `noiz processing prepare_datachunks` with NOIZ_* environment variables and verify successful execution with proper config validation

**Tasks**:

- [X] T027 [US1] Update src/noiz/app.py to import get_config from noiz.config
- [X] T028 [US1] Update create_app() in src/noiz/app.py to call get_config(reload=True)
- [X] T029 [US1] Update create_app() in src/noiz/app.py to use config.to_flask_config() for app.config population
- [X] T030 [US1] Store config object in app.config['NOIZ_CONFIG'] in src/noiz/app.py
- [X] T031 [US1] Remove old app.config.from_object("noiz.settings") call in src/noiz/app.py
- [X] T032 [US1] Update src/noiz/settings.py to import get_config and expose backward compat variables
- [X] T033 [US1] Remove old environs usage from src/noiz/settings.py
- [X] T034 [US1] Update src/noiz/database.py to remove env var reading (use Flask app context)
- [X] T035 [US1] Update src/noiz/globals.py PROCESSED_DATA_DIR proxy for new config system
- [X] T036 [US1] Update tests/conftest.py to use NOIZ_* prefix for environment variables
- [ ] T037 [US1] Run just unit_tests to verify CLI configuration works
- [ ] T038 [US1] Test CLI command with NOIZ_* env vars: noiz --help
- [ ] T039 [US1] Test CLI command with missing required var for error validation

**Completion Criteria**: CLI commands work with NOIZ_* environment variables, validation errors are clear

---

## Phase 4: User Story 2 - Notebook User Initializes Flask App (P1)

**Story Goal**: Enable notebook users to call create_app() with Flask integration and config reload support

**Independent Test**: Create notebook calling `from noiz.app import create_app; app = create_app()` and verify config loaded and database connection works

**Tasks**:

- [ ] T040 [P] [US2] Create tests/integration/test_config_integration.py with test structure
- [ ] T041 [P] [US2] Add test for create_app() with valid config in tests/integration/test_config_integration.py
- [ ] T042 [P] [US2] Add test for config reload between create_app() calls in tests/integration/test_config_integration.py
- [ ] T043 [P] [US2] Add test for Flask config keys populated correctly in tests/integration/test_config_integration.py
- [ ] T044 [P] [US2] Add test for database connection using config in tests/integration/test_config_integration.py
- [ ] T045 [US2] Run just unit_tests to verify Flask integration tests pass
- [ ] T046 [US2] Test notebook pattern: create app, change env vars, create app again, verify reload

**Completion Criteria**: Flask app factory works in notebooks with config reload support, all integration tests pass

---

## Phase 5: User Story 4 - CI Pipeline Uses New Configuration (P1)

**Story Goal**: Update CI pipeline to use NOIZ_* environment variables across all stages

**Independent Test**: Push to branch and verify all CI stages (testing, linting, documentation, system-testing) pass with new NOIZ_* variables

**Tasks**:

- [X] T047 [P] [US4] Update .gitlab-ci.yml to use NOIZ_DATABASE_URL instead of DATABASE_URL
- [X] T048 [P] [US4] Update .gitlab-ci.yml to use NOIZ_PROCESSED_DATA_DIR instead of PROCESSED_DATA_DIR
- [X] T049 [P] [US4] Update .gitlab-ci.yml to use NOIZ_MSEEDINDEX_EXECUTABLE
- [X] T050 [P] [US4] Update .gitlab/templates/linting.yml with NOIZ_* prefix for all env vars
- [X] T051 [P] [US4] Update .gitlab/templates/documentation.yml with NOIZ_* prefix for all env vars
- [X] T052 [P] [US4] Review other .gitlab/templates/ files for env var usage and update
- [ ] T053 [US4] Commit CI configuration changes
- [ ] T054 [US4] Push to branch and verify CI pipeline runs
- [ ] T055 [US4] Verify all CI stages pass (testing, linting, documentation)
- [ ] T056 [US4] Check CI logs for proper config loading and no password leaks

**Completion Criteria**: CI pipeline passes 100% with NOIZ_* environment variables, no sensitive data in logs

---

## Phase 6: User Story 3 - Developer Documents Available Configuration (P2)

**Story Goal**: Provide code-level documentation for configuration variables (docstrings, type hints)

**Independent Test**: Review src/noiz/config.py docstrings and verify all config variables are documented with purpose, type, default, required status

**Tasks**:

- [ ] T057 [P] [US3] Add comprehensive docstrings to NoizConfig class in src/noiz/config.py
- [ ] T058 [P] [US3] Add comprehensive docstrings to DatabaseConfig class in src/noiz/config.py
- [ ] T059 [P] [US3] Add comprehensive docstrings to ProcessingConfig class in src/noiz/config.py
- [ ] T060 [P] [US3] Add field descriptions using Field(description="...") for all config fields in src/noiz/config.py
- [ ] T061 [P] [US3] Add docstring to get_config() function explaining reload parameter in src/noiz/config.py
- [ ] T062 [US3] Run just mypy to verify type hints are complete

**Completion Criteria**: All config classes have comprehensive docstrings, Field descriptions for every variable

---

## Phase 7: User Story 5 - User Reads Configuration Documentation (P2)

**Story Goal**: Create user-facing RST documentation for configuration system with .env guide

**Independent Test**: Follow documentation from scratch to configure Noiz and verify new user can complete in <15 minutes

**Tasks**:

- [X] T063 [US5] Create docs/content/user_guide/configuration.rst with document structure
- [X] T064 [P] [US5] Write "Overview" section in docs/content/user_guide/configuration.rst
- [X] T065 [P] [US5] Write "Required Variables" section in docs/content/user_guide/configuration.rst
- [X] T066 [P] [US5] Write "Optional Variables" section in docs/content/user_guide/configuration.rst
- [X] T067 [P] [US5] Write "Database Configuration" section in docs/content/user_guide/configuration.rst
- [X] T068 [P] [US5] Write "Using .env Files" section with shell commands in docs/content/user_guide/configuration.rst
- [X] T069 [P] [US5] Write "Flask Integration" section for notebooks in docs/content/user_guide/configuration.rst
- [X] T070 [P] [US5] Write "CI/CD Configuration" section in docs/content/user_guide/configuration.rst
- [X] T071 [P] [US5] Write "Migration Guide" section (old to new env var names) in docs/content/user_guide/configuration.rst
- [X] T072 [US5] Add configuration.rst to appropriate docs/content/index.rst toctree
- [X] T073 [US5] Run just docs to build documentation
- [X] T074 [US5] Run just lint_docs to validate RST formatting
- [X] T075 [US5] Review rendered HTML documentation for clarity

**Completion Criteria**: Comprehensive RST documentation with .env examples, builds without warnings, readable by new users

---

## Phase 8: User Story 6 - System Identifies Unused Environment Variables (P3)

**Story Goal**: Document deprecated/unused environment variables in code and documentation

**Independent Test**: Review config module and documentation for list of deprecated variables with migration notes

**Tasks**:

- [ ] T076 [P] [US6] Add comment block in src/noiz/config.py documenting deprecated variables (CELERY_*, SECRET_KEY, etc.)
- [ ] T077 [P] [US6] Add "Deprecated Variables" section to docs/content/user_guide/configuration.rst
- [ ] T078 [P] [US6] Document CELERY_BROKER_URL and CELERY_RESULT_BACKEND as unused in documentation
- [ ] T079 [P] [US6] Document SECRET_KEY, BCRYPT_LOG_ROUNDS, WEBPACK_MANIFEST_PATH as removed in documentation
- [ ] T080 [US6] Update documentation build and verify deprecated section renders correctly

**Completion Criteria**: All unused variables documented in code and user docs with clear status

---

## Phase 9: Polish & Cross-Cutting Concerns

**Goal**: Final quality checks, linting, and integration verification

**Tasks**:

- [ ] T081 [P] Run just mypy to verify type checking passes
- [ ] T082 [P] Run just ruff_check_ci to verify linting passes
- [ ] T083 [P] Run just ruff_format_check to verify formatting is correct
- [ ] T084 Run just unit_tests to verify all tests pass with coverage
- [ ] T085 Run just lint_docs to verify documentation lints cleanly
- [ ] T086 Run just docs to verify documentation builds without errors
- [ ] T087 Verify backward compatibility: test from noiz.settings import PROCESSED_DATA_DIR
- [ ] T088 Verify backward compatibility: test all existing imports still function
- [ ] T089 Run just run_system_tests (if available) to verify end-to-end functionality
- [ ] T090 Final verification: Run CLI command with NOIZ_* env vars and check logs for masking

**Completion Criteria**: All quality gates pass, backward compatibility verified, no regressions

---

## Dependencies Between User Stories

```
Setup (Phase 1)
    ↓
Foundational (Phase 2: Core Config Module)
    ↓
    ├─→ US1: CLI Configuration (P1) ─┐
    ├─→ US2: Flask Integration (P1) ──┤
    └─→ US4: CI Pipeline (P1) ────────┴─→ US3: Code Docs (P2) ─→ US5: User Docs (P2) ─→ US6: Cleanup (P3)
```

**Critical Path**: Setup → Foundational → US1 & US2 & US4 (parallel) → US3 → US5 → US6 → Polish

**Parallel Execution**:
- Phase 2 Foundational: Tasks T004-T006, T009-T012, T015-T025 can run in parallel
- Phase 3 US1: Most tasks sequential due to dependencies
- Phase 4 US2: Tasks T040-T044 (integration tests) can run in parallel
- Phase 5 US4: Tasks T047-T052 (CI files) can run in parallel
- Phase 6 US3: Tasks T057-T060 (docstrings) can run in parallel
- Phase 7 US5: Tasks T064-T071 (doc sections) can run in parallel
- Phase 8 US6: Tasks T076-T079 (deprecation docs) can run in parallel
- Phase 9 Polish: Tasks T081-T083 (quality checks) can run in parallel

---

## MVP Definition

**Minimum Viable Product**: User Stories 1 & 2 (Phases 3-4)

**Rationale**: These deliver the core value:
- US1: CLI users can configure application (primary usage pattern)
- US2: Notebook users can use Flask app factory (critical workflow)

**MVP Deliverables**:
- Core config module (Phase 2)
- CLI configuration working (Phase 3)
- Flask integration working (Phase 4)
- Basic tests passing

**Post-MVP**: Can incrementally add US4 (CI), US3 (code docs), US5 (user docs), US6 (cleanup) as time permits

---

## Parallel Execution Examples

### Example 1: Phase 2 Foundational (Maximum Parallelism)

Run concurrently:
- Terminal 1: Tasks T004-T006 (Create config module structure)
- Terminal 2: Tasks T009-T012 (Implement validation logic)
- Terminal 3: Tasks T015-T025 (Write all unit tests)

Then sequentially:
- T007 (Implement NoizConfig - depends on T005-T006)
- T008 (Add get_config - depends on T007)
- T013-T014 (Add methods - depends on T007)
- T026 (Run tests - depends on all above)

### Example 2: Phase 5 US4 CI Updates (Maximum Parallelism)

Run concurrently:
- Terminal 1: T047-T049 (.gitlab-ci.yml updates)
- Terminal 2: T050 (linting.yml update)
- Terminal 3: T051 (documentation.yml update)
- Terminal 4: T052 (other templates review)

Then sequentially:
- T053-T056 (Commit, push, verify CI)

### Example 3: Phase 7 US5 Documentation (Maximum Parallelism)

Run concurrently:
- Terminal 1: T064-T065 (Overview, Required Variables)
- Terminal 2: T066-T067 (Optional Variables, Database Config)
- Terminal 3: T068-T069 (.env guide, Flask integration)
- Terminal 4: T070-T071 (CI/CD, Migration guide)

Then sequentially:
- T072-T075 (Add to toctree, build, lint, review)

---

## Task Execution Notes

### For Each Task:
1. Read task description and file path
2. If [P], can run concurrently with other [P] tasks
3. If [Story], maps to user story in spec.md
4. Complete task following research.md and contracts
5. Run relevant quality checks (mypy, ruff, tests)
6. Commit if task represents logical unit

### Testing Strategy:
- Unit tests after each module (T026, T037, T045, T062)
- Integration tests after Flask changes (T045)
- CLI manual testing (T038-T039, T046)
- CI verification (T054-T056)
- Full suite at end (T084, T089)

### Quality Gate Checkpoints:
- After Phase 2: Config module complete, all unit tests passing
- After Phase 4: US1 & US2 complete, MVP functional
- After Phase 5: CI passing with new config
- After Phase 9: All gates pass, ready for merge

---

## Success Metrics

Track completion against spec.md success criteria:

- **SC-001**: 100% CLI commands work → Verify in T037-T039
- **SC-002**: Flask app factory works → Verify in T045-T046
- **SC-003**: Clear error messages <1 min → Verify in T039, check validation output
- **SC-004**: Understand config <10 min → Verify in T075, time documentation review
- **SC-005**: <100ms startup overhead → Measure in T090, add timing log
- **SC-006**: Zero test failures → Verify in T084
- **SC-007**: Unused vars documented → Complete in T080
- **SC-008**: CI 100% pass → Verify in T055
- **SC-009**: Configure <15 min → Verify in T075, follow docs from scratch
- **SC-010**: Copy-paste examples work → Verify in T075, test .env commands

---

## Estimated Effort

**Total Tasks**: 90
**Parallel Tasks**: 18 (20% can run concurrently)
**Sequential Tasks**: 72

**Time Estimates** (for experienced Python developer):
- Phase 1 (Setup): 15 min
- Phase 2 (Foundational): 4-6 hours (core module + tests)
- Phase 3 (US1): 2-3 hours (CLI integration)
- Phase 4 (US2): 1-2 hours (Flask integration tests)
- Phase 5 (US4): 1-2 hours (CI updates + verification)
- Phase 6 (US3): 1 hour (docstrings)
- Phase 7 (US5): 2-3 hours (user documentation)
- Phase 8 (US6): 30 min (deprecation docs)
- Phase 9 (Polish): 1-2 hours (quality gates)

**Total Estimated Time**: 14-20 hours (2-3 days)
**MVP Time (US1+US2)**: 8-12 hours (1-1.5 days)

---

## Next Steps

1. Review this task breakdown with stakeholders
2. Confirm MVP scope (recommend US1+US2 only)
3. Assign tasks (can parallelize Phase 2 across developers)
4. Begin with Phase 1 (Setup)
5. Execute incrementally, committing after each logical unit
6. Track progress using task checkboxes
7. Verify success metrics as you complete each phase

**Ready to begin**: All planning artifacts complete, tasks are actionable and dependency-ordered.

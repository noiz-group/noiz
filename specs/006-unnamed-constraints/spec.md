# Feature Specification: Remove Named Database Constraints

**Feature Branch**: `006-unnamed-constraints`
**Created**: 2025-10-24
**Status**: Draft
**Input**: User description: "In previous work we introduced compatibility of noiz with SQLite. In order to do that, we introduced some translation layer for Named Constraints that are available in postgres but not in sqlite. Non-named constraints are also available in postgres. I would like to convert all constrainst to be compatible with each other and not named. This should allow us to get rid of that compatibility layer for upsert commands."

## Clarifications

### Session 2025-10-24

- Q: What is the migration rollback strategy? → A: Fresh baseline migration (no rollback)
- Q: How should constraint violation error messages identify conflicting columns when constraints are unnamed? → A: Application layer catches constraint violations and enhances error messages with column information
- Q: What testing strategy should be used to validate the migration? → A: Test migration with sample production-like data for both backends, validate all constraints enforce correctly
- Q: Are there external tools depending on named constraints? → A: No external tool dependencies

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Simplified Database Operations (Priority: P1)

Developers perform database operations (inserts, updates, upserts) without needing dialect-specific constraint handling logic.
The system automatically handles constraint conflicts using column definitions rather than named constraints.

**Why this priority**: This is the core value of the feature.
It eliminates technical debt and reduces code complexity, making the codebase more maintainable.
It also removes a potential source of bugs when adding new database operations.

**Independent Test**: Can be fully tested by running existing database operations against both PostgreSQL and SQLite backends and verifying all upsert operations succeed without the translation layer.

**Acceptance Scenarios**:

1. **Given** the application is configured with PostgreSQL, **When** an upsert operation is performed with conflicting unique constraint values, **Then** the existing record is updated using column-based conflict resolution without requiring named constraints
2. **Given** the application is configured with SQLite, **When** an upsert operation is performed with conflicting unique constraint values, **Then** the existing record is updated using the same column-based conflict resolution as PostgreSQL
3. **Given** a developer adds a new model with unique constraints, **When** they implement upsert operations, **Then** they do not need to add entries to the CONSTRAINT_TO_COLUMNS mapping dictionary

---

### User Story 2 - Migration Compatibility (Priority: P2)

Existing databases with named constraints continue to function correctly after the migration removes constraint names.
Database migrations apply cleanly to both new and existing database instances.

**Why this priority**: This ensures the transition is smooth for existing deployments and prevents data loss or migration failures.

**Independent Test**: Can be tested by applying fresh baseline migration to empty databases and then loading sample production-like data to verify all constraints enforce uniqueness correctly in both PostgreSQL and SQLite.

**Acceptance Scenarios**:

1. **Given** a fresh PostgreSQL database, **When** the baseline migration is applied, **Then** all constraints are created as unnamed constraints
2. **Given** a fresh SQLite database, **When** the baseline migration is applied, **Then** all constraints are created identically to PostgreSQL
3. **Given** a database with the new schema and sample production-like data loaded, **When** attempting to insert duplicate records, **Then** all unique constraints are enforced correctly and produce clear error messages

---

### User Story 3 - Code Cleanup (Priority: P3)

The codebase has reduced complexity with removal of dialect-specific constraint translation logic.
Developers can understand and maintain database operations more easily.

**Why this priority**: This is a beneficial outcome but not critical for core functionality.
It improves developer experience and reduces maintenance burden.

**Independent Test**: Can be verified by code review showing the removal of CONSTRAINT_TO_COLUMNS mapping and dialect_agnostic_on_conflict function.

**Acceptance Scenarios**:

1. **Given** all models use unnamed constraints, **When** a developer reviews the database.py module, **Then** they do not see the CONSTRAINT_TO_COLUMNS mapping dictionary
2. **Given** all upsert operations use column-based conflict resolution, **When** a developer reviews upsert methods, **Then** they see consistent patterns using index_elements across all operations
3. **Given** the translation layer is removed, **When** running the test suite, **Then** all tests pass for both PostgreSQL and SQLite backends

---

### Edge Cases

- How does the system handle partial migration failures on databases with many constraints?
- What happens when multiple constraints could match the same violation pattern?
- How are constraint violations in direct SQL queries (outside application code) reported?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST convert all named unique constraints in model definitions to unnamed unique constraints
- **FR-002**: System MUST update all upsert operations to use index_elements (column lists) instead of constraint names
- **FR-003**: System MUST remove the CONSTRAINT_TO_COLUMNS mapping dictionary from database.py
- **FR-004**: System MUST remove the dialect_agnostic_on_conflict function from database.py
- **FR-005**: System MUST create a fresh baseline migration that defines all constraints as unnamed (no incremental migration from named to unnamed)
- **FR-006**: Migration applies to both PostgreSQL and SQLite, defining constraints uniformly without dialect-specific handling
- **FR-007**: System MUST maintain identical constraint enforcement behavior before and after the migration
- **FR-008**: All upsert operations MUST continue to work correctly with both PostgreSQL and SQLite after changes
- **FR-009**: System MUST update all model files where UniqueConstraint is defined with a name parameter
- **FR-010**: System MUST update all API files where _prepare_upsert_command functions exist
- **FR-011**: Application MUST catch database constraint violations and enhance error messages to include conflicting column names for both PostgreSQL and SQLite
- **FR-012**: System MUST provide test fixtures or sample production-like data for validating constraint enforcement in both database backends

### Key Entities *(include if feature involves data)*

- **UniqueConstraint**: Database constraints that enforce uniqueness on one or more columns.
  - In the current implementation, constraints have names (e.g., "unique_qcone_results_per_config_per_datachunk")
  - In the target implementation, constraints will be defined only by their column lists

- **Upsert Command**: Database insert operations with conflict resolution that update existing records when unique constraints are violated.
  - Currently use constraint_name parameter for PostgreSQL
  - Currently use index_elements parameter for SQLite (via translation layer)
  - Will use index_elements parameter for both databases after changes

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All 79 named constraints identified in the codebase are converted to unnamed constraints
- **SC-002**: The CONSTRAINT_TO_COLUMNS mapping dictionary containing 19 constraint mappings is removed from database.py
- **SC-003**: All upsert operations continue to succeed with 100% success rate for both PostgreSQL and SQLite backends
- **SC-004**: Fresh baseline migration applies successfully to both PostgreSQL and SQLite with sample production-like data validating all constraint enforcement
- **SC-005**: Code complexity is reduced by removing approximately 100 lines of constraint translation logic
- **SC-006**: All existing unit and system tests pass for both database backends
- **SC-007**: Constraint violations produce clear error messages that identify the conflicting columns through application-layer error enhancement

## Assumptions

- PostgreSQL supports unnamed unique constraints (standard SQL feature)
- SQLite already uses column-based constraint resolution, so named constraints are effectively ignored
- All existing tests that verify constraint enforcement are sufficient to validate the migration
- The on_conflict_do_update method in SQLAlchemy's PostgreSQL dialect supports index_elements parameter
- No external tools, monitoring systems, or custom migrations depend on constraint names
- Database migrations can be run during a maintenance window if needed
- Fresh baseline migration approach means existing databases will need to be rebuilt or data migrated to new schema

## Dependencies

- SQLAlchemy version must support index_elements parameter for PostgreSQL dialect's on_conflict_do_update
- Flask-Migrate (Alembic) must be used to generate database migrations
- All existing database operations must be tested against both backends before deployment

## Out of Scope

- Changing the behavior of constraint enforcement (only changing how constraints are referenced)
- Adding new constraints or modifying which columns are included in existing constraints
- Performance optimization of constraint checking
- Adding database-specific constraint features beyond basic uniqueness
- Modifying foreign key constraints (only unique constraints are in scope)

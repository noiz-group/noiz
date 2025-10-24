# Feature Specification: ULID Primary Keys Migration

**Feature Branch**: `005-ulid-primary-keys`
**Created**: 2025-10-21
**Status**: Draft
**Input**: User description: "Migrate all model primary keys from BigInteger to ULID for SQLite compatibility and parallel processing safety"

## Clarifications

### Session 2025-10-21

- Q: Should the migration run as a single atomic transaction across all tables, or use separate transactions per table/batch? → A: Per-table transactions with dependency ordering - each table migrated independently in correct sequence
- Q: How should existing production databases be migrated? → A: Create new baseline migration after changing all IDs to ULIDs - no migration of existing data needed

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Developer Runs System Tests with SQLite (Priority: P1)

A developer clones the repository and runs system tests without requiring PostgreSQL installation.
The tests create database records using ULID primary keys, which work correctly in both SQLite and PostgreSQL environments.

**Why this priority**: This is the core blocker preventing SQLite from being used as the default test database.
Without this, developers cannot run tests locally without PostgreSQL.

**Independent Test**: Can be fully tested by running the system test suite with SQLite as the database backend.
Success is demonstrated when all tests that insert data complete without IntegrityError exceptions related to autoincrement on BIGINT columns.

**Acceptance Scenarios**:

1. **Given** a fresh repository clone with no PostgreSQL installed, **When** developer runs `just run_system_tests_sqlite`, **Then** all system tests pass without database constraint errors
2. **Given** a test that inserts ComponentFile records, **When** the test creates multiple records in parallel, **Then** each record receives a unique ULID primary key without conflicts
3. **Given** an existing SQLite database with data, **When** migrations are applied to convert primary keys to ULIDs, **Then** all existing data is preserved with newly generated ULIDs

---

### User Story 2 - Processing Pipeline Creates Records with Stable Identifiers (Priority: P1)

During parallel processing workflows (datachunk preparation, cross-correlation, beamforming), workers create database records with ULIDs generated before insert.
This allows workers to reference objects by stable identifiers before database commits, preventing the parallel processing data loss bug.

**Why this priority**: This is equally critical as it fixes the parallel processing bug where foreign key relationships failed because objects were created without stable IDs before commit.
The ULID infrastructure already exists for this purpose but is not yet used as the primary key.

**Independent Test**: Can be tested by running parallel processing workflows (e.g., `noiz processing prepare_datachunks --parallel`) and verifying that all foreign key relationships are correctly established and no data loss occurs when workers commit concurrently.

**Acceptance Scenarios**:

1. **Given** a datachunk processing job with 10 parallel workers, **When** workers create DatachunkFile and Datachunk records, **Then** all file_ulid foreign key references are correctly established before commit
2. **Given** a beamforming workflow, **When** BeamformingFile is created with a ULID, **Then** multiple BeamformingResult records can reference the file by its ULID before the file record is committed
3. **Given** concurrent database operations, **When** multiple workers insert records simultaneously, **Then** no primary key conflicts occur and all relationships are preserved

---

### User Story 3 - Fresh Database Schema with ULID Primary Keys (Priority: P2)

A new database is created from scratch using the updated schema with ULID primary keys.
All tables are created with ULID identifiers from the start, establishing a new baseline migration.

**Why this priority**: This is lower priority because it affects new database creation rather than the core SQLite compatibility goal.
Existing production databases will need manual data migration handled separately.

**Independent Test**: Can be tested by creating a fresh database, running migrations, and verifying all tables have ULID primary keys with proper constraints.

**Acceptance Scenarios**:

1. **Given** an empty database, **When** migrations are applied from scratch, **Then** all 13 tables are created with ULID primary keys
2. **Given** a fresh database schema, **When** application code inserts records, **Then** all foreign key relationships work correctly with ULID references
3. **Given** the new baseline migration, **When** inspecting table schemas, **Then** no integer-based primary key columns exist

---

### Edge Cases

- What happens when existing code references `.id` attributes after changing to `.ulid`?
  All code must be updated to use `.ulid` before deploying the new schema.
- What happens if a ULID generation collision occurs (extremely unlikely)?
  The ULID library handles this with sufficient randomness (80 bits), making collisions astronomically improbable.
- How are API endpoints or external scripts that reference integer IDs affected?
  No external systems consume integer identifiers, so changes can proceed without API compatibility concerns.
- How does identifier lookup performance compare between ULID and integer-based keys?
  ULID-based primary keys have ~5-10% slower comparison but ULID's fixed-width and lexicographic sorting mitigate this impact.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST change all database table primary keys from integer-based identifiers to ULID identifiers for all 13 data models
- **FR-002**: System MUST update all foreign key relationships to reference ULID identifiers instead of integer identifiers (26 foreign key relationships total)
- **FR-003**: System MUST remove redundant foreign key references where both integer and ULID references exist
- **FR-004**: System MUST create new baseline migrations with ULID primary keys for fresh database creation
- **FR-005**: System MUST ensure all data models support ULID identifier generation
- **FR-006**: System MUST update application code to generate unique identifiers before record creation
- **FR-007**: System MUST create reversible database migrations for both upgrade and rollback scenarios, using per-table transactions with dependency ordering to allow incremental progress
- **FR-008**: System MUST ensure new database schemas create all relationships correctly with ULID foreign keys
- **FR-009**: System MUST maintain uniqueness constraints on all identifier fields
- **FR-010**: System MUST work correctly on both SQLite and PostgreSQL database systems
- **FR-011**: System MUST update data model definitions to use ULID as primary identifier
- **FR-012**: System MUST remove old integer identifier columns after ULID migration is complete
- **FR-013**: System MUST update all code that references identifiers to use ULID-based lookups

### Key Entities

- **Data Models with Existing ULID Support (7 modules, ~15 classes)**: Already have ULID fields, need primary identifier changed, redundant integer foreign keys removed
  - Beamforming data: File and result records
  - Cross-correlation data: Cartesian and cylindrical files and results
  - Data chunks: Raw and processed data segments and files
  - Power Spectral Density: Files and results
  - Stacking: Cross-correlation function stacks

- **Data Models without ULID Support (6 modules, ~10 classes)**: Need ULID capability added, identifier generation updated, migration to add and populate ULID fields
  - Component data: Seismic component files
  - Event detection: Event detection models
  - Quality control: QC stage one and two results
  - State of health: GPS and instrument health data
  - Time series: Time series index
  - Time span: Processing time windows

- **Foreign Key Relationships**: 26 foreign key relationships need conversion to ULID-based references

- **Association Tables**: Join tables with foreign key references need updates for many-to-many relationships

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All system tests pass on SQLite without database constraint errors related to identifier generation
- **SC-002**: All system tests pass on PostgreSQL with identical behavior to SQLite
- **SC-003**: Fresh database creation completes successfully with all tables using ULID primary keys
- **SC-004**: All 26 foreign key relationships remain enforced after migration
- **SC-005**: Zero instances of integer-based identifier access remain in application code
- **SC-006**: All 13 data models use ULID as primary identifier
- **SC-007**: Parallel processing workflows complete without relationship constraint violations
- **SC-008**: Baseline migration can be rolled back to recreate empty database schema
- **SC-009**: Identifier lookup performance degrades by no more than 10% compared to previous implementation
- **SC-010**: All ULID identifier fields have uniqueness constraints enforced at database level
- **SC-011**: Application code generates identifiers before record creation for all data models

## Assumptions

- Existing ULID identifier infrastructure is correct and provides proper unique identifier generation
- ULID storage format is sufficient for both SQLite and PostgreSQL database systems
- Existing processing code that uses ULIDs follows correct pattern for identifier generation
- No external systems consume integer identifiers via APIs (confirmed - no compatibility layer needed)
- New baseline migration will be used for fresh database creation; existing production data migration handled separately
- Up to 10% performance degradation from identifier changes is acceptable given benefits
- ULID collision probability is negligible
- All code paths that create records can be updated to generate identifiers before record creation

## Dependencies

- ULID identifier generation library (already installed and in use)
- Database systems support variable-length string identifier storage
- Database migration framework available for schema changes
- Both PostgreSQL and SQLite support string-based unique constraints

## Out of Scope

- Migrating existing production database data from integer IDs to ULIDs (handled separately as manual process)
- Migrating to UUIDs instead of ULIDs (ULID provides lexicographic sorting and timestamp encoding)
- Changing ULID format or length (fixed at 26 characters per ULID spec)
- Performance optimization beyond ensuring no more than 10% degradation
- Updating external systems or APIs that may reference integer IDs
- Creating public-facing ULID APIs or documentation for external consumers

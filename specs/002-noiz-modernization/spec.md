# Feature Specification: Noiz Modernization for Reliable Data Processing

**Feature Branch**: `002-noiz-modernization`
**Created**: 2025-10-16
**Status**: Draft
**Input**: Modernize Noiz for reliable installation, SQLite-first architecture, and portable configuration system

## User Scenarios & Testing

### User Story 1 - First-Time Installation and Setup (Priority: P1)

A seismologist at a university or oil company wants to try Noiz on their laptop for exploratory seismic noise analysis. They have Python installed and basic command-line experience but no database administration skills.

**Why this priority**: This is the entry point for 100% of new users. If installation fails, adoption is blocked entirely. Currently represents the highest friction point in the user journey.

**Independent Test**: Can be fully tested by installing Noiz on a fresh Python 3.10+ environment, running init, and verifying that a working SQLite database is created with no manual configuration.

**Acceptance Scenarios**:

1. **Given** fresh Ubuntu/MacOS machine with Python 3.10+, **When** user runs `pip install noiz`, **Then** installation completes in under 2 minutes without errors
2. **Given** successful installation, **When** user runs `noiz init my-project`, **Then** SQLite database is created, default configuration is generated, and user receives confirmation message with next steps
3. **Given** initialized project, **When** user runs `noiz --help`, **Then** all available commands are listed with clear descriptions
4. **Given** initialized project, **When** user adds test seismic data, **Then** data is ingested successfully into SQLite database

---

### User Story 2 - Reliable Parallel Processing (Priority: P1)

A researcher needs to process 3 months of continuous seismic data from 10 stations on their workstation. They want the processing to run overnight without data loss or crashes.

**Why this priority**: This is the core value proposition of Noiz. The current parallel processing bug causes data loss, making Noiz unreliable for production use. This blocks all serious scientific work.

**Independent Test**: Process a known dataset with parallel execution enabled, verify that 100% of expected output files are created, and validate data integrity by comparing with sequential processing results.

**Acceptance Scenarios**:

1. **Given** 3 months of data from 10 stations, **When** user runs `noiz processing prepare_datachunks --parallel`, **Then** all datachunks are created without data loss, and database foreign key constraints are satisfied
2. **Given** prepared datachunks, **When** user runs `noiz processing run_crosscorrelations_cartesian --parallel`, **Then** all cross-correlations complete successfully with no missing results
3. **Given** processing running for 8 hours, **When** a single worker process encounters an error, **Then** other workers continue, failed task is logged, and user can retry failed tasks without reprocessing successful ones
4. **Given** completed processing, **When** user queries database for results, **Then** all expected records exist with valid foreign key relationships

---

### User Story 3 - Portable Configuration Sharing (Priority: P2)

A research group has developed an optimal processing pipeline for ambient noise tomography. They want to share this exact configuration with collaborators at another institution and publish it with their paper for reproducibility.

**Why this priority**: Scientific reproducibility is critical for research credibility. Without portable configurations, results cannot be independently verified, and best practices cannot be shared effectively.

**Independent Test**: Export a complete processing pipeline (with all parameter dependencies) to a single TOML file, send to another user, import on their system, and verify identical processing results.

**Acceptance Scenarios**:

1. **Given** completed processing pipeline with 5 configuration stages, **When** user runs `noiz configs export-pipeline --pipeline-id ambient_noise_2023 --output pipeline.toml`, **Then** single TOML file is generated containing all configurations with dependency relationships
2. **Given** exported pipeline file, **When** collaborator runs `noiz configs import-pipeline --file pipeline.toml`, **Then** all configurations are imported with preserved IDs, dependencies are validated, and conflicts are detected
3. **Given** imported pipeline, **When** collaborator processes same dataset, **Then** results match original processing within numerical precision
4. **Given** pipeline with complex dependencies (multiple parents, branching paths), **When** user runs `noiz configs visualize --pipeline-id ambient_noise_2023 --output graph.png`, **Then** dependency graph is generated showing all relationships clearly

---

### User Story 4 - HPC Cluster Processing (Priority: P3)

An advanced user needs to process 2 years of data from 100 stations on their institution's HPC cluster using SLURM job scheduling.

**Why this priority**: This enables large-scale processing but is not required for basic Noiz usage. Most users will process smaller datasets on local workstations first.

**Independent Test**: Submit processing job to SLURM cluster, verify that multiple compute nodes can process data concurrently without database conflicts, and validate that all results are written correctly.

**Acceptance Scenarios**:

1. **Given** HPC cluster with SLURM scheduler, **When** user submits `noiz processing` job array with 50 tasks, **Then** all tasks complete successfully without database deadlocks
2. **Given** processing across multiple nodes, **When** tasks write results concurrently, **Then** database handles concurrent writes without corruption or lost data
3. **Given** long-running cluster job, **When** user checks progress, **Then** completed/pending task counts are accurate and user can monitor progress without disrupting execution
4. **Given** failed node mid-processing, **When** job is resubmitted, **Then** completed work is detected, only remaining tasks are executed, and no duplicate processing occurs

---

### User Story 5 - Configuration Updates and Python Compatibility (Priority: P3)

A developer wants to upgrade Noiz dependencies to support Python 3.11-3.13 and update to modern package versions for security and performance improvements.

**Why this priority**: Important for long-term maintainability and security, but doesn't block current users from doing science with Python 3.10.

**Independent Test**: Install Noiz on Python 3.11, 3.12, and 3.13 environments, run full test suite, and verify all tests pass on each version.

**Acceptance Scenarios**:

1. **Given** Python 3.11 environment, **When** user installs Noiz, **Then** all dependencies install successfully without version conflicts
2. **Given** Python 3.13 environment, **When** user runs processing pipeline, **Then** results match Python 3.10 baseline within numerical precision
3. **Given** updated Flask 3.0+, **When** user runs Flask API, **Then** all routes work without deprecation warnings
4. **Given** SQLAlchemy 2.0+, **When** database queries execute, **Then** performance is equal or better than SQLAlchemy 1.4

---

### Edge Cases

- **Disk space exhaustion mid-processing**: System MUST only write database records for objects that successfully saved their processing results to disk. If disk space exhaustion occurs, processing stops immediately. User can resume processing after freeing disk space - system detects already-completed work and continues from last successful checkpoint.

- **mseedindex version incompatibility**: System MUST use mseedindex from PyPI (installable via pip) rather than Docker-compiled version. Tsindex data MUST come from JSON output (not direct database writes) to avoid PostgreSQL-specific type dependencies (HSTORE, ARRAY, NUMRANGE) that block SQLite support.

- **Graceful and forceful stop/resume**: User MUST be able to stop processing at any point (Ctrl+C or kill signal) and resume later. System detects completed work in database and skips already-processed items. No duplicate processing occurs on resume.

- **Database connection loss**: Processing MUST stop immediately with clear error message if database connection is lost. No silent failures or data loss queues. User must fix database connectivity and restart - system will resume from last successful database commit.

- **Concurrent project initialization**: System MUST prevent concurrent `noiz init` in same directory. Lock file or directory existence check ensures one project per directory. Second initialization attempt fails with clear error message.

- **Malformed configuration import**: System MUST validate TOML configuration files during import before any database writes. Validation detects circular dependencies, missing required fields, type mismatches, and invalid parameter values. Import fails atomically - either all configurations imported successfully or none at all.

- **Large dataset memory limits**: Not a primary concern - typical seismic files are small enough that memory exhaustion is rare. Batch size parameter allows users to reduce memory footprint if needed. System does not implement complex memory management or out-of-core processing.

- **Missing seismic data files**: System MUST fail processing tasks that reference missing or moved data files with clear error indicating which file is missing. Failed tasks are logged and user can retry after restoring files. Other tasks continue processing.

- **Python version mismatch**: Installation MUST fail on Python 3.9 or below with clear error message indicating minimum version requirement. Python 3.14+ may work but is not officially supported - no effort made to maintain backward compatibility if newer Python features improve code quality.

- **PostgreSQL migration**: NOT SUPPORTED. Database backend is chosen at project initialization and cannot be changed. Projects are not intended for continuous monitoring - they are ephemeral processing workspaces. Users needing different backend must create new project and reprocess data.

## Requirements

### Functional Requirements

- **FR-001**: System MUST install successfully on Python 3.10, 3.11, 3.12, and 3.13 via `pip install noiz`
- **FR-002**: Installation MUST complete in under 2 minutes on typical broadband connection
- **FR-003**: System MUST default to SQLite database for new projects (no PostgreSQL setup required)
- **FR-004**: `noiz init` command MUST create SQLite database, default configuration directory, and data directories without user intervention
- **FR-004a**: System MUST use mseedindex from PyPI (not Docker-compiled version)
- **FR-004b**: System MUST decouple from mseedindex's direct database writes using JSON output mode
- **FR-005**: Parallel processing MUST preserve all data without loss, maintaining foreign key integrity in all scenarios
- **FR-006**: System MUST use UUID primary keys for all entities involved in parallel processing
- **FR-007**: SQLite backend MUST support all core processing operations (datachunks, cross-correlations, stacking, beamforming, PPSD)
- **FR-008**: System MUST allow optional PostgreSQL backend for users with existing deployments or specific requirements
- **FR-009**: Processing configurations MUST have human-readable IDs (e.g., "dc_2023_highfreq_v1") independent of database auto-increment
- **FR-010**: Configuration files MUST support explicit parent-child dependency relationships
- **FR-011**: Users MUST be able to export complete processing pipelines to single TOML file
- **FR-012**: Users MUST be able to import pipeline TOML files with validation and conflict detection
- **FR-013**: System MUST visualize configuration dependency graphs
- **FR-014**: All configuration metadata MUST include author, version, creation date, and description
- **FR-015**: Processing MUST support configurable execution backends (sequential, multiprocessing, Dask)
- **FR-016**: System MUST detect completed work and resume processing from last successful checkpoint
- **FR-017**: Processing commands MUST provide clear progress indicators
- **FR-018**: Failed processing tasks MUST be resumable without reprocessing successful tasks
- **FR-019**: System MUST validate configuration compatibility before starting processing
- **FR-020**: All dependencies MUST be declared in pyproject.toml (no external tool installation required)
- **FR-021**: mseedindex MUST use JSON output mode to decouple from database schema constraints

### Key Entities

- **Project**: Represents a Noiz analysis project with database, configuration, and output directories
- **Configuration**: Processing parameters with human-readable ID, version, author, and creation metadata
- **Configuration Dependency**: Parent-child or multi-parent relationship between configurations
- **Pipeline**: Complete processing workflow with ordered configuration stages
- **Processing Task**: Unit of work that can be executed independently in parallel
- **Execution Backend**: Strategy for running tasks (sequential, multiprocessing, or Dask)

## Success Criteria

### Measurable Outcomes

- **SC-001**: New user can install Noiz, initialize project, ingest data, and start processing in under 15 minutes (currently: 1-2 hours with PostgreSQL + Docker setup)
- **SC-002**: Parallel processing completes with 0% data loss on datasets of any size (currently: unpredictable failures)
- **SC-003**: Processing 3 months of 10-station data completes in approximately 1 hour on modern workstation (8 cores, 16GB RAM)
- **SC-004**: SQLite backend handles typical research datasets (1-2 years, 10-50 stations) without performance degradation
- **SC-005**: Configuration export/import workflow takes under 30 seconds for typical pipeline (5-10 configuration stages)
- **SC-006**: Installation success rate reaches 95% on first attempt (measured by CI test matrix across Python versions and OS)
- **SC-007**: Parallel processing speedup is linear up to 8 cores (8x faster than sequential)
- **SC-008**: Interrupted processing can be resumed without data loss or duplicate work (100% resume success rate)
- **SC-009**: 90% of users successfully complete first processing run without consulting documentation
- **SC-010**: Configuration conflicts are detected before processing starts (100% detection rate in validation tests)

### Non-Functional Requirements

- **NFR-001**: Installation package size under 100MB
- **NFR-002**: SQLite database file size under 10GB for typical 1-year dataset
- **NFR-003**: Memory usage under 8GB for parallel processing with 8 workers
- **NFR-004**: Documentation covers all configuration portability features
- **NFR-005**: All processing functions have type hints and pass mypy strict checking
- **NFR-006**: Test coverage above 80% for critical paths (parallel processing, database operations)
- **NFR-007**: CI pipeline runs on Python 3.10, 3.11, 3.12, 3.13 with all tests passing

## Assumptions

- Users have basic command-line proficiency and Python installed
- Typical research datasets are 1-2 years of continuous data from 10-50 stations
- Most users have workstations with 4-8 CPU cores and 16-32GB RAM
- SQLite is acceptable for research use cases (not high-concurrency multi-user scenarios)
- ObsPy will support Python 3.11+ within next 6 months (if not, maintain 3.10 compatibility)
- Existing PostgreSQL users are willing to continue using PostgreSQL or create new SQLite projects
- Configuration portability is more valuable than backward compatibility with old config format
- Projects are ephemeral processing workspaces, not long-lived continuous monitoring systems

## Dependencies

- **ObsPy**: Core seismic data processing library, version compatibility critical
- **SQLAlchemy 2.0+**: Required for modern async support and performance
- **Flask 3.0+**: Web framework for future API/UI features
- **Click**: CLI framework, no breaking changes expected
- **Dask**: Optional dependency for distributed processing
- **Python 3.10-3.13**: Target version range

## Out of Scope

- Cloud deployment automation (future enhancement)
- Real-time data processing (future enhancement)
- Web-based GUI for processing (future enhancement, configuration editor only)
- Multi-user concurrent processing on single database (use PostgreSQL instead)
- Windows-specific optimizations (works on Windows but not primary target)
- GPU acceleration (future enhancement)
- Database backend migration (SQLite ↔ PostgreSQL)
- Backward compatibility with Python 3.9 or earlier
- Advanced memory management or out-of-core processing

## References

- Existing design documents:
  - refactoring_roadmap.rst: Technical implementation details
  - config_system.rst: Configuration portability design
  - architecture.rst: Code quality analysis
  - s3_storage.rst: Cloud storage future plans

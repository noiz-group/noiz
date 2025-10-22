# Implementation Plan: Noiz Modernization for Reliable Data Processing

**Branch**: `002-noiz-modernization` | **Date**: 2025-10-16 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-noiz-modernization/spec.md`

## Summary

Modernize Noiz to eliminate data loss in parallel processing, simplify installation with SQLite-first architecture, and enable portable configuration sharing. Primary goals: fix ULID/foreign key bug causing data loss, default to SQLite (no PostgreSQL setup required), support Python 3.10-3.13, and implement transferrable TOML-based configuration system. Uses ULID (Universally Unique Lexicographically Sortable Identifier) for globally unique, human-readable identifiers. Target: 15-minute setup time (from 1-2 hours), 0% data loss (from unpredictable), and scientific reproducibility through pipeline export/import.

## Technical Context

**Language/Version**: Python 3.10-3.13 (primary target: 3.10, extend to 3.11-3.13)
**Primary Dependencies**:
- ObsPy 1.4.2 (seismic data processing - blocks SQLAlchemy 2.0 upgrade)
- SQLAlchemy 1.4 (database ORM - staying on 1.4 due to ObsPy constraint)
- Flask 2.0.2 (web framework - staying on 2.x due to ObsPy/SQLAlchemy)
- Click (CLI framework - stable, no breaking changes expected)
- Dask 2021.11.2 (parallel processing - old version, stable)
- mseedindex >=3.0.8 (NEW - miniSEED indexing from PyPI)
- python-ulid >=2.0.0 (NEW - ULID generation)
- pytest (testing framework)

**Storage**: SQLite (default), PostgreSQL (optional backend for advanced users)
**Testing**: pytest with coverage, mypy for type checking, ruff for linting
**Target Platform**: Linux/MacOS workstations (4-8 cores, 16-32GB RAM), HPC clusters with SLURM (optional)
**Project Type**: Single Python project with CLI interface
**Performance Goals**:
- Installation: < 2 minutes
- Setup to first processing: < 15 minutes total
- Processing: 3 months/10 stations in ~1 hour (8 cores)
- Parallel speedup: Linear up to 8 cores

**Constraints**:
- Must maintain scientific correctness (no precision loss)
- Zero data loss in parallel processing (currently fails)
- SQLite database < 10GB for typical datasets
- Memory usage < 8GB for 8 parallel workers
- Must support resume after interruption
- ObsPy version compatibility may limit Python 3.11+ adoption timing

**Scale/Scope**:
- Typical: 1-2 years data, 10-50 stations
- Large: 2+ years, 100+ stations on HPC
- SQLite suitable for typical, PostgreSQL for large/concurrent

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Constitution Compliance Analysis

**Principle I: Scientific Correctness First** ✅ PASS
- ULID primary keys ensure data lineage integrity in parallel processing
- All processing parameters versioned in database
- Stop/resume capability preserves checkpoints
- *Compliance*: Feature directly addresses data loss bug that violates scientific correctness

**Principle II: Database-Driven Configuration** ⚠️ **EVOLUTION REQUIRED**
- Current: Configs in database only
- Proposed: Add human-readable IDs + TOML export/import
- *Justification*: Scientific reproducibility requires portable configs for publications
- *Impact*: Enhances principle - configs still in database, adds portability layer
- *Action*: Update constitution to reflect "Database-driven with portable export"

**Principle III: CLI-First Interface** ✅ PASS
- All new features accessible via CLI (`noiz init`, `noiz configs export-pipeline`)
- Maintains existing CLI patterns (`-sd/-ed`, `--parallel`, `-v`)
- *Compliance*: Full adherence

**Principle IV: Parallel Processing by Default** ✅ PASS with FIX
- Fixes critical parallel processing data loss bug
- Maintains Dask as execution backend
- Adds multiprocessing option for local-only use
- *Compliance*: Fixes existing violation (data loss made parallel unusable)

**Principle V: Type Safety and Quality Checks** ✅ PASS
- mypy, ruff, doc8 checks maintained
- New code will include type hints
- *Compliance*: Full adherence

**Principle VI: Test Coverage for Scientific Code** ✅ PASS
- Parallel processing fix requires comprehensive test coverage
- Resume capability needs integration tests
- *Compliance*: Aligned with principle

**Principle VII: Documentation as Code** ⚠️ **CONFLICT** - Resolution Required
- Constitution requires: RST in Sphinx
- Spec-kit generates: Markdown in `.specify/`
- *Resolution*: Spec-kit docs are planning artifacts, not end-user documentation
- *Action*: Final user-facing docs must be converted to RST and added to Sphinx

### Constitution Violations Requiring Justification

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| SQLite default (constitution specifies PostgreSQL) | 95% of users blocked by PostgreSQL setup complexity | PostgreSQL-only approach: Requires database admin skills, violates "simple install" user story |
| Python 3.11-3.13 support (constitution locks to 3.10) | Security updates, modern features, broader adoption | Stay on 3.10 only: Blocks users on newer Python, misses performance improvements |
| Dask optional (constitution implies required) | Lighter default install, local multiprocessing sufficient for most users | Dask-required approach: Unnecessary complexity for laptop users, heavier install |

**GATE DECISION**: ✅ PASS with constitution amendments required

## Project Structure

### Documentation (this feature)

```
specs/002-noiz-modernization/
├── plan.md              # This file
├── research.md          # Phase 0: Technology decisions
├── data-model.md        # Phase 1: Entity relationships
├── quickstart.md        # Phase 1: Testing scenarios
├── contracts/           # Phase 1: Not applicable (no API contracts for this feature)
└── tasks.md             # Phase 2: Generated by /speckit.tasks
```

### Source Code (repository root)

Noiz uses **Option 1: Single project** structure (already established):

```
src/noiz/
├── models/              # MODIFIED: Add ULID fields, simplify Tsindex
│   ├── __init__.py      # MODIFIED: Export ULIDMixin
│   ├── mixins.py        # MODIFIED: Add ULID mixin
│   ├── component.py
│   ├── datachunk.py     # MODIFIED: Add ulid, file_ulid FK
│   ├── crosscorrelation.py  # MODIFIED: Add ulid, file_ulid FK
│   ├── timeseries.py    # MODIFIED: Remove HSTORE/ARRAY, use JSON
│   ├── processing_params.py  # MODIFIED: Add config_id, parent_config_id
│   └── ...
├── api/                 # MODIFIED: Update bulk insert logic for ULIDs
│   ├── helpers.py       # MODIFIED: ULID-based upsert, resume detection
│   ├── datachunk.py
│   ├── crosscorrelations.py
│   └── ...
├── processing/          # MODIFIED: Workers generate ULIDs upfront
│   ├── datachunk.py
│   ├── crosscorrelations.py
│   └── ...
├── config_system/       # NEW: Configuration portability
│   ├── __init__.py
│   ├── parser.py        # TOML parsing and validation
│   ├── exporter.py      # Pipeline export
│   ├── importer.py      # Pipeline import with validation
│   └── visualizer.py    # Dependency graph generation
├── database.py          # MODIFIED: SQLite/PostgreSQL backend selection
├── settings.py          # MODIFIED: Add DATABASE_BACKEND env var
├── cli.py               # MODIFIED: Add `noiz init`, `noiz configs` commands
└── ...

migrations/              # NEW: ULID migration, config ID migration
├── versions/
│   ├── XXXX_add_ulid_fields.py
│   ├── XXXX_add_config_ids.py
│   └── ...

tests/
├── unit/
│   ├── test_ulid_generation.py     # NEW
│   ├── test_config_parser.py       # NEW
│   └── ...
├── integration/
│   ├── test_parallel_resume.py     # NEW
│   ├── test_sqlite_backend.py      # NEW
│   └── ...
└── system/
    ├── test_pipeline_export_import.py  # NEW
    └── ...

config_examples/         # MODIFIED: Add pipeline examples with config IDs
├── pipeline_ambient_noise.toml     # NEW
├── datachunk_basic.toml            # MODIFIED: Add [config] section
└── ...

pyproject.toml           # MODIFIED: Update dependencies, Python versions
```

**Structure Decision**: Maintain existing single-project structure. This feature extends core Noiz functionality rather than adding separate services. New `config_system/` package added for configuration portability. Database schema evolution handled via migrations directory (already established pattern).

## Complexity Tracking

*Constitution violations justified above*

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| SQLite + PostgreSQL dual backend | Different user needs (laptop vs HPC) | Single backend: Either blocks simple users (PostgreSQL) or blocks HPC users (SQLite only) |
| Optional Dask dependency | Most users don't need distributed processing | Required Dask: Heavier install, unnecessary for 90% of users |

---

## Phase 0: Outline & Research

### Research Tasks

Given the comprehensive nature of this modernization and the existing `refactoring_roadmap.rst`, most technical decisions are already documented. Phase 0 focuses on validating specific implementation choices:

1. **SQLite UUID Support**: Validate UUID storage approaches in SQLite (CHAR(36) vs BLOB)
2. **SQLAlchemy 2.0 Migration Path**: Confirm breaking changes and migration effort
3. **Resume Detection Strategy**: Research best practice for checkpoint-based resumption
4. **Config ID Collision Handling**: Best practices for namespace/conflict resolution

### Research Findings

See [research.md](./research.md) for detailed findings.

**Key Decisions Summary**:
- **ULID Format**: Use TEXT ULID (CHAR(26)) for 28% space savings vs UUID, hyphen-free, human-readable
- **SQLAlchemy 2.0**: Adopt `select()` API, migrate all `session.query()` calls
- **Resume Detection**: Query database for existing records by ULID before processing
- **Config ID Namespacing**: Support optional username prefix (`user/config_id`)

---

## Phase 1: Design & Contracts

### Data Model

See [data-model.md](./data-model.md) for complete entity relationships.

**Key Entity Changes**:
- Add `ulid` field (CHAR(26)) to all processing result entities
- Add `config_id`, `config_version`, `config_author` to all `*Params` entities
- Add `parent_config_id` field for configuration dependencies
- Add `ExecutionBackend` enum (Sequential, Multiprocessing, Dask)

### Contracts

This feature does not introduce new API endpoints. Configuration export/import and visualization are CLI operations producing files, not REST/GraphQL APIs.

**CLI Contract** (command-line interface):

```bash
# New commands
noiz init <project-name> [--database sqlite|postgresql] [--db-url <url>]
noiz configs export-pipeline --pipeline-id <id> --output <file.toml>
noiz configs import-pipeline --file <file.toml> [--validate-only]
noiz configs visualize --pipeline-id <id> --output <file.png>
noiz configs list [--stage <stage>]
noiz configs validate --file <file.toml>
noiz configs tree --config-id <id>

# Modified commands (add execution backend options)
noiz processing <command> --execution-mode [sequential|multiprocessing|dask] \
                          --workers <N> \
                          [--dask-scheduler <address>]
```

### Quickstart

See [quickstart.md](./quickstart.md) for user testing scenarios.

---

## Implementation Phases (High-Level)

### Phase 0: Critical Fixes (4-5 weeks)
- Fix parallel processing data loss (ULID migration)
- SQLite backend support
- mseedindex modernization (JSON mode, simplified Tsindex)
- Stop/resume capability

### Phase 1: Configuration Portability (2-3 weeks)
- Human-readable config IDs
- TOML export/import
- Dependency validation

### Phase 2: Python & Dependency Updates (2-3 weeks)
- SQLAlchemy 2.0 migration
- Flask 3.0 upgrade
- Python 3.11-3.13 support

### Phase 3: Polish & Testing (1-2 weeks)
- Comprehensive test coverage
- Documentation updates
- Performance validation

**Total Estimated Duration**: 9-13 weeks

---

## Risks & Mitigation

### Risk 1: SQLAlchemy 2.0 Migration Complexity
**Impact**: High - touches all database code
**Mitigation**: Incremental migration, enable deprecation warnings first, comprehensive testing

### Risk 2: ULID Performance on Large Datasets
**Impact**: Low - ULIDs are 28% smaller than UUIDs, text comparison efficient
**Mitigation**: Benchmark on large datasets, optimize indexes, monitor query performance

### Risk 3: SQLite Concurrency Limitations
**Impact**: Medium - HPC users may hit write conflicts
**Mitigation**: Document PostgreSQL for HPC, implement retry logic, batch writes

### Risk 4: ObsPy Python 3.11+ Compatibility
**Impact**: Low - may delay Python version expansion
**Mitigation**: Monitor ObsPy releases, maintain 3.10 compatibility, test incrementally

### Risk 5: Configuration ID Collisions
**Impact**: Low - users importing conflicting pipelines
**Mitigation**: Validation on import, prompt for rename, namespace support

---

## Success Metrics

- [ ] Installation completes in < 2 minutes on Python 3.10-3.13
- [ ] `noiz init` creates working SQLite project in < 30 seconds
- [ ] Parallel processing achieves 0% data loss on 1000+ tasks
- [ ] Interrupted processing resumes without duplicate work
- [ ] Configuration export/import roundtrip preserves all parameters
- [ ] All existing tests pass with SQLite backend
- [ ] SQLAlchemy 2.0 migration complete with no deprecation warnings
- [ ] CI passes on Python 3.10, 3.11, 3.12, 3.13

---

## Next Steps

1. ✅ Complete Phase 0 research (see research.md)
2. ✅ Generate data model (see data-model.md)
3. ⏭️ Run `/speckit.tasks` to generate detailed task breakdown
4. ⏭️ Begin implementation starting with UUID migration

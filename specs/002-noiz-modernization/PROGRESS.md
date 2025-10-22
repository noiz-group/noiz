# Noiz Modernization - Progress Summary

**Date**: 2025-10-16
**Status**: Specification revised, partial implementation complete

## What We've Learned

### Discovery 1: ObsPy Blocks Dependency Updates
- **Finding**: ObsPy 1.4.2 requires `sqlalchemy<2.0`
- **Impact**: Blocks SQLAlchemy 2.0, Flask 3.0, pandas 2.0, pydantic 2.0
- **Decision**: Defer US5 (Python 3.11-3.13 + dependency updates) until ObsPy updates
- **Current focus**: Python 3.10 + SQLAlchemy 1.4 + ULID migration

### Discovery 2: mseedindex Blocks SQLite
- **Finding**: tsindex table uses PostgreSQL-specific types (HSTORE, ARRAY, NUMRANGE)
- **Root cause**: Ancient Docker-compiled mseedindex writes directly to database
- **Solution**: Use modern PyPI mseedindex (3.0.8) with JSON output mode
- **Impact**: Major win - eliminates Docker compilation, simplifies installation

### Discovery 3: Testing Discipline Missing
- **Finding**: Tests not run after each change during implementation
- **Action**: Added testing discipline principle to constitution (v1.2.0)
- **Requirement**: Run tests after logical units, before commits

## Implementation Progress

### ✅ Completed Work

**Branch**: `phase2-ulid-foundation` (7 commits)
- T012: DatabaseBackend enum
- T013-T014: DATABASE_BACKEND env var and logic
- T015: SQLite PRAGMA configuration (FK enforcement, WAL mode)
- T016: ULIDMixin class
- T017: Export ULIDMixin

**Branch**: `phase4-ulid-model-migration` (14 commits)
- T037-T042: Added ULID to 6 file entities
- T043-T055: Added ULID + file_ulid FK to 8 result entities
- **Tests**: Unit tests passing (189 passed)

### 🔄 Specification Updates (Branch: `002-noiz-modernization`)

**Constitution v1.0.0 → v1.2.0**:
- v1.1.0: Added commit discipline (small, incremental commits)
- v1.2.0: Added testing discipline (test after each unit, before commits)

**Spec.md updates**:
- FR-004a/b: mseedindex from PyPI with JSON output
- FR-020/21: All deps in pyproject.toml, JSON decoupling
- Edge case: mseedindex version compatibility

**Research.md updates**:
- Question 1: ULID format (TEXT CHAR(26))
- Question 6: mseedindex JSON approach (NEW)
- Summary table updated

**Plan.md updates**:
- Technical context: Accurate dependency versions
- Project structure: Document Tsindex changes
- Phase 0: Add mseedindex modernization

## Branches

```
main
└── spec-drived-development
    └── 002-noiz-modernization (spec updates)
        ├── phase2-ulid-foundation (foundation complete)
        └── phase4-ulid-model-migration (models updated, needs migrations)
```

## What Needs to Be Done

### Immediate (Before Continuing Implementation)

1. **Regenerate tasks.md** - Include mseedindex modernization tasks
   - Add to US1: Install mseedindex from PyPI
   - Add to US1: Modify Tsindex model (remove HSTORE/ARRAY)
   - Add to US1: Update add_seismic_data to use JSON mode
   - Add to US1: Create JSON parser for tsindex

2. **Run `/speckit.analyze`** - Check spec/plan/tasks consistency

3. **Decide on approach**:
   - Continue with existing tasks.md (focused on ULID only)
   - OR regenerate tasks.md with mseedindex work included

### Implementation Remaining

**If continuing current tasks.md** (ULID-focused):
- Phase 4 Part B: Database migrations (T056-T068)
- Phase 4 Part C: Update worker functions (T069-T075)
- Phase 4 Part D: Update bulk insert logic (T076-T083)
- Phase 4 Part E: Resume detection (T084-T089)
- Phase 4 Part F: Testing (T090-T098)

**If regenerating** (ULID + mseedindex):
- Tsindex model simplification
- mseedindex JSON parsing
- add_seismic_data refactoring
- Then continue with ULID work

## Recommendations

### Option A: Two-Phase Approach (Recommended)

**Phase 1 (Current Sprint)**:
- Complete ULID migration (fix data loss bug)
- Skip SQLite for now (test on PostgreSQL)
- Merge when stable

**Phase 2 (Next Sprint)**:
- mseedindex modernization
- SQLite enablement
- Merge when tested

**Why**: ULID fix is critical and independent. mseedindex is valuable but not blocking parallel processing fix.

### Option B: Combined Approach

Complete both together before merging.

**Why NOT recommended**: Increases risk, delays critical bug fix.

## Next Steps

1. **Decide**: Two-phase or combined?
2. **If two-phase**: Continue with Phase 4 Part B (migrations)
3. **If combined**: Run `/speckit.tasks` to regenerate with mseedindex work
4. **Either way**: Commit this summary document

## Testing Status

- Unit tests: ✅ PASSING (Phase 2 + Phase 4 Part A)
- System tests: ⏳ Running in CI (PostgreSQL backend)
- SQLite tests: ⏸️ Blocked by HSTORE issue (will fix with mseedindex work)

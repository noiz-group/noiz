# Research: Noiz Modernization Technical Decisions

**Feature**: Noiz Modernization for Reliable Data Processing
**Date**: 2025-10-16
**Purpose**: Validate technical implementation choices for UUID migration, SQLite support, and configuration portability

## Research Question 1: Unique Identifier Format - ULID vs UUID

### Decision
Use **TEXT ULID (CHAR(26))** for unique identifiers across both SQLite and PostgreSQL.

### Rationale
- **CHAR(26)** stores ULID format (e.g., `"01ARZ3NDEKTSV4RRFFQ69G5FAV"`)
- 28% smaller than UUID (26 chars vs 36 chars with hyphens)
- Human-readable in database tools and logs
- No hyphens - cleaner, easier to copy/paste
- Lexicographically sortable (though not by seismic data time - see caveat below)
- 80 bits randomness sufficient for collision resistance

### IMPORTANT CAVEAT: ULID Timestamp
ULIDs encode **creation time** (when ULID is generated), NOT seismic data timestamp (Timespan.midtime). We use ULID purely for uniqueness and readability, not for time-range queries. Time-based queries still use indexed Timespan columns.

```python
# ULID encodes "now" (when processing happens)
ulid = ULID()  # Timestamp = current time

# Seismic data timestamp (what we query by)
timespan.midtime  # Actual data time
```

### Alternatives Considered
1. **UUID (CHAR(36))**
   - Pros: Better known, PostgreSQL native type available
   - Cons: 36 bytes (28% larger), hyphens
   - Rejected: ULID offers space savings with same guarantees

2. **ULID with custom timestamps** (using Timespan.midtime)
   - Pros: Could enable time-range queries on ULID
   - Cons: Collision risk (multiple workers, same timespan), conceptually confusing
   - Rejected: Separate time indexes already exist and work well

3. **Binary ULID/UUID (BLOB/BYTEA)**
   - Pros: 16 bytes (smaller)
   - Cons: Not human-readable, debugging harder, conversion overhead
   - Rejected: Scientific users need readable database inspection

### Implementation Notes
```python
# SQLAlchemy model
from ulid import ULID
from sqlalchemy import String

class ULIDMixin:
    @declared_attr
    def ulid(cls):
        # TEXT format for both PostgreSQL and SQLite
        return Column(
            String(26),
            unique=True,
            nullable=False,
            default=lambda: str(ULID())
        )

# Usage in worker
from ulid import ULID

file_ulid = ULID()
result_ulid = ULID()

ccf_file = CrosscorrelationCartesianFile(
    ulid=str(file_ulid),
    filepath="..."
)

xcorr = CrosscorrelationCartesian(
    ulid=str(result_ulid),
    file_ulid=str(file_ulid)
)
```

### Space Savings
**Typical dataset** (1 year, 10 stations, ~100k records):
- UUID: 36 bytes × 100k = 3.6 MB
- ULID: 26 bytes × 100k = 2.6 MB
- **Savings: 1 MB per 100k records**

Plus cleaner logs without hyphens:
```
# ULID
Processing datachunk: 01ARZ3NDEKTSV4RRFFQ69G5FAV

# UUID
Processing datachunk: 550e8400-e29b-41d4-a716-446655440000
```

---

## Research Question 2: SQLAlchemy 2.0 Migration Path

### Decision
Adopt **SQLAlchemy 2.0** with incremental migration strategy.

### Rationale
- SQLAlchemy 1.4 enters maintenance mode soon
- 2.0 offers better performance and async support
- Breaking changes are well-documented
- Can enable 2.0 deprecation warnings in 1.4 first

### Key Breaking Changes
1. **Query API**: `session.query()` → `session.execute(select())`
2. **Relationships**: Lazy loading patterns change
3. **Autocommit**: Removed, explicit transactions required
4. **Connection handling**: Context managers enforced

### Migration Strategy
**Phase 1**: Enable deprecation warnings
```python
# In conftest.py or database.py
import warnings
from sqlalchemy import exc as sa_exc
warnings.simplefilter("always", sa_exc.SADeprecationWarning)
```

**Phase 2**: Migrate queries incrementally
```python
# OLD (1.4 style)
results = session.query(Datachunk).filter_by(station_id=1).all()

# NEW (2.0 style)
from sqlalchemy import select
stmt = select(Datachunk).where(Datachunk.station_id == 1)
results = session.execute(stmt).scalars().all()
```

**Phase 3**: Update relationship lazy loading
```python
# OLD
relationship("Parent", lazy="joined")

# NEW
relationship("Parent", lazy="selectin")  # Or explicit joinedload()
```

### Alternatives Considered
1. **Stay on SQLAlchemy 1.4**
   - Rejected: End of active development, missing performance improvements

2. **Big-bang migration**
   - Rejected: Too risky, prefer incremental with testing

### References
- [SQLAlchemy 2.0 Migration Guide](https://docs.sqlalchemy.org/en/20/changelog/migration_20.html)
- [What's New in 2.0](https://docs.sqlalchemy.org/en/20/changelog/whatsnew_20.html)

---

## Research Question 3: Resume Detection Strategy

### Decision
**Database-query-based checkpoint detection** using UUID existence checks.

### Rationale
- Simple: Query database for existing UUIDs before processing
- Reliable: Database is source of truth for completed work
- Atomic: Either record exists or it doesn't
- No additional checkpoint files needed

### Implementation Pattern
```python
def process_batch_with_resume(tasks: List[Task]) -> List[Result]:
    """Process tasks, skipping already-completed work."""

    # Generate UUIDs upfront for all tasks
    task_uuids = {task.id: uuid.uuid4() for task in tasks}

    # Check which UUIDs already exist in database
    existing_uuids = set(
        db.session.execute(
            select(Result.uuid)
            .where(Result.uuid.in_(task_uuids.values()))
        ).scalars().all()
    )

    # Filter to only pending tasks
    pending_tasks = [
        task for task in tasks
        if task_uuids[task.id] not in existing_uuids
    ]

    logger.info(
        f"Resume detected: {len(tasks) - len(pending_tasks)} already complete, "
        f"{len(pending_tasks)} remaining"
    )

    # Process only pending tasks
    return [process_task(task, task_uuids[task.id]) for task in pending_tasks]
```

### Alternatives Considered
1. **Checkpoint files** (`.checkpoint` markers on disk)
   - Pros: Works without database
   - Cons: Can get out of sync, filesystem-dependent, not atomic
   - Rejected: Database already tracks state

2. **Processing state table** (separate status tracking)
   - Pros: Explicit state machine
   - Cons: Additional complexity, duplicate information
   - Rejected: Existence of result record IS the checkpoint

3. **Timestamp-based** (check file modification times)
   - Pros: No database query
   - Cons: Unreliable (files can be touched), no guarantee of completion
   - Rejected: Not deterministic

### Edge Cases Handled
- **Partial batch completion**: Only reprocess failed items
- **Disk full during write**: Database record not created, file not written, task retried
- **Database crash mid-insert**: Transaction rollback, no partial state
- **Kill -9 during processing**: Next run queries database, skips completed work

---

## Research Question 4: Config ID Collision Handling

### Decision
**Namespace-aware validation** with optional username prefixes and collision detection on import.

### Rationale
- Human-readable IDs make collisions more likely than auto-increment
- Scientific users share configs across institutions
- Need both: conflict detection AND resolution strategy

### Config ID Format
```toml
[config]
id = "dc_2023_highfreq_v1"  # Recommended: descriptive without username
# OR
id = "jsmith/dc_2023_highfreq_v1"  # With namespace for shared configs
```

### Collision Detection & Resolution
```python
def import_config(config_data: dict, allow_overwrite: bool = False) -> Config:
    """Import configuration with collision handling."""

    config_id = config_data['config']['id']

    # Check for existing config
    existing = db.session.execute(
        select(Config).where(Config.config_id == config_id)
    ).scalar_one_or_none()

    if existing:
        if not allow_overwrite:
            # Prompt user for resolution
            raise ConfigCollisionError(
                f"Configuration '{config_id}' already exists. Options:\n"
                f"1. Rename incoming config\n"
                f"2. Overwrite existing (use --force)\n"
                f"3. Skip import"
            )
        else:
            # Overwrite mode: update existing
            logger.warning(f"Overwriting config '{config_id}'")
            existing.update_from_dict(config_data)
            return existing

    # No collision, create new
    return Config.from_dict(config_data)
```

### Namespace Guidelines
**When to use namespace**:
- Sharing configs publicly (e.g., publications)
- Institutional standard pipelines
- Multi-user environments

**When to omit namespace**:
- Personal local configs
- Single-user projects
- Organization-internal use

### Alternatives Considered
1. **Auto-rename on collision** (append `_1`, `_2`, etc.)
   - Rejected: Silently changes IDs, breaks references

2. **UUID-based config IDs**
   - Rejected: Defeats purpose of human-readable IDs

3. **Mandatory namespaces** (always require username)
   - Rejected: Overkill for single-user scenarios

### Validation Rules
- Config ID must match: `^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)?$`
- Max length: 255 characters
- Reserved prefixes: `noiz/`, `system/`, `default/`

---

## Research Question 5: Execution Backend Abstraction

### Decision
Create **ExecutionBackend** interface with three implementations: Sequential, Multiprocessing, Dask.

### Rationale (from refactoring_roadmap.rst)
- Dask overhead unnecessary for local processing
- Most users have 4-8 cores, not distributed clusters
- Simpler debugging with sequential/multiprocessing

### Implementation
```python
from abc import ABC, abstractmethod
from enum import Enum

class ExecutionMode(Enum):
    SEQUENTIAL = "sequential"
    MULTIPROCESSING = "multiprocessing"
    DASK = "dask"

class ExecutionBackend(ABC):
    @abstractmethod
    def map(self, func, inputs):
        pass

    @abstractmethod
    def shutdown(self):
        pass

class SequentialBackend(ExecutionBackend):
    def map(self, func, inputs):
        return [func(inp) for inp in inputs]

    def shutdown(self):
        pass

class MultiprocessingBackend(ExecutionBackend):
    def __init__(self, workers=4):
        from multiprocessing import Pool
        self.pool = Pool(processes=workers)

    def map(self, func, inputs):
        return self.pool.map(func, inputs)

    def shutdown(self):
        self.pool.close()
        self.pool.join()

class DaskBackend(ExecutionBackend):
    def __init__(self, scheduler_address=None):
        from dask.distributed import Client
        self.client = Client(scheduler_address) if scheduler_address else Client()

    def map(self, func, inputs):
        futures = self.client.map(func, inputs)
        return self.client.gather(futures)

    def shutdown(self):
        self.client.close()
```

---

## Research Question 6: mseedindex Modernization

### Decision
Use **mseedindex from PyPI with JSON output mode** instead of Docker-compiled PostgreSQL-only version.

### Rationale
- **PyPI package available**: `mseedindex==3.0.8` (August 2025) - modern, maintained
- **JSON output decouples from database**: `-json output.json` produces portable format
- **No compilation needed**: Pre-built wheels for all platforms
- **Database-agnostic**: Eliminates PostgreSQL-specific types (HSTORE, ARRAY, NUMRANGE)
- **Simpler installation**: Just `pip install noiz` includes mseedindex automatically

### Current Approach (Problems)
```bash
# Ancient mseedindex compiled with PostgreSQL flags in Docker
# Writes directly to database with HSTORE/ARRAY/NUMRANGE types
# Blocks SQLite support entirely
```

### New Approach (JSON-based)
```bash
# mseedindex installed from PyPI
mseedindex -json /tmp/index.json seismic_data/*.mseed

# Noiz parses JSON and inserts into simplified table
{
  "network": "XX",
  "station": "STA",
  "channel": "HHZ",
  "starttime": "2023-01-01T00:00:00",
  "endtime": "2023-01-01T23:59:59",
  "filename": "/path/to/data.mseed",
  "byteoffset": 0,
  "bytes": 512000
  // ... simple fields only, no HSTORE/ARRAY
}
```

### Tsindex Model Changes
- Remove `HSTORE` (timeindex) → Store as JSON or flatten
- Remove `ARRAY(NUMRANGE)` (timespans) → Store as JSON array
- Remove `ARRAY(NUMERIC)` (timerates) → Store as JSON array
- Keep essential fields: network, station, location, channel, times, filename, offsets

### Alternatives Considered
1. **Keep Docker-compiled mseedindex**
   - Rejected: Complex installation, blocks SQLite, locks to old version

2. **Use ObsPy's tsindex module directly**
   - Considered: ObsPy has TSIndex functionality
   - Rejected: Still uses SQLAlchemy schema, same HSTORE issues
   - May revisit: If ObsPy's approach is more flexible

3. **Write custom miniSEED indexer**
   - Rejected: Reinventing wheel, mseedindex is proven

### Implementation Impact
- Add `mseedindex>=3.0.8` to pyproject.toml dependencies
- Modify `add_seismic_data` to use `-json` mode
- Create JSON parser for tsindex data
- Simplify Tsindex model (remove PostgreSQL types)
- Create new migration for simplified schema

---

## Summary of Key Decisions

| Decision Area | Choice | Primary Reason |
|---------------|--------|----------------|
| Unique IDs | TEXT ULID (CHAR(26)) | 28% smaller than UUID, human-readable, hyphen-free |
| SQLAlchemy Version | Stay on 1.4 (defer 2.0) | ObsPy 1.4.2 blocks SQLAlchemy 2.0 |
| Resume Detection | Database ULID queries | Simple, reliable, atomic |
| Config ID Collisions | Namespace-aware validation | Flexible, supports sharing |
| Execution Backends | Sequential/Multiprocessing/Dask | Right tool for each scale |
| mseedindex | PyPI package with JSON output | Decouples from DB, enables SQLite |

---

## Open Questions for Implementation Phase

1. **ULID Index Strategy**: Single index vs composite with other frequently-queried fields?
   - Defer to performance testing with realistic datasets

2. **SQLite Write-Ahead Logging**: Enable by default for better concurrency?
   - Test with parallel processing workloads

3. **Config Version Comparison**: Semantic versioning enforcement or freeform?
   - Start with freeform strings, add semver validation if needed

4. **Dask Dependency**: Make truly optional or keep as default install?
   - Profile install time/size impact, consider `noiz[dask]` extra

5. **ULID Library**: Use `python-ulid` or alternative?
   - `python-ulid` is well-maintained, pure Python, sufficient performance

---

## References

- [docs/content/development/design_documents/refactoring_roadmap.rst](../../docs/content/development/design_documents/refactoring_roadmap.rst)
- [docs/content/development/design_documents/config_system.rst](../../docs/content/development/design_documents/config_system.rst)
- [SQLAlchemy 2.0 Documentation](https://docs.sqlalchemy.org/en/20/)
- [ULID Specification](https://github.com/ulid/spec)
- [python-ulid Library](https://github.com/mdomke/python-ulid)

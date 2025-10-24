# ULID Implementation Patterns Research

**Date**: 2025-10-21
**Phase**: Phase 5 - SQLite Migration
**Spec**: 005 - ULID Primary Keys

## Executive Summary

This document analyzes the current ULID implementation patterns in the Noiz codebase to inform the decision on how to migrate from dual integer/ULID columns to ULID-only primary keys for SQLite compatibility.

## 1. ULID Best Practices for SQLAlchemy

### 1.1 Current ULIDMixin Implementation

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/mixins.py` (lines 21-42)

```python
class ULIDMixin:
    """Mixin to add ULID field to models for parallel processing safety.

    ULIDs (Universally Unique Lexicographically Sortable Identifiers) are generated
    before database insert, enabling workers to create objects with stable unique
    identifiers that can be referenced before database commits.

    This fixes the parallel processing data loss bug where foreign key relationships
    failed because objects were created without IDs.
    """

    @declared_attr
    def ulid(cls):
        """ULID field: 26-character unique identifier.

        Format: 01ARZ3NDEKTSV4RRFFQ69G5FAV
        - Globally unique (80 bits randomness)
        - Human-readable (no hyphens)
        - Lexicographically sortable by creation time
        """
        return db.Column(db.String(26), unique=True, nullable=False, default=lambda: str(ULID()))
```

**Key Characteristics**:
- Uses `@declared_attr` decorator for SQLAlchemy mixin pattern
- ULID stored as `String(26)` in database
- Has `unique=True` constraint
- Has `nullable=False` constraint
- Provides `default=lambda: str(ULID())` for automatic generation
- **Important**: The default is a fallback; code explicitly generates ULIDs before object creation

### 1.2 Current Dual-Column Pattern

**Example from**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/datachunk.py` (lines 15-21, 23-58)

```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)


class Datachunk(ULIDMixin, db.Model):
    __tablename__ = "datachunk"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "component_id",
            "datachunk_params_id",
            name="unique_datachunk_per_timespan_per_station_per_processing",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"), nullable=False)
    datachunk_params_id = db.Column(
        "datachunk_params_id",
        db.Integer,
        db.ForeignKey("datachunk_params.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    sampling_rate = db.Column("sampling_rate", db.Float, nullable=False)
    npts = db.Column("npts", db.Integer, nullable=False)
    padded_npts = db.Column("padded_npts", db.Integer, nullable=True)
    datachunk_file_id = db.Column(
        "datachunk_file_id",
        db.BigInteger,
        db.ForeignKey("datachunk_file.id"),
        nullable=True,
    )
    file_ulid = db.Column(
        "file_ulid",
        db.String(26),
        db.ForeignKey("datachunk_file.ulid"),
        nullable=True,
    )
    device_id = db.Column("device_id", db.Integer, db.ForeignKey("device.id"), nullable=True)
```

**Pattern Observations**:
1. **Primary Key**: `id = db.Column("id", db.BigInteger, primary_key=True)`
   - Uses `BigInteger` type
   - Explicitly declared with `primary_key=True`
   - Database-generated autoincrement value

2. **ULID Field**: From `ULIDMixin`
   - `ulid = db.Column(db.String(26), unique=True, nullable=False, default=lambda: str(ULID()))`
   - Not a primary key (no `primary_key=True`)
   - Has unique constraint
   - Non-nullable

3. **Dual Foreign Keys**: Both integer and ULID foreign keys exist
   - `datachunk_file_id` (BigInteger) → `datachunk_file.id`
   - `file_ulid` (String(26)) → `datachunk_file.ulid`
   - Both nullable (migration safety)

### 1.3 Models Using ULIDMixin

From grep analysis, the following models use ULIDMixin:

**File Models** (inherit from `ULIDMixin, FileModelMixin`):
- `BeamformingFile` (`beamforming.py`)
- `PPSDFile` (`ppsd.py`)

**File Models** (inherit from `ULIDMixin, db.Model`):
- `DatachunkFile` (`datachunk.py`)
- `ProcessedDatachunkFile` (`datachunk.py`)
- `CrosscorrelationCartesianFile` (`crosscorrelation.py`)
- `CrosscorrelationCylindricalFile` (`crosscorrelation.py`)

**Result/Data Models**:
- `Datachunk` (`datachunk.py`)
- `ProcessedDatachunk` (`datachunk.py`)
- `BeamformingResult` (`beamforming.py`)
- `CrosscorrelationCartesian` (`crosscorrelation.py`)
- `CrosscorrelationCylindrical` (`crosscorrelation.py`)
- `PPSDResult` (`ppsd.py`)
- `CCFStack` (`stacking.py`)

**Models NOT Using ULIDMixin**:
- `Component` (uses Integer primary key)
- `Device` (uses Integer primary key)
- `Timespan` (uses BigInteger primary key)
- `QCOneResults`, `QCOneConfig` (uses Integer primary key)
- `*Params` classes (configuration, uses Integer primary key)
- Association tables (use composite foreign keys)

## 2. ULID Generation Patterns

### 2.1 Explicit ULID Generation Before Object Creation

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/processing/datachunk.py` (lines 826-849)

```python
# T069: Generate ULIDs BEFORE creating objects (fixes parallel processing bug)
file_ulid = ULID()
datachunk_ulid = ULID()

# T070: Create file with ULID
datachunk_file = DatachunkFile(ulid=str(file_ulid), filepath=str(filepath))
trimmed_st.write(datachunk_file.filepath, format="mseed")

sampling_rate: Union[str, float] = trimmed_st[0].stats.sampling_rate
npts: int = trimmed_st[0].stats.npts

# T070: Create datachunk with ULID and file_ulid reference
datachunk = Datachunk(
    ulid=str(datachunk_ulid),
    datachunk_params_id=processing_params.id,
    component_id=component.id,
    timespan_id=timespan.id,
    sampling_rate=sampling_rate,
    npts=npts,
    file=datachunk_file,
    file_ulid=str(file_ulid),
    padded_npts=padded_npts,
    device_id=component.device_id,
)
```

**Pattern Summary**:
1. Generate ULID objects BEFORE model instantiation: `file_ulid = ULID()`
2. Convert to string when passing to constructor: `ulid=str(file_ulid)`
3. Use the same ULID for foreign key reference: `file_ulid=str(file_ulid)`
4. Pass related object via relationship: `file=datachunk_file`

**Rationale** (from docstring in mixins.py):
> "ULIDs are generated before database insert, enabling workers to create objects with stable unique identifiers that can be referenced before database commits. This fixes the parallel processing data loss bug where foreign key relationships failed because objects were created without IDs."

### 2.2 Beamforming Pattern

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/processing/beamforming.py` (lines 598-604, 871-873)

```python
# Generate ULID for BeamformingResult
result_ulid = ULID()

logger.debug("Creating an empty BeamformingResult")
res = BeamformingResult(
    ulid=str(result_ulid), timespan_id=timespan.id, beamforming_params_id=beamforming_params.id
)

# ... later in code ...

def save_beamforming_file(
    self, params: BeamformingParams, ts: Timespan, result_ulid: ULID
) -> Optional[BeamformingFile]:
    # Generate file ULID
    file_ulid = ULID()
    bf = BeamformingFile(ulid=str(file_ulid))
    fpath = bf.find_empty_filepath(ts=ts, params=params)
```

**Pattern Summary**:
1. Generate result ULID first
2. Pass ULID to result constructor
3. Later, generate file ULID when creating file
4. ULIDs generated at different times for parent and child objects

### 2.3 Import Pattern

From all processing files using ULID:
```python
from ulid import ULID
```

Files importing ULID:
- `/Users/qsbt/noiz-group/noiz/src/noiz/processing/beamforming.py`
- `/Users/qsbt/noiz-group/noiz/src/noiz/processing/datachunk.py`
- `/Users/qsbt/noiz-group/noiz/src/noiz/api/crosscorrelations.py`
- `/Users/qsbt/noiz-group/noiz/src/noiz/api/stacking.py`
- `/Users/qsbt/noiz-group/noiz/src/noiz/processing/ppsd.py`

## 3. Foreign Key Patterns

### 3.1 Dual Foreign Key Pattern (Current)

**Example from**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/datachunk.py` (lines 47-58)

```python
datachunk_file_id = db.Column(
    "datachunk_file_id",
    db.BigInteger,
    db.ForeignKey("datachunk_file.id"),
    nullable=True,
)
file_ulid = db.Column(
    "file_ulid",
    db.String(26),
    db.ForeignKey("datachunk_file.ulid"),
    nullable=True,
)
```

**Pattern**:
- Two columns for same relationship
- Integer FK: `*_id` → `table.id`
- ULID FK: `file_ulid` (or similar) → `table.ulid`
- Both nullable for migration safety
- Relationship defined using integer FK:
  ```python
  file = db.relationship(
      "DatachunkFile",
      foreign_keys=[datachunk_file_id],
      uselist=False,
      lazy="joined",
  )
  ```

### 3.2 All Dual Foreign Key Examples

**Datachunk → DatachunkFile**:
```python
datachunk_file_id = db.Column("datachunk_file_id", db.BigInteger, db.ForeignKey("datachunk_file.id"), nullable=True)
file_ulid = db.Column("file_ulid", db.String(26), db.ForeignKey("datachunk_file.ulid"), nullable=True)
```

**ProcessedDatachunk → ProcessedDatachunkFile**:
```python
processed_datachunk_file_id = db.Column("processed_datachunk_file_id", db.BigInteger, db.ForeignKey("processed_datachunk_file.id"), nullable=True)
file_ulid = db.Column("file_ulid", db.String(26), db.ForeignKey("processed_datachunk_file.ulid"), nullable=True)
```

**CrosscorrelationCartesian → CrosscorrelationCartesianFile**:
```python
crosscorrelation_cartesian_file_id = db.Column("crosscorrelation_cartesian_file_id", db.BigInteger, db.ForeignKey("crosscorrelation_cartesian_file.id"), nullable=True)
file_ulid = db.Column("file_ulid", db.String(26), db.ForeignKey("crosscorrelation_cartesian_file.ulid"), nullable=True)
```

**CrosscorrelationCylindrical → CrosscorrelationCylindricalFile**:
```python
crosscorrelation_cylindrical_file_id = db.Column("crosscorrelation_cylindrical_file_id", db.BigInteger, db.ForeignKey("crosscorrelation_cylindrical_file.id"), nullable=True)
file_ulid = db.Column("file_ulid", db.String(26), db.ForeignKey("crosscorrelation_cylindrical_file.ulid"), nullable=True)
```

**BeamformingResult → BeamformingFile**:
```python
beamforming_file_id = db.Column("beamforming_file_id", db.BigInteger, db.ForeignKey("beamforming_file.id"), nullable=True)
file_ulid = db.Column("file_ulid", db.String(26), db.ForeignKey("beamforming_file.ulid"), nullable=True)
```

**PPSDResult → PPSDFile**:
```python
ppsd_file_id = db.Column("ppsd_file_id", db.BigInteger, db.ForeignKey("ppsd_file.id"), nullable=True)
file_ulid = db.Column("file_ulid", db.String(26), db.ForeignKey("ppsd_file.ulid"), nullable=True)
```

**Observation**: Consistent naming pattern for ULID foreign keys is `file_ulid` (not `*_ulid` matching the integer FK pattern).

### 3.3 Association Tables

**Example from**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/stacking.py` (lines 118-123)

```python
ccf_ccfstack_association_table = db.Table(
    "stacking_association",
    db.metadata,
    db.Column("crosscorrelation_cartesian_id", db.BigInteger, db.ForeignKey("crosscorrelation_cartesian.id")),
    db.Column("ccfstack_id", db.BigInteger, db.ForeignKey("ccfstack.id")),
)
```

**Pattern**:
- Uses `db.Table()` for many-to-many relationships
- References integer primary keys
- No ULID foreign keys in association tables currently
- Named with `association` or `association_table` prefix

**All Association Tables**:
1. `beamforming_association_datachunks` - BeamformingResult ↔ Datachunk
2. `beamforming_result_association_avg_abspower` - BeamformingResult ↔ BeamformingPeakAverageAbspower
3. `beamforming_result_association_avg_relpower` - BeamformingResult ↔ BeamformingPeakAverageRelpower
4. `beamforming_result_association_all_abspower` - BeamformingResult ↔ BeamformingPeakAllAbspower
5. `beamforming_result_association_all_relpower` - BeamformingResult ↔ BeamformingPeakAllRelpower
6. `stacking_association` - CrosscorrelationCartesian ↔ CCFStack
7. `association_table_soh_instr` - SOHInstr ↔ Component
8. `association_table_soh_gps` - SOHGps ↔ Component
9. `association_table_averaged_soh_gps_components` - AveragedSohGps ↔ Component
10. `association_table_event_confirmation_result_event_detection_result` - EventConfirmationResult ↔ EventDetectionResult
11. `association_table_event_confirmation_run_datachunk` - EventConfirmationRun ↔ Datachunk

## 4. Migration Patterns

### 4.1 Current Migration Structure

**Directory**: `/Users/qsbt/noiz-group/noiz/migrations/versions/`

**Files**:
- `.keep` - Empty placeholder
- `61bee3b59594_baseline_with_ulid_sqlite_compatible.py` - 51KB baseline migration
- `__pycache__/` - Compiled Python files

### 4.2 Baseline Migration Pattern

**Location**: `/Users/qsbt/noiz-group/noiz/migrations/versions/61bee3b59594_baseline_with_ulid_sqlite_compatible.py`

**Structure**:
```python
"""baseline_with_ulid_sqlite_compatible

Revision ID: 61bee3b59594
Revises:
Create Date: 2025-10-21 08:32:41.323312
"""
from alembic import op
import sqlalchemy as sa
import noiz

revision = '61bee3b59594'
down_revision = None
branch_labels = None
depends_on = None
```

**Key Pattern for ULID Tables** (lines 22-28, 88-94):
```python
op.create_table('beamforming_file',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('filepath', noiz.models.custom_db_types.PathInDB(), nullable=False),
    sa.Column('ulid', sa.String(length=26), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('ulid')
)

op.create_table('datachunk_file',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('filepath', sa.UnicodeText(), nullable=False),
    sa.Column('ulid', sa.String(length=26), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('ulid')
)
```

**Key Pattern for Tables with ULID FK** (lines 375-396):
```python
op.create_table('datachunk',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('component_id', sa.Integer(), nullable=False),
    sa.Column('datachunk_params_id', sa.Integer(), nullable=False),
    sa.Column('timespan_id', sa.BigInteger(), nullable=False),
    sa.Column('sampling_rate', sa.Float(), nullable=False),
    sa.Column('npts', sa.Integer(), nullable=False),
    sa.Column('padded_npts', sa.Integer(), nullable=True),
    sa.Column('datachunk_file_id', sa.BigInteger(), nullable=True),
    sa.Column('file_ulid', sa.String(length=26), nullable=True),
    sa.Column('device_id', sa.Integer(), nullable=True),
    sa.Column('ulid', sa.String(length=26), nullable=False),
    sa.ForeignKeyConstraint(['component_id'], ['component.id'], ),
    sa.ForeignKeyConstraint(['datachunk_file_id'], ['datachunk_file.id'], ),
    sa.ForeignKeyConstraint(['datachunk_params_id'], ['datachunk_params.id'], ),
    sa.ForeignKeyConstraint(['device_id'], ['device.id'], ),
    sa.ForeignKeyConstraint(['file_ulid'], ['datachunk_file.ulid'], ),
    sa.ForeignKeyConstraint(['timespan_id'], ['timespan.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('timespan_id', 'component_id', 'datachunk_params_id',
                        name='unique_datachunk_per_timespan_per_station_per_processing'),
    sa.UniqueConstraint('ulid')
)
```

**Migration Observations**:
1. ULID column always has `sa.UniqueConstraint('ulid')`
2. Both integer and ULID foreign key constraints defined
3. Primary key is still integer ID
4. ULID FKs are nullable
5. Uses custom types: `noiz.models.custom_db_types.PathInDB()`

### 4.3 SQLite Considerations

From the migration filename: `baseline_with_ulid_sqlite_compatible.py`

**SQLite-Specific Considerations**:
1. No PostgreSQL-specific types used in ULID-related columns
2. `String(26)` is SQLite-compatible
3. No use of `ARRAY`, `JSONB`, or PostgreSQL-specific types in ULID columns
4. `BigInteger` is SQLite-compatible (stored as INTEGER)

**Note**: The codebase recently removed PostgreSQL-specific types from other models (see git history: `feat(migrations): Remove PostgreSQL-specific types from tsindex`, `feat(models): Simplify Tsindex model for SQLite compatibility`).

## 5. Decision: ULID as Primary Key Pattern

### 5.1 Recommended Pattern

Based on the research, the recommended pattern for migrating to ULID-only primary keys:

```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"

    # ULID as primary key - override ULIDMixin to add primary_key=True
    id = db.Column("id", db.String(26), primary_key=True, unique=True, nullable=False, default=lambda: str(ULID()))
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)

class Datachunk(ULIDMixin, db.Model):
    __tablename__ = "datachunk"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "component_id",
            "datachunk_params_id",
            name="unique_datachunk_per_timespan_per_station_per_processing",
        ),
    )

    # ULID as primary key
    id = db.Column("id", db.String(26), primary_key=True, unique=True, nullable=False, default=lambda: str(ULID()))
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"), nullable=False)
    datachunk_params_id = db.Column("datachunk_params_id", db.Integer, db.ForeignKey("datachunk_params.id"), nullable=False)
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    sampling_rate = db.Column("sampling_rate", db.Float, nullable=False)
    npts = db.Column("npts", db.Integer, nullable=False)
    padded_npts = db.Column("padded_npts", db.Integer, nullable=True)

    # Single ULID foreign key (no more dual FKs)
    datachunk_file_id = db.Column(
        "datachunk_file_id",
        db.String(26),
        db.ForeignKey("datachunk_file.id"),
        nullable=True,
    )
    device_id = db.Column("device_id", db.Integer, db.ForeignKey("device.id"), nullable=True)

    # Relationships
    file = db.relationship("DatachunkFile", foreign_keys=[datachunk_file_id], uselist=False, lazy="joined")
```

**Key Changes**:
1. `id` column changes from `BigInteger` to `String(26)`
2. `id` column becomes `primary_key=True`
3. Remove old `ulid` field from mixin (now `id` is the ULID)
4. Remove dual foreign keys - only keep ULID foreign key
5. Rename foreign key from `file_ulid` to `datachunk_file_id` (standard FK naming)
6. Keep explicit ULID generation in code (don't rely on default)

### 5.2 Updated ULIDMixin

**Option A: Modify ULIDMixin to use 'id' as column name**

```python
class ULIDMixin:
    """Mixin to add ULID primary key field to models.

    ULIDs (Universally Unique Lexicographically Sortable Identifiers) are generated
    before database insert, enabling workers to create objects with stable unique
    identifiers that can be referenced before database commits.
    """

    @declared_attr
    def id(cls):
        """ULID primary key field: 26-character unique identifier.

        Format: 01ARZ3NDEKTSV4RRFFQ69G5FAV
        - Globally unique (80 bits randomness)
        - Human-readable (no hyphens)
        - Lexicographically sortable by creation time
        """
        return db.Column("id", db.String(26), primary_key=True, nullable=False, default=lambda: str(ULID()))
```

**Option B: Remove ULIDMixin, declare explicitly**

Remove `ULIDMixin` and declare `id` column explicitly in each model:
```python
class DatachunkFile(db.Model):
    __tablename__ = "datachunk_file"

    id = db.Column("id", db.String(26), primary_key=True, nullable=False, default=lambda: str(ULID()))
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

**Recommendation**: Use Option A (modify ULIDMixin) to minimize code changes and maintain consistency.

### 5.3 Code Generation Pattern (Unchanged)

The explicit ULID generation pattern in processing code should remain unchanged:

```python
# Generate ULIDs BEFORE creating objects
file_ulid = ULID()
datachunk_ulid = ULID()

# Create file with ULID as id
datachunk_file = DatachunkFile(id=str(file_ulid), filepath=str(filepath))

# Create datachunk with ULID as id and reference to file
datachunk = Datachunk(
    id=str(datachunk_ulid),
    datachunk_params_id=processing_params.id,
    component_id=component.id,
    timespan_id=timespan.id,
    sampling_rate=sampling_rate,
    npts=npts,
    file=datachunk_file,
    datachunk_file_id=str(file_ulid),  # Reference file by ULID
    padded_npts=padded_npts,
    device_id=component.device_id,
)
```

**Changes**:
- Use `id=str(file_ulid)` instead of `ulid=str(file_ulid)`
- Use `datachunk_file_id=str(file_ulid)` instead of `file_ulid=str(file_ulid)`

## 6. Rationale

### 6.1 Why ULID as Primary Key

**Advantages**:
1. **Parallel Processing Safety**: ULIDs are generated before database insert, enabling stable references in distributed workers
2. **SQLite Compatibility**: No reliance on PostgreSQL SERIAL/autoincrement
3. **Sortability**: Lexicographically sortable by creation time
4. **No ID Collisions**: Globally unique, no coordination needed
5. **Single Source of Truth**: One ID column instead of dual id/ulid columns
6. **Simpler Foreign Keys**: One FK column instead of dual *_id/*_ulid columns

**From Production Evidence**:
- Current code already generates ULIDs explicitly before object creation
- Parallel processing bug was fixed by using ULID references
- ULID pattern is proven and working in production

### 6.2 Why String(26) Type

**Advantages**:
1. **Universal Compatibility**: Works with PostgreSQL, SQLite, MySQL, etc.
2. **Human-Readable**: Can be read and debugged in database browsers
3. **No UUID Parsing**: No conversion needed, stored as generated
4. **Index-Friendly**: B-tree indexes work well with lexicographically sorted strings
5. **Fixed Length**: `String(26)` is fixed-width, performs like integer in indexes

**Disadvantages**:
- Slightly larger than UUID binary (26 bytes vs 16 bytes)
- String comparison slightly slower than integer comparison

**Verdict**: Advantages outweigh disadvantages for this use case.

### 6.3 Why Keep Explicit Generation

The current pattern of explicit ULID generation should be maintained:

```python
file_ulid = ULID()
obj = Model(id=str(file_ulid), ...)
```

**Rationale**:
1. **Parallel Processing**: Objects created without database connection need IDs
2. **Foreign Key References**: Parent IDs must be known before creating children
3. **Explicit > Implicit**: Clear when and where IDs are generated
4. **Testability**: Tests can provide deterministic IDs
5. **Debugging**: Easier to trace ID generation in logs

The `default=lambda: str(ULID())` in the column definition is a safety fallback, not the primary mechanism.

## 7. Alternatives Considered

### 7.1 Alternative 1: Keep Dual Columns

**Pattern**: Keep both `id` (BigInteger) and `ulid` (String) columns

**Rejected Because**:
- Violates DRY principle
- Doubles storage for IDs
- Creates ambiguity about which ID to use
- More complex foreign key management
- Defeats purpose of SQLite migration (simplification)

### 7.2 Alternative 2: Integer Primary Key + ULID Unique

**Pattern**: Keep integer `id` as PK, ULID as unique secondary index

**Rejected Because**:
- Integer IDs still require database coordination
- Defeats parallel processing safety benefits
- SQLite limitations with autoincrement in distributed scenarios
- More complex migration (must populate integer IDs)

### 7.3 Alternative 3: UUID Binary Type

**Pattern**: Use UUID stored as 16-byte binary

**Rejected Because**:
- Not human-readable in database browsers
- ULIDs already in use and working
- Not lexicographically sortable by time
- Would require changing all existing ULID generation code

### 7.4 Alternative 4: Composite Keys

**Pattern**: Use natural composite keys (e.g., `(timespan_id, component_id)`)

**Rejected Because**:
- More complex foreign key management
- Slower joins with multi-column keys
- Doesn't solve parallel processing coordination
- Many models don't have natural composite keys

## 8. Migration Strategy

### 8.1 Phased Approach

**Phase 1: Add ULID Primary Keys** (while keeping old columns)
1. Add new `id_ulid` column with ULID primary key
2. Add `*_id_ulid` foreign key columns
3. Populate new columns from existing `ulid` columns
4. Add indexes and constraints

**Phase 2: Switch References**
1. Update relationships to use new ULID FKs
2. Update queries to use new ULID PKs
3. Test thoroughly

**Phase 3: Remove Old Columns**
1. Drop old integer `id` column
2. Rename `id_ulid` to `id`
3. Drop old `*_id` foreign key columns
4. Rename `*_id_ulid` to `*_id`

**Phase 4: Update Code**
1. Change model definitions to use ULID PKs
2. Update ULID generation to use `id=` parameter
3. Update foreign key references

### 8.2 Data Migration Considerations

**For Existing Data**:
1. All ULID-using models already have populated `ulid` columns
2. Can copy `ulid` → `id_ulid` for data migration
3. No need to generate new ULIDs

**For Association Tables**:
1. Must update foreign keys to reference ULID PKs
2. Can derive from existing relationships
3. May need temporary migration tables

### 8.3 SQLite-Specific Migration

**SQLite Limitations**:
1. Cannot drop columns directly (must recreate table)
2. Cannot modify column types (must recreate table)
3. Foreign key constraints must be recreated

**Strategy**:
```python
# For each table:
1. Create new table with ULID primary key
2. Copy data from old table (mapping old id → existing ulid)
3. Drop old table
4. Rename new table to old name
```

## 9. Testing Strategy

### 9.1 Model Tests

**Test Cases**:
1. ✓ ULID generation on model creation
2. ✓ ULID uniqueness across instances
3. ✓ ULID as primary key in queries
4. ✓ Foreign key relationships work with ULIDs
5. ✓ Association tables work with ULIDs

### 9.2 Parallel Processing Tests

**Critical Tests**:
1. ✓ Objects created without database connection have valid IDs
2. ✓ Parent-child relationships work before database commit
3. ✓ Dask workers can create objects with stable IDs
4. ✓ No ID collision in concurrent object creation

### 9.3 Migration Tests

**Test Cases**:
1. ✓ Migrate existing data preserves all relationships
2. ✓ Queries return same results before/after migration
3. ✓ Foreign key constraints enforced correctly
4. ✓ Indexes perform acceptably

## 10. Implementation Checklist

### Models to Update
- [ ] `DatachunkFile`
- [ ] `Datachunk`
- [ ] `ProcessedDatachunkFile`
- [ ] `ProcessedDatachunk`
- [ ] `BeamformingFile`
- [ ] `BeamformingResult`
- [ ] `CrosscorrelationCartesianFile`
- [ ] `CrosscorrelationCartesian`
- [ ] `CrosscorrelationCylindricalFile`
- [ ] `CrosscorrelationCylindrical`
- [ ] `PPSDFile`
- [ ] `PPSDResult`
- [ ] `CCFStack`

### Association Tables to Update
- [ ] `beamforming_association_datachunks`
- [ ] `beamforming_result_association_avg_abspower`
- [ ] `beamforming_result_association_avg_relpower`
- [ ] `beamforming_result_association_all_abspower`
- [ ] `beamforming_result_association_all_relpower`
- [ ] `stacking_association`

### Code Files to Update
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/models/mixins.py` - Update ULIDMixin
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/models/datachunk.py` - Update Datachunk models
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/models/beamforming.py` - Update Beamforming models
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/models/crosscorrelation.py` - Update Crosscorrelation models
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/models/ppsd.py` - Update PPSD models
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/models/stacking.py` - Update Stacking models
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/processing/datachunk.py` - Update ULID generation
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/processing/beamforming.py` - Update ULID generation
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/api/crosscorrelations.py` - Update ULID generation
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/api/stacking.py` - Update ULID generation
- [ ] `/Users/qsbt/noiz-group/noiz/src/noiz/processing/ppsd.py` - Update ULID generation

### Migration Files
- [ ] Create Alembic migration for ULID primary key transition
- [ ] Test migration with sample database
- [ ] Document rollback procedure

## 11. Code Examples

### 11.1 Before (Current Pattern)

```python
# Model definition
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"
    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)

class Datachunk(ULIDMixin, db.Model):
    __tablename__ = "datachunk"
    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    datachunk_file_id = db.Column("datachunk_file_id", db.BigInteger, db.ForeignKey("datachunk_file.id"), nullable=True)
    file_ulid = db.Column("file_ulid", db.String(26), db.ForeignKey("datachunk_file.ulid"), nullable=True)

    file = db.relationship("DatachunkFile", foreign_keys=[datachunk_file_id], uselist=False, lazy="joined")

# Object creation
file_ulid = ULID()
datachunk_ulid = ULID()

datachunk_file = DatachunkFile(ulid=str(file_ulid), filepath=str(filepath))
datachunk = Datachunk(
    ulid=str(datachunk_ulid),
    file=datachunk_file,
    file_ulid=str(file_ulid),
)
```

### 11.2 After (ULID Primary Key Pattern)

```python
# Updated mixin
class ULIDMixin:
    @declared_attr
    def id(cls):
        return db.Column("id", db.String(26), primary_key=True, nullable=False, default=lambda: str(ULID()))

# Model definition
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"
    # id field from ULIDMixin (ULID as PK)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)

class Datachunk(ULIDMixin, db.Model):
    __tablename__ = "datachunk"
    # id field from ULIDMixin (ULID as PK)
    datachunk_file_id = db.Column("datachunk_file_id", db.String(26), db.ForeignKey("datachunk_file.id"), nullable=True)

    file = db.relationship("DatachunkFile", foreign_keys=[datachunk_file_id], uselist=False, lazy="joined")

# Object creation (minimal changes)
file_ulid = ULID()
datachunk_ulid = ULID()

datachunk_file = DatachunkFile(id=str(file_ulid), filepath=str(filepath))
datachunk = Datachunk(
    id=str(datachunk_ulid),
    file=datachunk_file,
    datachunk_file_id=str(file_ulid),
)
```

**Key Differences**:
1. Remove explicit `id` column declaration (comes from mixin)
2. Remove old `ulid` column reference
3. Change `ulid=` to `id=` in constructor
4. Change `file_ulid=` to `datachunk_file_id=` in constructor
5. Foreign key column type changes from `BigInteger` to `String(26)`
6. Single foreign key column instead of dual columns

## 12. References

### Documentation
- ULID Specification: https://github.com/ulid/spec
- Python ULID Library: https://github.com/mdomke/python-ulid
- SQLAlchemy Mixins: https://docs.sqlalchemy.org/en/20/orm/declarative_mixins.html

### Codebase References
- `src/noiz/models/mixins.py` - ULIDMixin implementation
- `src/noiz/processing/datachunk.py` - ULID generation pattern
- `migrations/versions/61bee3b59594_baseline_with_ulid_sqlite_compatible.py` - Current migration baseline

### Related Git Commits
- `f62ab84a` - docs: Add next session handoff for SQLite work
- `04b3c0d0` - refactor(models): Rename tsindex table to raw_data_index
- `eecc2333` - feat(migrations): Remove PostgreSQL-specific types from tsindex
- `33d0c0bf` - feat(models): Simplify Tsindex model for SQLite compatibility

---

**End of Research Document**

# Model Contracts: ULID Primary Keys

**Date**: 2025-10-21
**Phase**: Phase 5 - SQLite Migration
**Spec**: 005 - ULID Primary Keys

## Overview

This document defines the technical contracts for ULID primary key implementation in Noiz models. These contracts establish the interfaces, guarantees, and requirements that must be maintained during the migration from integer to ULID primary keys.

## 1. ULIDMixin Contract

### 1.1 What ULIDMixin Provides

The `ULIDMixin` provides a single, standardized ULID primary key field to all models that inherit from it.

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/mixins.py`

**Contract**:

```python
class ULIDMixin:
    """Mixin to add ULID primary key field to models.

    ULIDs (Universally Unique Lexicographically Sortable Identifiers) are generated
    before database insert, enabling workers to create objects with stable unique
    identifiers that can be referenced before database commits.

    This enables distributed parallel processing without database coordination.
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

**Guarantees**:

1. **Field Name**: Always provides a field named `id` (not `ulid`)
2. **Field Type**: `String(26)` - fixed-width string suitable for indexing
3. **Primary Key**: `primary_key=True` - this is the table's primary key
4. **Not Nullable**: `nullable=False` - every instance must have a ULID
5. **Default Generator**: `default=lambda: str(ULID())` - fallback ULID generation
6. **Column Name**: Database column named `"id"` (explicit string argument)

**What ULIDMixin Does NOT Provide**:

- Foreign key relationships (models must declare these themselves)
- Unique constraints beyond the primary key constraint
- Any other columns or fields

### 1.2 Example Usage

```python
from noiz.models.mixins import ULIDMixin
from noiz.database import db

class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"

    # id field automatically provided by ULIDMixin
    # NO need to declare: id = db.Column(...)

    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

## 2. ULID Primary Key Model Contract

### 2.1 Requirements for Models Using ULIDMixin

Any model that inherits from `ULIDMixin` MUST:

1. **Inherit from ULIDMixin first** (before `db.Model` or other mixins)
2. **NOT declare an explicit `id` field** (inherited from mixin)
3. **Specify a `__tablename__`** (standard SQLAlchemy requirement)
4. **Generate ULIDs explicitly before object creation** (don't rely on default)

```python
class MyModel(ULIDMixin, db.Model):  # ✓ ULIDMixin first
    __tablename__ = "my_model"       # ✓ Table name specified

    # ✓ NO explicit id declaration
    # id = db.Column(...)  # ✗ WRONG - don't do this

    other_field = db.Column("other_field", db.String(100))
```

### 2.2 Foreign Key Type Contract

When referencing a ULID primary key model, foreign keys MUST:

1. **Use `String(26)` type** (matching the primary key type)
2. **Reference the target table's `id` column**
3. **Follow standard naming convention**: `<target_table>_id` (singular)

**Correct Examples**:

```python
# Reference to DatachunkFile (ULID model)
datachunk_file_id = db.Column(
    "datachunk_file_id",
    db.String(26),                        # ✓ String(26) for ULID FK
    db.ForeignKey("datachunk_file.id"),   # ✓ References .id
    nullable=True,
)

# Reference to Datachunk (ULID model)
datachunk_id = db.Column(
    "datachunk_id",
    db.String(26),                        # ✓ String(26) for ULID FK
    db.ForeignKey("datachunk.id"),        # ✓ References .id
    nullable=False,
)
```

**Incorrect Examples**:

```python
# ✗ WRONG - Integer type for ULID FK
datachunk_file_id = db.Column(
    "datachunk_file_id",
    db.BigInteger,                        # ✗ Wrong type
    db.ForeignKey("datachunk_file.id"),
    nullable=True,
)

# ✗ WRONG - References non-existent .ulid field
datachunk_file_id = db.Column(
    "datachunk_file_id",
    db.String(26),
    db.ForeignKey("datachunk_file.ulid"),  # ✗ No .ulid field anymore
    nullable=True,
)

# ✗ WRONG - Dual foreign keys (old pattern)
datachunk_file_id = db.Column("datachunk_file_id", db.BigInteger, db.ForeignKey("datachunk_file.id"))
file_ulid = db.Column("file_ulid", db.String(26), db.ForeignKey("datachunk_file.ulid"))
```

### 2.3 Complete Model Example

```python
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

    # id field from ULIDMixin (ULID primary key - String(26))

    # Foreign keys to non-ULID models (Integer/BigInteger)
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"), nullable=False)
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    datachunk_params_id = db.Column("datachunk_params_id", db.Integer, db.ForeignKey("datachunk_params.id"), nullable=False)
    device_id = db.Column("device_id", db.Integer, db.ForeignKey("device.id"), nullable=True)

    # Foreign key to ULID model (String(26))
    datachunk_file_id = db.Column(
        "datachunk_file_id",
        db.String(26),
        db.ForeignKey("datachunk_file.id"),
        nullable=True,
    )

    # Data fields
    sampling_rate = db.Column("sampling_rate", db.Float, nullable=False)
    npts = db.Column("npts", db.Integer, nullable=False)
    padded_npts = db.Column("padded_npts", db.Integer, nullable=True)

    # Relationships
    file = db.relationship("DatachunkFile", foreign_keys=[datachunk_file_id], uselist=False, lazy="joined")
    component = db.relationship("Component", foreign_keys=[component_id])
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id], back_populates="datachunks")
```

## 3. ULID Generation Contract

### 3.1 When to Generate ULIDs

ULIDs MUST be generated explicitly before object creation in these scenarios:

1. **Parallel Processing**: When objects are created by Dask workers without immediate database access
2. **Parent-Child Relationships**: When child objects need to reference parent IDs before database commit
3. **Foreign Key References**: When an object's ID must be known before creating related objects

**Contract**: Do NOT rely on the `default` parameter in the column definition. The default is a fallback only.

### 3.2 How to Generate ULIDs

**Import**:

```python
from ulid import ULID
```

**Generation Pattern**:

```python
# Generate ULID objects BEFORE model instantiation
file_ulid = ULID()           # Generate as ULID object
datachunk_ulid = ULID()      # Generate as ULID object

# Convert to string when passing to constructor
datachunk_file = DatachunkFile(
    id=str(file_ulid),       # ✓ Pass as string via id= parameter
    filepath=str(filepath)
)

datachunk = Datachunk(
    id=str(datachunk_ulid),                # ✓ Pass as string via id= parameter
    datachunk_file_id=str(file_ulid),      # ✓ Reference same ULID for FK
    component_id=component.id,
    timespan_id=timespan.id,
    # ... other fields
)
```

### 3.3 Complete Code Example

**Location**: Based on `/Users/qsbt/noiz-group/noiz/src/noiz/processing/datachunk.py`

```python
from ulid import ULID
from noiz.models import Datachunk, DatachunkFile

# Step 1: Generate ULIDs BEFORE creating objects
file_ulid = ULID()
datachunk_ulid = ULID()

# Step 2: Create file object with ULID
datachunk_file = DatachunkFile(
    id=str(file_ulid),           # Pass ULID as id
    filepath=str(filepath)
)

# Write file to disk (doesn't require database commit)
trimmed_st.write(datachunk_file.filepath, format="mseed")

# Step 3: Create datachunk with ULID and FK reference
datachunk = Datachunk(
    id=str(datachunk_ulid),                    # Pass ULID as id
    datachunk_file_id=str(file_ulid),          # Reference file by ULID
    datachunk_params_id=processing_params.id,
    component_id=component.id,
    timespan_id=timespan.id,
    sampling_rate=sampling_rate,
    npts=npts,
    file=datachunk_file,                       # Also pass relationship object
    padded_npts=padded_npts,
    device_id=component.device_id,
)

# Step 4: Add to database session (commit happens later in batch)
db.session.add(datachunk)
```

**Key Principles**:

1. Generate ULID as `ULID()` object first
2. Convert to string with `str(ulid)` when passing to models
3. Use same ULID value for both `id=` and foreign key references
4. Pass both FK scalar value and relationship object
5. Objects can be created and referenced before database commit

### 3.4 Anti-Patterns to Avoid

```python
# ✗ WRONG - Using old 'ulid' parameter
datachunk_file = DatachunkFile(
    ulid=str(file_ulid),  # ✗ No 'ulid' parameter anymore
    filepath=str(filepath)
)

# ✗ WRONG - Using old 'file_ulid' FK name
datachunk = Datachunk(
    id=str(datachunk_ulid),
    file_ulid=str(file_ulid),  # ✗ No 'file_ulid' column anymore
)

# ✗ WRONG - Relying on default generation
datachunk_file = DatachunkFile(
    filepath=str(filepath)  # ✗ No id provided - will use fallback default
)

# ✗ WRONG - Not converting ULID to string
datachunk_file = DatachunkFile(
    id=file_ulid,  # ✗ Must convert to string: str(file_ulid)
    filepath=str(filepath)
)

# ✗ WRONG - Integer FK type for ULID reference
datachunk = Datachunk(
    id=str(datachunk_ulid),
    datachunk_file_id=123,  # ✗ Must be ULID string, not integer
)
```

## 4. Association Table Contract

### 4.1 Requirements for Association Tables

Association tables that reference ULID primary key models MUST:

1. **Update foreign key column types** to `String(26)` for ULID references
2. **Keep standard foreign key column types** for non-ULID references
3. **Maintain the same table structure** (table name, constraints, etc.)

### 4.2 Example: Beamforming-Datachunk Association

**Before** (Integer FKs):

```python
association_table_beamforming_results_datachunks = db.Table(
    "beamforming_association_datachunks",
    db.metadata,
    db.Column("datachunk_id", db.BigInteger, db.ForeignKey("datachunk.id")),
    db.Column("beamforming_result_id", db.BigInteger, db.ForeignKey("beamforming_result.id")),
    db.UniqueConstraint("beamforming_result_id", "datachunk_id"),
)
```

**After** (ULID FKs):

```python
association_table_beamforming_results_datachunks = db.Table(
    "beamforming_association_datachunks",
    db.metadata,
    db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id")),           # ✓ String(26) for ULID
    db.Column("beamforming_result_id", db.String(26), db.ForeignKey("beamforming_result.id")),  # ✓ String(26) for ULID
    db.UniqueConstraint("beamforming_result_id", "datachunk_id"),
)
```

### 4.3 Example: Mixed FK Types

**Before**:

```python
association_table_event_confirmation_run_datachunk = db.Table(
    "association_table_event_confirmation_run_datachunk",
    db.metadata,
    db.Column("event_confirmation_run_id", db.BigInteger, db.ForeignKey("event_confirmation_run.id")),
    db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id")),
)
```

**After**:

```python
association_table_event_confirmation_run_datachunk = db.Table(
    "association_table_event_confirmation_run_datachunk",
    db.metadata,
    db.Column("event_confirmation_run_id", db.BigInteger, db.ForeignKey("event_confirmation_run.id")),  # ✓ Unchanged (non-ULID)
    db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id")),                          # ✓ String(26) for ULID
)
```

## 5. FileModelMixin Contract

### 5.1 Updated FileModelMixin Requirements

The `FileModelMixin` must be updated to work with `ULIDMixin`:

**Before** (Conflicts with ULIDMixin):

```python
class FileModelMixin(db.Model):
    __abstract__ = True
    id: int = db.Column("id", db.BigInteger, primary_key=True)  # ✗ Conflicts with ULIDMixin
    _filepath: Path = db.Column("filepath", PathInDB, nullable=False)
```

**After** (Compatible with ULIDMixin):

```python
class FileModelMixin(db.Model):
    __abstract__ = True
    # id field comes from ULIDMixin (removed explicit declaration)
    _filepath: Path = db.Column("filepath", PathInDB, nullable=False)
```

**Usage**:

```python
class BeamformingFile(ULIDMixin, FileModelMixin):
    __tablename__ = "beamforming_file"

    # id field from ULIDMixin (ULID as primary key)
    # _filepath field from FileModelMixin
    _file_model_type: str = "beamforming"
    _filename_extension: str = "npz"
```

## 6. Type Consistency Contract

### 6.1 Foreign Key Type Rules

**Rule 1**: Foreign keys MUST match the type of the referenced primary key

| Referenced Model Type | Referenced PK Type | FK Column Type |
|----------------------|-------------------|----------------|
| ULID model | `String(26)` | `String(26)` |
| Timespan | `BigInteger` | `BigInteger` |
| Component, Device, *Params | `Integer` | `Integer` |

**Rule 2**: All foreign keys to the same table MUST use the same type

```python
# ✓ CORRECT - Consistent types
class Model1:
    datachunk_id = db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id"))

class Model2:
    datachunk_id = db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id"))

# ✗ WRONG - Inconsistent types
class Model1:
    datachunk_id = db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id"))

class Model2:
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"))  # ✗ Wrong type
```

### 6.2 Type Consistency Fixes Required

These inconsistencies exist in the current codebase and MUST be fixed:

1. **BeamformingResult.timespan_id**:
   - Current: `Integer`
   - Should be: `BigInteger` (Timespan.id is BigInteger)

2. **PPSDResult.timespan_id**:
   - Current: `Integer`
   - Should be: `BigInteger` (Timespan.id is BigInteger)

## 7. Migration Contract

### 7.1 Data Preservation Guarantee

During migration, the system MUST guarantee:

1. **No data loss**: All existing records preserved
2. **ID stability**: ULID values from old `ulid` column become new `id` column values
3. **Relationship integrity**: All foreign key relationships maintained
4. **Constraint enforcement**: All unique constraints and foreign key constraints enforced

### 7.2 SQLite Migration Pattern

Because SQLite cannot alter column types, use the table recreation pattern:

```python
def upgrade():
    # 1. Create new table with ULID primary key
    op.create_table(
        'datachunk_new',
        sa.Column('id', sa.String(26), primary_key=True, nullable=False),
        sa.Column('datachunk_file_id', sa.String(26), nullable=True),
        # ... other columns with updated FK types ...
        sa.ForeignKeyConstraint(['datachunk_file_id'], ['datachunk_file.id']),
        # ... other constraints ...
    )

    # 2. Copy data (map old columns to new)
    op.execute("""
        INSERT INTO datachunk_new (id, datachunk_file_id, ...)
        SELECT ulid, file_ulid, ...
        FROM datachunk
    """)

    # 3. Drop old table
    op.drop_table('datachunk')

    # 4. Rename new table
    op.rename_table('datachunk_new', 'datachunk')
```

## 8. Testing Contract

### 8.1 Required Test Coverage

Every model migration MUST include tests for:

1. **ULID Generation**: Test that objects can be created with explicit ULID
2. **Uniqueness**: Test that duplicate ULIDs are rejected
3. **Foreign Keys**: Test that FK relationships work with ULID
4. **Queries**: Test that queries by ULID work correctly
5. **Parallel Processing**: Test that objects can be created before database commit

### 8.2 Example Test Structure

```python
def test_datachunk_ulid_primary_key():
    """Test that Datachunk uses ULID as primary key."""
    file_ulid = ULID()
    datachunk_ulid = ULID()

    # Create objects with explicit ULID
    datachunk_file = DatachunkFile(id=str(file_ulid), filepath="test.mseed")
    datachunk = Datachunk(
        id=str(datachunk_ulid),
        datachunk_file_id=str(file_ulid),
        # ... required fields ...
    )

    # Test ID is ULID string
    assert datachunk.id == str(datachunk_ulid)
    assert len(datachunk.id) == 26

    # Test FK reference works
    assert datachunk.datachunk_file_id == str(file_ulid)

    # Test query by ULID
    db.session.add(datachunk)
    db.session.commit()

    found = Datachunk.query.filter_by(id=str(datachunk_ulid)).first()
    assert found is not None
    assert found.id == str(datachunk_ulid)
```

## 9. Summary Checklist

### For Every ULID Model:

- [ ] Inherits from `ULIDMixin` (first in inheritance list)
- [ ] Does NOT declare explicit `id` field
- [ ] All foreign keys to ULID models use `String(26)`
- [ ] All foreign keys to non-ULID models use correct type (Integer/BigInteger)
- [ ] No dual foreign key columns (old `file_ulid` pattern removed)
- [ ] Code explicitly generates ULIDs with `ULID()` before object creation
- [ ] Code passes ULIDs as strings: `id=str(ulid)`
- [ ] Foreign keys use standard naming: `<table>_id` (not `file_ulid`)

### For Association Tables:

- [ ] Foreign keys to ULID models updated to `String(26)`
- [ ] Foreign keys to non-ULID models unchanged
- [ ] Table name and constraints preserved

### For Migrations:

- [ ] Uses table recreation pattern for SQLite
- [ ] Maps old `ulid` column to new `id` column
- [ ] Updates all foreign key column types
- [ ] Preserves all data and relationships
- [ ] Includes both upgrade and downgrade

### For Tests:

- [ ] Tests ULID generation and uniqueness
- [ ] Tests foreign key relationships
- [ ] Tests queries by ULID
- [ ] Tests parallel processing scenarios
- [ ] Tests data migration integrity

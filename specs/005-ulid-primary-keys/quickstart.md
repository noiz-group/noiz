# Quickstart Guide: Implementing ULID Primary Keys

**Date**: 2025-10-21
**Phase**: Phase 5 - SQLite Migration
**Spec**: 005 - ULID Primary Keys

## Overview

Quick reference guide for developers implementing ULID primary key changes in Noiz models. This guide provides practical recipes for the most common tasks.

## Quick Reference

### Common Tasks

1. [Add ULIDMixin to a model](#1-add-ulidmixin-to-a-model)
2. [Declare ULID foreign keys](#2-declare-ulid-foreign-keys)
3. [Generate ULIDs in processing code](#3-generate-ulids-in-processing-code)
4. [Update existing code that references .id](#4-update-existing-code)
5. [Update association tables](#5-update-association-tables)
6. [Common pitfalls and solutions](#6-common-pitfalls)

---

## 1. Add ULIDMixin to a Model

### Recipe: File Model

**Before**:
```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"

    id = db.Column("id", db.BigInteger, primary_key=True)  # ← Remove this
    # ulid field from ULIDMixin                             # ← Remove this comment
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

**After**:
```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"

    # id field from ULIDMixin (ULID as primary key)        # ← Add this comment
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

**Changes**:
1. Remove explicit `id` column declaration
2. Remove old `# ulid field from ULIDMixin` comment
3. Add new `# id field from ULIDMixin (ULID as primary key)` comment

---

### Recipe: Result Model with File FK

**Before**:
```python
class Datachunk(ULIDMixin, db.Model):
    __tablename__ = "datachunk"

    id = db.Column("id", db.BigInteger, primary_key=True)  # ← Remove this
    # ulid field from ULIDMixin                             # ← Remove this comment

    datachunk_file_id = db.Column(
        "datachunk_file_id",
        db.BigInteger,                                      # ← Change to String(26)
        db.ForeignKey("datachunk_file.id"),
        nullable=True,
    )
    file_ulid = db.Column(                                 # ← Remove entire column
        "file_ulid",
        db.String(26),
        db.ForeignKey("datachunk_file.ulid"),
        nullable=True,
    )

    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"), nullable=False)
```

**After**:
```python
class Datachunk(ULIDMixin, db.Model):
    __tablename__ = "datachunk"

    # id field from ULIDMixin (ULID as primary key)        # ← Add this comment

    datachunk_file_id = db.Column(
        "datachunk_file_id",
        db.String(26),                                      # ← Changed to String(26)
        db.ForeignKey("datachunk_file.id"),
        nullable=True,
    )

    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"), nullable=False)
```

**Changes**:
1. Remove explicit `id` column declaration
2. Remove old comment about `ulid` field
3. Add new comment about `id` field from mixin
4. Change `datachunk_file_id` type from `BigInteger` to `String(26)`
5. Remove `file_ulid` column completely (dual FK pattern removed)

---

## 2. Declare ULID Foreign Keys

### Recipe: FK to ULID Model

```python
# Foreign key to a ULID primary key model
datachunk_file_id = db.Column(
    "datachunk_file_id",           # Column name in database
    db.String(26),                 # Type MUST be String(26) for ULID FK
    db.ForeignKey("datachunk_file.id"),  # References target table's id column
    nullable=True,                 # or False, depending on requirements
)

# Relationship (optional but recommended)
file = db.relationship(
    "DatachunkFile",
    foreign_keys=[datachunk_file_id],
    uselist=False,
    lazy="joined",
)
```

### Recipe: Mixed FK Types in One Model

```python
class ProcessedDatachunk(ULIDMixin, db.Model):
    __tablename__ = "processeddatachunk"

    # id field from ULIDMixin (ULID as primary key)

    # FK to non-ULID model (Integer)
    processed_datachunk_params_id = db.Column(
        "processed_datachunk_params_id",
        db.Integer,                                      # Integer for non-ULID FK
        db.ForeignKey("processed_datachunk_params.id"),
        nullable=False,
    )

    # FK to ULID model (String(26))
    datachunk_id = db.Column(
        "datachunk_id",
        db.String(26),                                   # String(26) for ULID FK
        db.ForeignKey("datachunk.id"),
        nullable=False,
    )

    # FK to ULID file model (String(26))
    processed_datachunk_file_id = db.Column(
        "processed_datachunk_file_id",
        db.String(26),                                   # String(26) for ULID FK
        db.ForeignKey("processed_datachunk_file.id"),
        nullable=True,
    )
```

### FK Type Quick Reference

| Target Model Type | PK Type | FK Type to Use |
|------------------|---------|----------------|
| ULID models (Datachunk, etc.) | `String(26)` | `db.String(26)` |
| Timespan | `BigInteger` | `db.BigInteger` |
| Component, Device | `Integer` | `db.Integer` |
| *Params models | `Integer` | `db.Integer` |

---

## 3. Generate ULIDs in Processing Code

### Recipe: Simple Object Creation

**Before**:
```python
from ulid import ULID

file_ulid = ULID()

datachunk_file = DatachunkFile(
    ulid=str(file_ulid),           # ← Change to id=
    filepath=str(filepath)
)
```

**After**:
```python
from ulid import ULID

file_ulid = ULID()

datachunk_file = DatachunkFile(
    id=str(file_ulid),             # ← Changed to id=
    filepath=str(filepath)
)
```

---

### Recipe: Parent-Child with FK

**Before**:
```python
from ulid import ULID

file_ulid = ULID()
datachunk_ulid = ULID()

datachunk_file = DatachunkFile(
    ulid=str(file_ulid),           # ← Change to id=
    filepath=str(filepath)
)

datachunk = Datachunk(
    ulid=str(datachunk_ulid),      # ← Change to id=
    file_ulid=str(file_ulid),      # ← Change to datachunk_file_id=
    component_id=component.id,
    timespan_id=timespan.id,
)
```

**After**:
```python
from ulid import ULID

file_ulid = ULID()
datachunk_ulid = ULID()

datachunk_file = DatachunkFile(
    id=str(file_ulid),                    # ← Changed to id=
    filepath=str(filepath)
)

datachunk = Datachunk(
    id=str(datachunk_ulid),               # ← Changed to id=
    datachunk_file_id=str(file_ulid),     # ← Changed to datachunk_file_id=
    component_id=component.id,
    timespan_id=timespan.id,
)
```

**Key Changes**:
1. `ulid=str(...)` → `id=str(...)`
2. `file_ulid=str(...)` → `datachunk_file_id=str(...)`
3. ULID objects still generated the same way: `ULID()`
4. Still convert to string: `str(ulid)`

---

### Recipe: Complete Example from Processing Code

**File**: `/Users/qsbt/noiz-group/noiz/src/noiz/processing/datachunk.py`

**Before**:
```python
from ulid import ULID

# Generate ULIDs
file_ulid = ULID()
datachunk_ulid = ULID()

# Create file
datachunk_file = DatachunkFile(ulid=str(file_ulid), filepath=str(filepath))
trimmed_st.write(datachunk_file.filepath, format="mseed")

sampling_rate = trimmed_st[0].stats.sampling_rate
npts = trimmed_st[0].stats.npts

# Create datachunk
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

**After**:
```python
from ulid import ULID

# Generate ULIDs (unchanged)
file_ulid = ULID()
datachunk_ulid = ULID()

# Create file (ulid → id)
datachunk_file = DatachunkFile(id=str(file_ulid), filepath=str(filepath))
trimmed_st.write(datachunk_file.filepath, format="mseed")

sampling_rate = trimmed_st[0].stats.sampling_rate
npts = trimmed_st[0].stats.npts

# Create datachunk (ulid → id, file_ulid → datachunk_file_id)
datachunk = Datachunk(
    id=str(datachunk_ulid),                    # Changed
    datachunk_params_id=processing_params.id,
    component_id=component.id,
    timespan_id=timespan.id,
    sampling_rate=sampling_rate,
    npts=npts,
    file=datachunk_file,
    datachunk_file_id=str(file_ulid),          # Changed
    padded_npts=padded_npts,
    device_id=component.device_id,
)
```

---

## 4. Update Existing Code

### Recipe: Query by ID

**Before** (still works, no change needed):
```python
# Queries work the same way
datachunk = Datachunk.query.filter_by(id=ulid_string).first()
datachunk = Datachunk.query.get(ulid_string)
```

**After** (no change):
```python
# Queries work the same way
datachunk = Datachunk.query.filter_by(id=ulid_string).first()
datachunk = Datachunk.query.get(ulid_string)
```

**Note**: Query API doesn't change - `id` is still `id`, just the type changed from Integer to String.

---

### Recipe: Access Related Object via FK

**Before**:
```python
# Access via relationship (no change)
datachunk = Datachunk.query.first()
filepath = datachunk.file.filepath

# Access FK value (type changed but usage same)
file_id = datachunk.datachunk_file_id  # Now returns ULID string instead of integer
```

**After** (no change):
```python
# Access via relationship (no change)
datachunk = Datachunk.query.first()
filepath = datachunk.file.filepath

# Access FK value (now ULID string)
file_id = datachunk.datachunk_file_id  # Returns "01ARZ3NDEKTSV4RRFFQ69G5FAV" instead of 123
```

**Note**: Most code that accesses relationships doesn't need changes. Only code that directly compares ID values might need updates.

---

### Recipe: Update Code that References old .ulid

**Before**:
```python
# Old code that accessed .ulid field
obj_ulid = datachunk.ulid  # No longer exists!
query = Datachunk.query.filter_by(ulid=ulid_string)  # No longer exists!
```

**After**:
```python
# New code that accesses .id field
obj_ulid = datachunk.id  # Use .id instead
query = Datachunk.query.filter_by(id=ulid_string)  # Use id= instead
```

**Search Command**:
```bash
# Find all references to .ulid in processing code
grep -r "\.ulid" src/noiz/processing/
grep -r "ulid=" src/noiz/processing/
grep -r "file_ulid=" src/noiz/processing/
```

---

## 5. Update Association Tables

### Recipe: ULID-to-ULID Association

**Before**:
```python
association_table = db.Table(
    "stacking_association",
    db.metadata,
    db.Column("crosscorrelation_cartesian_id", db.BigInteger, db.ForeignKey("crosscorrelation_cartesian.id")),
    db.Column("ccfstack_id", db.BigInteger, db.ForeignKey("ccfstack.id")),
)
```

**After**:
```python
association_table = db.Table(
    "stacking_association",
    db.metadata,
    db.Column("crosscorrelation_cartesian_id", db.String(26), db.ForeignKey("crosscorrelation_cartesian.id")),
    db.Column("ccfstack_id", db.String(26), db.ForeignKey("ccfstack.id")),
)
```

**Changes**: Both columns changed from `db.BigInteger` to `db.String(26)`

---

### Recipe: Mixed Association (ULID and Non-ULID)

**Before**:
```python
association_table = db.Table(
    "association_table_event_confirmation_run_datachunk",
    db.metadata,
    db.Column("event_confirmation_run_id", db.BigInteger, db.ForeignKey("event_confirmation_run.id")),
    db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id")),
)
```

**After**:
```python
association_table = db.Table(
    "association_table_event_confirmation_run_datachunk",
    db.metadata,
    db.Column("event_confirmation_run_id", db.BigInteger, db.ForeignKey("event_confirmation_run.id")),  # Unchanged
    db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id")),                          # Changed
)
```

**Changes**: Only `datachunk_id` changed (references ULID model)

---

## 6. Common Pitfalls

### Pitfall 1: Forgetting to Remove Explicit id Declaration

**Wrong**:
```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"

    id = db.Column("id", db.BigInteger, primary_key=True)  # ✗ WRONG - conflicts with mixin
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

**Error**: SQLAlchemy will complain about duplicate column declaration.

**Fix**: Remove the explicit `id` declaration.

---

### Pitfall 2: Using Wrong FK Type

**Wrong**:
```python
# FK to ULID model using Integer type
datachunk_id = db.Column(
    "datachunk_id",
    db.Integer,                        # ✗ WRONG - should be String(26)
    db.ForeignKey("datachunk.id"),
    nullable=False,
)
```

**Error**: Type mismatch - cannot reference String(26) primary key with Integer foreign key.

**Fix**: Use `db.String(26)` for ULID foreign keys.

---

### Pitfall 3: Using Old Parameter Names

**Wrong**:
```python
from ulid import ULID

file_ulid = ULID()
datachunk_file = DatachunkFile(
    ulid=str(file_ulid),               # ✗ WRONG - no 'ulid' parameter
    filepath=str(filepath)
)
```

**Error**: `TypeError: __init__() got an unexpected keyword argument 'ulid'`

**Fix**: Use `id=` instead of `ulid=`.

---

### Pitfall 4: Using Old FK Column Names

**Wrong**:
```python
datachunk = Datachunk(
    id=str(datachunk_ulid),
    file_ulid=str(file_ulid),          # ✗ WRONG - no 'file_ulid' column
    component_id=component.id,
)
```

**Error**: `TypeError: __init__() got an unexpected keyword argument 'file_ulid'`

**Fix**: Use standard FK name `datachunk_file_id=` instead of `file_ulid=`.

---

### Pitfall 5: Not Converting ULID to String

**Wrong**:
```python
from ulid import ULID

file_ulid = ULID()
datachunk_file = DatachunkFile(
    id=file_ulid,                      # ✗ WRONG - should be str(file_ulid)
    filepath=str(filepath)
)
```

**Error**: Type error - column expects string, got ULID object.

**Fix**: Always convert to string: `id=str(file_ulid)`.

---

### Pitfall 6: Referencing Removed .ulid Field

**Wrong**:
```python
# Trying to access old .ulid field
datachunk_ulid = datachunk.ulid        # ✗ WRONG - .ulid doesn't exist
```

**Error**: `AttributeError: 'Datachunk' object has no attribute 'ulid'`

**Fix**: Use `.id` instead: `datachunk_ulid = datachunk.id`.

---

### Pitfall 7: Forgetting to Update Association Tables

**Wrong**:
```python
# Association table not updated
association_table = db.Table(
    "stacking_association",
    db.metadata,
    db.Column("crosscorrelation_cartesian_id", db.BigInteger, ...),  # ✗ Should be String(26)
    db.Column("ccfstack_id", db.BigInteger, ...),                    # ✗ Should be String(26)
)
```

**Error**: Foreign key type mismatch.

**Fix**: Update association table FK types to match model primary keys.

---

## 7. Testing Checklist

After making changes, verify:

- [ ] Model instantiation works with explicit ULID
  ```python
  obj = Model(id=str(ULID()), ...)
  assert len(obj.id) == 26
  ```

- [ ] Foreign key relationships work
  ```python
  parent = ParentModel(id=str(ULID()), ...)
  child = ChildModel(id=str(ULID()), parent_id=parent.id, ...)
  assert child.parent_id == parent.id
  ```

- [ ] Queries by ID work
  ```python
  db.session.add(obj)
  db.session.commit()
  found = Model.query.filter_by(id=obj.id).first()
  assert found is not None
  ```

- [ ] Relationship traversal works
  ```python
  child = ChildModel.query.first()
  parent = child.parent
  assert parent is not None
  ```

- [ ] Parallel processing scenarios work
  ```python
  # Create objects without database
  parent = ParentModel(id=str(ULID()), ...)
  child = ChildModel(id=str(ULID()), parent_id=parent.id, parent=parent)
  # Can reference before commit
  assert child.parent is parent
  ```

---

## 8. Migration Checklist

For each model being migrated:

1. **Model Definition**:
   - [ ] Remove explicit `id` column declaration
   - [ ] Update FK column types (Integer/BigInteger → String(26) for ULID FKs)
   - [ ] Remove dual FK columns (`file_ulid`)
   - [ ] Update comments

2. **Processing Code**:
   - [ ] Find all object creation: `grep -r "ulid=str" src/noiz/`
   - [ ] Change `ulid=str(...)` to `id=str(...)`
   - [ ] Change `file_ulid=str(...)` to `<table>_file_id=str(...)`
   - [ ] Update any `.ulid` attribute access to `.id`

3. **Query Code**:
   - [ ] Find all queries: `grep -r "filter.*ulid" src/noiz/`
   - [ ] Change `filter_by(ulid=...)` to `filter_by(id=...)`
   - [ ] Update any FK queries to use new FK names

4. **Association Tables** (if applicable):
   - [ ] Update FK column types in association tables
   - [ ] Test many-to-many relationships

5. **Database Migration**:
   - [ ] Create Alembic migration
   - [ ] Test migration on sample database
   - [ ] Verify data preservation
   - [ ] Test rollback

6. **Testing**:
   - [ ] Run unit tests
   - [ ] Run integration tests
   - [ ] Test parallel processing
   - [ ] Verify no regressions

---

## 9. Quick Command Reference

### Find Files to Update

```bash
# Find all references to ULID in models
grep -r "ULIDMixin" src/noiz/models/

# Find all ULID generation in processing code
grep -r "ulid=str" src/noiz/processing/
grep -r "file_ulid=" src/noiz/

# Find all .ulid attribute access
grep -r "\.ulid" src/noiz/

# Find all association tables
grep -r "db\.Table" src/noiz/models/
```

### Create Migration

```bash
# Generate migration stub
uv run flask db revision -m "migrate_datachunk_to_ulid_pk"

# Edit migration file
# ... implement upgrade() and downgrade() ...

# Test migration
uv run flask db upgrade
uv run flask db downgrade
```

### Run Tests

```bash
# Run all tests
just unit_tests

# Run specific test file
uv run pytest tests/models/test_datachunk.py

# Run with verbose output
uv run pytest tests/models/test_datachunk.py -v
```

---

## 10. Getting Help

If you encounter issues:

1. **Check the contracts document**: `/Users/qsbt/noiz-group/noiz/specs/005-ulid-primary-keys/contracts/model_contracts.md`
2. **Review research document**: `/Users/qsbt/noiz-group/noiz/specs/005-ulid-primary-keys/research.md`
3. **Look at data model doc**: `/Users/qsbt/noiz-group/noiz/specs/005-ulid-primary-keys/data-model.md`
4. **Check existing migrations**: `/Users/qsbt/noiz-group/noiz/migrations/versions/`
5. **Review existing ULID usage**: `src/noiz/processing/datachunk.py` (reference implementation)

---

**End of Quickstart Guide**

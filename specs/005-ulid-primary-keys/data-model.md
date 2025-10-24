# Data Model Design: ULID Primary Keys Migration

**Date**: 2025-10-21
**Phase**: Phase 5 - SQLite Migration
**Spec**: 005 - ULID Primary Keys

## Table of Contents

1. [Model Inventory](#1-model-inventory)
2. [Dependency Graph](#2-dependency-graph)
3. [ULIDMixin Modification](#3-ulidmixin-modification)
4. [Model Changes](#4-model-changes)
5. [Foreign Key Mapping](#5-foreign-key-mapping)
6. [Association Table Updates](#6-association-table-updates)
7. [Migration Sequence](#7-migration-sequence)

---

## 1. Model Inventory

### 1.1 Models with ULIDMixin (13 models)

These models currently use the ULIDMixin and will be migrated to use ULID as primary key:

| # | Model Name | File | Current PK | Has ULID | Has Dual FKs |
|---|------------|------|------------|----------|--------------|
| 1 | `DatachunkFile` | `datachunk.py` | `BigInteger` | Yes | No |
| 2 | `Datachunk` | `datachunk.py` | `BigInteger` | Yes | Yes (file_ulid) |
| 3 | `ProcessedDatachunkFile` | `datachunk.py` | `BigInteger` | Yes | No |
| 4 | `ProcessedDatachunk` | `datachunk.py` | `BigInteger` | Yes | Yes (file_ulid) |
| 5 | `CrosscorrelationCartesianFile` | `crosscorrelation.py` | `BigInteger` | Yes | No |
| 6 | `CrosscorrelationCartesian` | `crosscorrelation.py` | `BigInteger` | Yes | Yes (file_ulid) |
| 7 | `CrosscorrelationCylindricalFile` | `crosscorrelation.py` | `BigInteger` | Yes | No |
| 8 | `CrosscorrelationCylindrical` | `crosscorrelation.py` | `BigInteger` | Yes | Yes (file_ulid) |
| 9 | `BeamformingFile` | `beamforming.py` | `BigInteger` (via FileModelMixin) | Yes | No |
| 10 | `BeamformingResult` | `beamforming.py` | `Integer` | Yes | Yes (file_ulid) |
| 11 | `PPSDFile` | `ppsd.py` | `BigInteger` (via FileModelMixin) | Yes | No |
| 12 | `PPSDResult` | `ppsd.py` | `Integer` | Yes | Yes (file_ulid) |
| 13 | `CCFStack` | `stacking.py` | `BigInteger` | Yes | No |

### 1.2 Parent Models Referenced by ULID Models (5 models)

These models do NOT use ULIDMixin but are referenced by models that do:

| Model Name | File | Current PK | Referenced By |
|------------|------|------------|---------------|
| `Component` | `component.py` | `Integer` | Datachunk, PPSDResult |
| `Timespan` | `timespan.py` | `BigInteger` | Datachunk, CrosscorrelationCartesian, CrosscorrelationCylindrical, BeamformingResult, PPSDResult |
| `Device` | `component.py` | `Integer` | Datachunk |
| `ComponentPairCartesian` | `component_pair.py` | `Integer` | CrosscorrelationCartesian, CCFStack |
| `ComponentPairCylindrical` | `component_pair.py` | `Integer` | CrosscorrelationCylindrical |

**Note**: These parent models will NOT be migrated to ULID primary keys. They will retain their integer primary keys.

### 1.3 Configuration Models (NOT Migrated)

These configuration/params models are referenced by ULID models but will NOT be migrated:

- `DatachunkParams`
- `ProcessedDatachunkParams`
- `CrosscorrelationCartesianParams`
- `CrosscorrelationCylindricalParams`
- `BeamformingParams`
- `PPSDParams`
- `QCOneConfig`
- `StackingSchema`

---

## 2. Dependency Graph

### 2.1 Parent-Child Relationships

```
Parent Models (No ULID):
├── Component (Integer PK)
├── Timespan (BigInteger PK)
├── Device (Integer PK)
├── ComponentPairCartesian (Integer PK)
├── ComponentPairCylindrical (Integer PK)
└── *Params models (Integer PK)

ULID File Models (No dependencies on other ULID models):
├── DatachunkFile (ULID → id)
├── ProcessedDatachunkFile (ULID → id)
├── CrosscorrelationCartesianFile (ULID → id)
├── CrosscorrelationCylindricalFile (ULID → id)
├── BeamformingFile (ULID → id)
└── PPSDFile (ULID → id)

ULID Result Models (Depend on File models and Parent models):
├── Datachunk
│   ├── → DatachunkFile.id (ULID FK)
│   ├── → Component.id (Integer FK)
│   ├── → Timespan.id (BigInteger FK)
│   ├── → DatachunkParams.id (Integer FK)
│   └── → Device.id (Integer FK)
│
├── ProcessedDatachunk
│   ├── → ProcessedDatachunkFile.id (ULID FK)
│   ├── → Datachunk.id (ULID FK)
│   └── → ProcessedDatachunkParams.id (Integer FK)
│
├── CrosscorrelationCartesian
│   ├── → CrosscorrelationCartesianFile.id (ULID FK)
│   ├── → ComponentPairCartesian.id (Integer FK)
│   ├── → Timespan.id (BigInteger FK)
│   └── → CrosscorrelationCartesianParams.id (Integer FK)
│
├── CrosscorrelationCylindrical
│   ├── → CrosscorrelationCylindricalFile.id (ULID FK)
│   ├── → ComponentPairCylindrical.id (Integer FK)
│   ├── → Timespan.id (BigInteger FK)
│   ├── → CrosscorrelationCartesian.id (ULID FK) [4 FKs]
│   └── → CrosscorrelationCylindricalParams.id (Integer FK)
│
├── BeamformingResult
│   ├── → BeamformingFile.id (ULID FK)
│   ├── → Timespan.id (Integer FK - Note: inconsistent type!)
│   └── → BeamformingParams.id (Integer FK)
│
├── PPSDResult
│   ├── → PPSDFile.id (ULID FK)
│   ├── → Timespan.id (Integer FK - Note: inconsistent type!)
│   ├── → Datachunk.id (ULID FK)
│   └── → PPSDParams.id (Integer FK)
│
└── CCFStack
    ├── → ComponentPairCartesian.id (Integer FK)
    ├── → StackingTimespan.id (BigInteger FK)
    └── → StackingSchema.id (Integer FK)
```

### 2.2 Foreign Key Type Inconsistencies

**IMPORTANT**: The following type inconsistencies exist in the current schema and need to be addressed:

1. **BeamformingResult.timespan_id**: Declared as `Integer` but Timespan.id is `BigInteger`
   - File: `beamforming.py:99`
   - Current: `db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)`
   - Should be: `db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)`

2. **PPSDResult.timespan_id**: Declared as `Integer` but Timespan.id is `BigInteger`
   - File: `ppsd.py:38`
   - Current: `db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)`
   - Should be: `db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)`

3. **PPSDResult.datachunk_id**: Declared as `Integer` but Datachunk.id will be `String(26)` ULID
   - File: `ppsd.py:39`
   - Current: `db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"), nullable=False)`
   - Will be: `db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id"), nullable=False)`

4. **ProcessedDatachunk.datachunk_id**: Declared as `Integer` but Datachunk.id will be `String(26)` ULID
   - File: `datachunk.py:146`
   - Current: `db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"), nullable=False)`
   - Will be: `db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id"), nullable=False)`

5. **CrosscorrelationCylindrical.crosscorrelation_cartesian_*_id**: Declared as `Integer` but CrosscorrelationCartesian.id will be `String(26)` ULID
   - File: `crosscorrelation.py:130-161`
   - Current: `db.Column("crosscorrelation_cartesian_1_id", db.Integer, db.ForeignKey("crosscorrelation_cartesian.id"), nullable=True)`
   - Will be: `db.Column("crosscorrelation_cartesian_1_id", db.String(26), db.ForeignKey("crosscorrelation_cartesian.id"), nullable=True)`
   - Same for `crosscorrelation_cartesian_2_id`, `_3_id`, `_4_id`

### 2.3 Migration Order

Based on dependencies, the migration order must be:

**Phase 1: File Models (No dependencies)**
1. DatachunkFile
2. ProcessedDatachunkFile
3. CrosscorrelationCartesianFile
4. CrosscorrelationCylindricalFile
5. BeamformingFile
6. PPSDFile

**Phase 2: First-Level Result Models**
7. Datachunk (depends on DatachunkFile)
8. CrosscorrelationCartesian (depends on CrosscorrelationCartesianFile)
9. BeamformingResult (depends on BeamformingFile)
10. CCFStack (no ULID dependencies)

**Phase 3: Second-Level Result Models**
11. ProcessedDatachunk (depends on Datachunk, ProcessedDatachunkFile)
12. CrosscorrelationCylindrical (depends on CrosscorrelationCylindrical, CrosscorrelationCylindricalFile)
13. PPSDResult (depends on Datachunk, PPSDFile)

---

## 3. ULIDMixin Modification

### 3.1 Current Implementation

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

### 3.2 Proposed Implementation

**Change**: Rename the field from `ulid` to `id` and make it a primary key.

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

### 3.3 Impact on Models

**Before**: Models using ULIDMixin had to explicitly declare `id` column:
```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"
    id = db.Column("id", db.BigInteger, primary_key=True)  # Explicit declaration
    # ulid field from ULIDMixin
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

**After**: Models using ULIDMixin inherit `id` as ULID primary key:
```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"
    # id field from ULIDMixin (ULID as primary key)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

---

## 4. Model Changes

### 4.1 DatachunkFile

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/datachunk.py` (lines 15-20)

#### Before
```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

#### After
```python
class DatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "datachunk_file"

    # id field from ULIDMixin (ULID as primary key)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

#### Changes
- Remove explicit `id` column declaration (inherited from ULIDMixin)
- Remove comment about `ulid` field (now it's the `id` field)

---

### 4.2 Datachunk

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/datachunk.py` (lines 23-74)

#### Before
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

    device = db.relationship("Device", foreign_keys=[device_id], uselist=False, lazy="joined")
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id], back_populates="datachunks")
    component = db.relationship("Component", foreign_keys=[component_id])
    params = db.relationship("DatachunkParams", uselist=False, foreign_keys=[datachunk_params_id])
    stats = db.relationship("DatachunkStats", uselist=False, back_populates="datachunk")
    qcones = db.relationship("QCOneResults", uselist=True, back_populates="datachunk")
    processed_datachunks = db.relationship("ProcessedDatachunk")

    file = db.relationship(
        "DatachunkFile",
        foreign_keys=[datachunk_file_id],
        uselist=False,
        lazy="joined",
    )
```

#### After
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

    # id field from ULIDMixin (ULID as primary key)
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
        db.String(26),
        db.ForeignKey("datachunk_file.id"),
        nullable=True,
    )
    device_id = db.Column("device_id", db.Integer, db.ForeignKey("device.id"), nullable=True)

    device = db.relationship("Device", foreign_keys=[device_id], uselist=False, lazy="joined")
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id], back_populates="datachunks")
    component = db.relationship("Component", foreign_keys=[component_id])
    params = db.relationship("DatachunkParams", uselist=False, foreign_keys=[datachunk_params_id])
    stats = db.relationship("DatachunkStats", uselist=False, back_populates="datachunk")
    qcones = db.relationship("QCOneResults", uselist=True, back_populates="datachunk")
    processed_datachunks = db.relationship("ProcessedDatachunk")

    file = db.relationship(
        "DatachunkFile",
        foreign_keys=[datachunk_file_id],
        uselist=False,
        lazy="joined",
    )
```

#### Changes
- Remove explicit `id` column declaration (inherited from ULIDMixin)
- Remove comment about `ulid` field
- Change `datachunk_file_id` type from `BigInteger` to `String(26)`
- **Remove** `file_ulid` column (consolidated into `datachunk_file_id`)

---

### 4.3 ProcessedDatachunkFile

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/datachunk.py` (lines 181-186)

#### Before
```python
class ProcessedDatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "processed_datachunk_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

#### After
```python
class ProcessedDatachunkFile(ULIDMixin, db.Model):
    __tablename__ = "processed_datachunk_file"

    # id field from ULIDMixin (ULID as primary key)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field

---

### 4.4 ProcessedDatachunk

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/datachunk.py` (lines 128-170)

#### Before
```python
class ProcessedDatachunk(ULIDMixin, db.Model):
    __tablename__ = "processeddatachunk"
    __table_args__ = (
        db.UniqueConstraint(
            "datachunk_id",
            "processed_datachunk_params_id",
            name="unique_processing_per_datachunk_per_config",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    processed_datachunk_params_id = db.Column(
        "processed_datachunk_params_id",
        db.Integer,
        db.ForeignKey("processed_datachunk_params.id"),
        nullable=False,
    )
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"), nullable=False)
    processed_datachunk_file_id = db.Column(
        "processed_datachunk_file_id",
        db.BigInteger,
        db.ForeignKey("processed_datachunk_file.id"),
        nullable=True,
    )
    file_ulid = db.Column(
        "file_ulid",
        db.String(26),
        db.ForeignKey("processed_datachunk_file.ulid"),
        nullable=True,
    )

    datachunk = db.relationship("Datachunk", foreign_keys=[datachunk_id], back_populates="processed_datachunks")
    datachunk_processing_config = db.relationship(
        "ProcessedDatachunkParams",
        foreign_keys=[processed_datachunk_params_id],
    )
    file = db.relationship(
        "ProcessedDatachunkFile",
        foreign_keys=[processed_datachunk_file_id],
        uselist=False,
        lazy="joined",
    )
```

#### After
```python
class ProcessedDatachunk(ULIDMixin, db.Model):
    __tablename__ = "processeddatachunk"
    __table_args__ = (
        db.UniqueConstraint(
            "datachunk_id",
            "processed_datachunk_params_id",
            name="unique_processing_per_datachunk_per_config",
        ),
    )

    # id field from ULIDMixin (ULID as primary key)
    processed_datachunk_params_id = db.Column(
        "processed_datachunk_params_id",
        db.Integer,
        db.ForeignKey("processed_datachunk_params.id"),
        nullable=False,
    )
    datachunk_id = db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id"), nullable=False)
    processed_datachunk_file_id = db.Column(
        "processed_datachunk_file_id",
        db.String(26),
        db.ForeignKey("processed_datachunk_file.id"),
        nullable=True,
    )

    datachunk = db.relationship("Datachunk", foreign_keys=[datachunk_id], back_populates="processed_datachunks")
    datachunk_processing_config = db.relationship(
        "ProcessedDatachunkParams",
        foreign_keys=[processed_datachunk_params_id],
    )
    file = db.relationship(
        "ProcessedDatachunkFile",
        foreign_keys=[processed_datachunk_file_id],
        uselist=False,
        lazy="joined",
    )
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field
- Change `datachunk_id` type from `Integer` to `String(26)` (ULID FK)
- Change `processed_datachunk_file_id` type from `BigInteger` to `String(26)`
- **Remove** `file_ulid` column (consolidated into `processed_datachunk_file_id`)

---

### 4.5 CrosscorrelationCartesianFile

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/crosscorrelation.py` (lines 14-18)

#### Before
```python
class CrosscorrelationCartesianFile(ULIDMixin, db.Model):
    __tablename__ = "crosscorrelation_cartesian_file"
    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

#### After
```python
class CrosscorrelationCartesianFile(ULIDMixin, db.Model):
    __tablename__ = "crosscorrelation_cartesian_file"
    # id field from ULIDMixin (ULID as primary key)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field

---

### 4.6 CrosscorrelationCartesian

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/crosscorrelation.py` (lines 21-75)

#### Before
```python
class CrosscorrelationCartesian(ULIDMixin, db.Model):
    __tablename__ = "crosscorrelation_cartesian"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_id",
            "crosscorrelation_cartesian_params_id",
            name="unique_ccfn_per_timespan_per_componentpair_per_config",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    componentpair_id = db.Column(
        "componentpair_id",
        db.Integer,
        db.ForeignKey("componentpair_cartesian.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    crosscorrelation_cartesian_params_id = db.Column(
        "crosscorrelation_cartesian_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian_params.id"),
        nullable=False,
    )

    crosscorrelation_cartesian_file_id = db.Column(
        "crosscorrelation_cartesian_file_id",
        db.BigInteger,
        db.ForeignKey("crosscorrelation_cartesian_file.id"),
        nullable=True,
    )
    file_ulid = db.Column(
        "file_ulid",
        db.String(26),
        db.ForeignKey("crosscorrelation_cartesian_file.ulid"),
        nullable=True,
    )

    componentpair_cartesian = db.relationship("ComponentPairCartesian", foreign_keys=[componentpair_id], lazy="joined")
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    crosscorrelation_cartesian_params = db.relationship(
        "CrosscorrelationCartesianParams", foreign_keys=[crosscorrelation_cartesian_params_id]
    )
    file = db.relationship(
        "CrosscorrelationCartesianFile",
        foreign_keys=[crosscorrelation_cartesian_file_id],
        uselist=False,
        lazy="joined",
    )
    stacks = db.relationship("CCFStack", secondary=ccf_ccfstack_association_table, back_populates="ccfs")
```

#### After
```python
class CrosscorrelationCartesian(ULIDMixin, db.Model):
    __tablename__ = "crosscorrelation_cartesian"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_id",
            "crosscorrelation_cartesian_params_id",
            name="unique_ccfn_per_timespan_per_componentpair_per_config",
        ),
    )

    # id field from ULIDMixin (ULID as primary key)
    componentpair_id = db.Column(
        "componentpair_id",
        db.Integer,
        db.ForeignKey("componentpair_cartesian.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    crosscorrelation_cartesian_params_id = db.Column(
        "crosscorrelation_cartesian_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian_params.id"),
        nullable=False,
    )

    crosscorrelation_cartesian_file_id = db.Column(
        "crosscorrelation_cartesian_file_id",
        db.String(26),
        db.ForeignKey("crosscorrelation_cartesian_file.id"),
        nullable=True,
    )

    componentpair_cartesian = db.relationship("ComponentPairCartesian", foreign_keys=[componentpair_id], lazy="joined")
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    crosscorrelation_cartesian_params = db.relationship(
        "CrosscorrelationCartesianParams", foreign_keys=[crosscorrelation_cartesian_params_id]
    )
    file = db.relationship(
        "CrosscorrelationCartesianFile",
        foreign_keys=[crosscorrelation_cartesian_file_id],
        uselist=False,
        lazy="joined",
    )
    stacks = db.relationship("CCFStack", secondary=ccf_ccfstack_association_table, back_populates="ccfs")
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field
- Change `crosscorrelation_cartesian_file_id` type from `BigInteger` to `String(26)`
- **Remove** `file_ulid` column (consolidated into `crosscorrelation_cartesian_file_id`)

---

### 4.7 CrosscorrelationCylindricalFile

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/crosscorrelation.py` (lines 102-107)

#### Before
```python
class CrosscorrelationCylindricalFile(ULIDMixin, db.Model):
    __tablename__ = "crosscorrelation_cylindrical_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

#### After
```python
class CrosscorrelationCylindricalFile(ULIDMixin, db.Model):
    __tablename__ = "crosscorrelation_cylindrical_file"

    # id field from ULIDMixin (ULID as primary key)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field

---

### 4.8 CrosscorrelationCylindrical

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/crosscorrelation.py` (lines 110-217)

#### Before
```python
class CrosscorrelationCylindrical(ULIDMixin, db.Model):
    __tablename__ = "crosscorrelation_cylindrical"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_cylindrical_id",
            "crosscorrelation_cylindrical_params_id",
            name="unique_ccfcylindrical_per_timespan_cylindrical_per_config",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    componentpair_cylindrical_id = db.Column(
        "componentpair_cylindrical_id",
        db.Integer,
        db.ForeignKey("componentpair_cylindrical.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    crosscorrelation_cartesian_1_id = db.Column(
        "crosscorrelation_cartesian_1_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_1_code_pair = db.Column(
        "crosscorrelation_cartesian_1_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_2_id = db.Column(
        "crosscorrelation_cartesian_2_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_2_code_pair = db.Column(
        "crosscorrelation_cartesian_2_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_3_id = db.Column(
        "crosscorrelation_cartesian_3_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_3_code_pair = db.Column(
        "crosscorrelation_cartesian_3_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_4_id = db.Column(
        "crosscorrelation_cartesian_4_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_4_code_pair = db.Column(
        "crosscorrelation_cartesian_4_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cylindrical_params_id = db.Column(
        "crosscorrelation_cylindrical_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cylindrical_params.id"),
        nullable=False,
    )
    crosscorrelation_cylindrical_file_id = db.Column(
        "crosscorrelation_cylindrical_file_id",
        db.BigInteger,
        db.ForeignKey("crosscorrelation_cylindrical_file.id"),
        nullable=True,
    )
    file_ulid = db.Column(
        "file_ulid",
        db.String(26),
        db.ForeignKey("crosscorrelation_cylindrical_file.ulid"),
        nullable=True,
    )

    # ... relationships ...
```

#### After
```python
class CrosscorrelationCylindrical(ULIDMixin, db.Model):
    __tablename__ = "crosscorrelation_cylindrical"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_cylindrical_id",
            "crosscorrelation_cylindrical_params_id",
            name="unique_ccfcylindrical_per_timespan_cylindrical_per_config",
        ),
    )

    # id field from ULIDMixin (ULID as primary key)
    componentpair_cylindrical_id = db.Column(
        "componentpair_cylindrical_id",
        db.Integer,
        db.ForeignKey("componentpair_cylindrical.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    crosscorrelation_cartesian_1_id = db.Column(
        "crosscorrelation_cartesian_1_id",
        db.String(26),
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_1_code_pair = db.Column(
        "crosscorrelation_cartesian_1_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_2_id = db.Column(
        "crosscorrelation_cartesian_2_id",
        db.String(26),
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_2_code_pair = db.Column(
        "crosscorrelation_cartesian_2_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_3_id = db.Column(
        "crosscorrelation_cartesian_3_id",
        db.String(26),
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_3_code_pair = db.Column(
        "crosscorrelation_cartesian_3_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_4_id = db.Column(
        "crosscorrelation_cartesian_4_id",
        db.String(26),
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_4_code_pair = db.Column(
        "crosscorrelation_cartesian_4_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cylindrical_params_id = db.Column(
        "crosscorrelation_cylindrical_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cylindrical_params.id"),
        nullable=False,
    )
    crosscorrelation_cylindrical_file_id = db.Column(
        "crosscorrelation_cylindrical_file_id",
        db.String(26),
        db.ForeignKey("crosscorrelation_cylindrical_file.id"),
        nullable=True,
    )

    # ... relationships ...
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field
- Change `crosscorrelation_cartesian_1_id` type from `Integer` to `String(26)` (ULID FK)
- Change `crosscorrelation_cartesian_2_id` type from `Integer` to `String(26)` (ULID FK)
- Change `crosscorrelation_cartesian_3_id` type from `Integer` to `String(26)` (ULID FK)
- Change `crosscorrelation_cartesian_4_id` type from `Integer` to `String(26)` (ULID FK)
- Change `crosscorrelation_cylindrical_file_id` type from `BigInteger` to `String(26)`
- **Remove** `file_ulid` column (consolidated into `crosscorrelation_cylindrical_file_id`)

---

### 4.9 BeamformingFile

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/beamforming.py` (lines 24-34)

**Note**: BeamformingFile inherits from both `ULIDMixin` and `FileModelMixin`. FileModelMixin currently declares `id` explicitly.

#### Before
```python
class BeamformingFile(ULIDMixin, FileModelMixin):
    __tablename__ = "beamforming_file"

    # ulid field from ULIDMixin
    # id field from FileModelMixin (BigInteger)
    _file_model_type: str = "beamforming"
    _filename_extension: str = "npz"

    def find_empty_filepath(self, ts: Timespan, params: BeamformingParams) -> Path:
        """filldocs"""
        self._filepath = self._find_empty_filepath(params=params, ts=ts, cmp=None)
        return self.filepath
```

**FileModelMixin** (`mixins.py:44-47`):
```python
class FileModelMixin(db.Model):
    __abstract__ = True
    id: int = db.Column("id", db.BigInteger, primary_key=True)
    _filepath: Path = db.Column("filepath", PathInDB, nullable=False)
```

#### After
```python
class BeamformingFile(ULIDMixin, FileModelMixin):
    __tablename__ = "beamforming_file"

    # id field from ULIDMixin (ULID as primary key)
    # _filepath field from FileModelMixin
    _file_model_type: str = "beamforming"
    _filename_extension: str = "npz"

    def find_empty_filepath(self, ts: Timespan, params: BeamformingParams) -> Path:
        """filldocs"""
        self._filepath = self._find_empty_filepath(params=params, ts=ts, cmp=None)
        return self.filepath
```

**FileModelMixin** (updated):
```python
class FileModelMixin(db.Model):
    __abstract__ = True
    # id field should come from ULIDMixin (remove explicit declaration)
    _filepath: Path = db.Column("filepath", PathInDB, nullable=False)
```

#### Changes
- Update `FileModelMixin` to remove explicit `id` declaration (will be inherited from ULIDMixin)
- Update comment to clarify `id` comes from ULIDMixin

---

### 4.10 BeamformingResult

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/beamforming.py` (lines 86-158)

#### Before
```python
class BeamformingResult(ULIDMixin, db.Model):
    __tablename__ = "beamforming_result"
    __table_args__ = (
        db.UniqueConstraint("timespan_id", "beamforming_params_id", name="unique_beam_per_config_per_timespan"),
    )
    id = db.Column("id", db.Integer, primary_key=True)
    # ulid field from ULIDMixin
    beamforming_params_id = db.Column(
        "beamforming_params_id",
        db.Integer,
        db.ForeignKey("beamforming_params.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)

    used_component_count = db.Column("used_component_count", db.Integer, nullable=False)

    beamforming_file_id = db.Column(
        "beamforming_file_id",
        db.BigInteger,
        db.ForeignKey("beamforming_file.id"),
        nullable=True,
    )
    file_ulid = db.Column(
        "file_ulid",
        db.String(26),
        db.ForeignKey("beamforming_file.ulid"),
        nullable=True,
    )

    # ... relationships ...
```

#### After
```python
class BeamformingResult(ULIDMixin, db.Model):
    __tablename__ = "beamforming_result"
    __table_args__ = (
        db.UniqueConstraint("timespan_id", "beamforming_params_id", name="unique_beam_per_config_per_timespan"),
    )
    # id field from ULIDMixin (ULID as primary key)
    beamforming_params_id = db.Column(
        "beamforming_params_id",
        db.Integer,
        db.ForeignKey("beamforming_params.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)

    used_component_count = db.Column("used_component_count", db.Integer, nullable=False)

    beamforming_file_id = db.Column(
        "beamforming_file_id",
        db.String(26),
        db.ForeignKey("beamforming_file.id"),
        nullable=True,
    )

    # ... relationships ...
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field
- **Fix**: Change `timespan_id` type from `Integer` to `BigInteger` (consistency fix!)
- Change `beamforming_file_id` type from `BigInteger` to `String(26)`
- **Remove** `file_ulid` column (consolidated into `beamforming_file_id`)

---

### 4.11 PPSDFile

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/ppsd.py` (lines 17-27)

#### Before
```python
class PPSDFile(ULIDMixin, FileModelMixin):
    __tablename__ = "ppsd_file"

    # ulid field from ULIDMixin
    # id field from FileModelMixin (BigInteger)
    _file_model_type: str = "psd"
    _filename_extension: str = "npz"

    def find_empty_filepath(self, cmp: Component, ts: Timespan, params: PPSDParams) -> Path:
        """filldocs"""
        self._filepath = self._find_empty_filepath(params=params, ts=ts, cmp=cmp)
        return self.filepath
```

#### After
```python
class PPSDFile(ULIDMixin, FileModelMixin):
    __tablename__ = "ppsd_file"

    # id field from ULIDMixin (ULID as primary key)
    # _filepath field from FileModelMixin
    _file_model_type: str = "psd"
    _filename_extension: str = "npz"

    def find_empty_filepath(self, cmp: Component, ts: Timespan, params: PPSDParams) -> Path:
        """filldocs"""
        self._filepath = self._find_empty_filepath(params=params, ts=ts, cmp=cmp)
        return self.filepath
```

#### Changes
- Update comment to clarify `id` comes from ULIDMixin
- Same FileModelMixin changes as BeamformingFile

---

### 4.12 PPSDResult

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/ppsd.py` (lines 30-79)

#### Before
```python
class PPSDResult(ULIDMixin, db.Model):
    __tablename__ = "ppsd_result"
    __table_args__ = (
        db.UniqueConstraint("datachunk_id", "ppsd_params_id", name="unique_ppsd_per_config_per_datachunk"),
    )
    id = db.Column("id", db.Integer, primary_key=True)
    # ulid field from ULIDMixin
    ppsd_params_id = db.Column("ppsd_params_id", db.Integer, db.ForeignKey("ppsd_params.id"), nullable=False)
    timespan_id = db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"), nullable=False)

    ppsd_file_id = db.Column(
        "ppsd_file_id",
        db.BigInteger,
        db.ForeignKey("ppsd_file.id"),
        nullable=True,
    )
    file_ulid = db.Column(
        "file_ulid",
        db.String(26),
        db.ForeignKey("ppsd_file.ulid"),
        nullable=True,
    )

    # ... relationships ...
```

#### After
```python
class PPSDResult(ULIDMixin, db.Model):
    __tablename__ = "ppsd_result"
    __table_args__ = (
        db.UniqueConstraint("datachunk_id", "ppsd_params_id", name="unique_ppsd_per_config_per_datachunk"),
    )
    # id field from ULIDMixin (ULID as primary key)
    ppsd_params_id = db.Column("ppsd_params_id", db.Integer, db.ForeignKey("ppsd_params.id"), nullable=False)
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    datachunk_id = db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id"), nullable=False)

    ppsd_file_id = db.Column(
        "ppsd_file_id",
        db.String(26),
        db.ForeignKey("ppsd_file.id"),
        nullable=True,
    )

    # ... relationships ...
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field
- **Fix**: Change `timespan_id` type from `Integer` to `BigInteger` (consistency fix!)
- Change `datachunk_id` type from `Integer` to `String(26)` (ULID FK)
- Change `ppsd_file_id` type from `BigInteger` to `String(26)`
- **Remove** `file_ulid` column (consolidated into `ppsd_file_id`)

---

### 4.13 CCFStack

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/stacking.py` (lines 126-167)

#### Before
```python
class CCFStack(ULIDMixin, db.Model):
    __tablename__ = "ccfstack"
    __table_args__ = (
        db.UniqueConstraint(
            "stacking_timespan_id", "stacking_schema_id", "componentpair_id", name="unique_stack_per_pair_per_config"
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    # ulid field from ULIDMixin
    stacking_timespan_id = db.Column(
        "stacking_timespan_id",
        db.BigInteger,
        db.ForeignKey("stacking_timespan.id"),
        nullable=False,
    )
    stacking_schema_id = db.Column(
        "stacking_schema_id",
        db.Integer,
        db.ForeignKey("stacking_schema.id"),
        nullable=False,
    )
    componentpair_id = db.Column(
        "componentpair_id",
        db.Integer,
        db.ForeignKey("componentpair_cartesian.id"),
        nullable=False,
    )
    stack = db.Column("stack", db.JSON, nullable=False)
    no_ccfs = db.Column("no_ccfs", db.Integer, nullable=False)

    # ... relationships ...
```

#### After
```python
class CCFStack(ULIDMixin, db.Model):
    __tablename__ = "ccfstack"
    __table_args__ = (
        db.UniqueConstraint(
            "stacking_timespan_id", "stacking_schema_id", "componentpair_id", name="unique_stack_per_pair_per_config"
        ),
    )

    # id field from ULIDMixin (ULID as primary key)
    stacking_timespan_id = db.Column(
        "stacking_timespan_id",
        db.BigInteger,
        db.ForeignKey("stacking_timespan.id"),
        nullable=False,
    )
    stacking_schema_id = db.Column(
        "stacking_schema_id",
        db.Integer,
        db.ForeignKey("stacking_schema.id"),
        nullable=False,
    )
    componentpair_id = db.Column(
        "componentpair_id",
        db.Integer,
        db.ForeignKey("componentpair_cartesian.id"),
        nullable=False,
    )
    stack = db.Column("stack", db.JSON, nullable=False)
    no_ccfs = db.Column("no_ccfs", db.Integer, nullable=False)

    # ... relationships ...
```

#### Changes
- Remove explicit `id` column declaration
- Remove comment about `ulid` field
- No foreign key type changes (does not reference other ULID models)

---

## 5. Foreign Key Mapping

### 5.1 Complete Foreign Key Inventory

This table shows all foreign key relationships for the 13 ULID models:

| Source Model | Target Model | Current Integer FK | Current ULID FK | Proposed Final FK | FK Type |
|--------------|--------------|-------------------|-----------------|-------------------|---------|
| **Datachunk** | DatachunkFile | `datachunk_file_id` (BigInteger) | `file_ulid` (String(26)) | `datachunk_file_id` (String(26)) | ULID |
| Datachunk | Component | `component_id` (Integer) | - | `component_id` (Integer) | Integer |
| Datachunk | Timespan | `timespan_id` (BigInteger) | - | `timespan_id` (BigInteger) | BigInteger |
| Datachunk | DatachunkParams | `datachunk_params_id` (Integer) | - | `datachunk_params_id` (Integer) | Integer |
| Datachunk | Device | `device_id` (Integer) | - | `device_id` (Integer) | Integer |
| **ProcessedDatachunk** | ProcessedDatachunkFile | `processed_datachunk_file_id` (BigInteger) | `file_ulid` (String(26)) | `processed_datachunk_file_id` (String(26)) | ULID |
| **ProcessedDatachunk** | Datachunk | `datachunk_id` (Integer) | - | `datachunk_id` (String(26)) | ULID |
| ProcessedDatachunk | ProcessedDatachunkParams | `processed_datachunk_params_id` (Integer) | - | `processed_datachunk_params_id` (Integer) | Integer |
| **CrosscorrelationCartesian** | CrosscorrelationCartesianFile | `crosscorrelation_cartesian_file_id` (BigInteger) | `file_ulid` (String(26)) | `crosscorrelation_cartesian_file_id` (String(26)) | ULID |
| CrosscorrelationCartesian | ComponentPairCartesian | `componentpair_id` (Integer) | - | `componentpair_id` (Integer) | Integer |
| CrosscorrelationCartesian | Timespan | `timespan_id` (BigInteger) | - | `timespan_id` (BigInteger) | BigInteger |
| CrosscorrelationCartesian | CrosscorrelationCartesianParams | `crosscorrelation_cartesian_params_id` (Integer) | - | `crosscorrelation_cartesian_params_id` (Integer) | Integer |
| **CrosscorrelationCylindrical** | CrosscorrelationCylindricalFile | `crosscorrelation_cylindrical_file_id` (BigInteger) | `file_ulid` (String(26)) | `crosscorrelation_cylindrical_file_id` (String(26)) | ULID |
| CrosscorrelationCylindrical | ComponentPairCylindrical | `componentpair_cylindrical_id` (Integer) | - | `componentpair_cylindrical_id` (Integer) | Integer |
| CrosscorrelationCylindrical | Timespan | `timespan_id` (BigInteger) | - | `timespan_id` (BigInteger) | BigInteger |
| **CrosscorrelationCylindrical** | CrosscorrelationCartesian | `crosscorrelation_cartesian_1_id` (Integer) | - | `crosscorrelation_cartesian_1_id` (String(26)) | ULID |
| **CrosscorrelationCylindrical** | CrosscorrelationCartesian | `crosscorrelation_cartesian_2_id` (Integer) | - | `crosscorrelation_cartesian_2_id` (String(26)) | ULID |
| **CrosscorrelationCylindrical** | CrosscorrelationCartesian | `crosscorrelation_cartesian_3_id` (Integer) | - | `crosscorrelation_cartesian_3_id` (String(26)) | ULID |
| **CrosscorrelationCylindrical** | CrosscorrelationCartesian | `crosscorrelation_cartesian_4_id` (Integer) | - | `crosscorrelation_cartesian_4_id` (String(26)) | ULID |
| CrosscorrelationCylindrical | CrosscorrelationCylindricalParams | `crosscorrelation_cylindrical_params_id` (Integer) | - | `crosscorrelation_cylindrical_params_id` (Integer) | Integer |
| **BeamformingResult** | BeamformingFile | `beamforming_file_id` (BigInteger) | `file_ulid` (String(26)) | `beamforming_file_id` (String(26)) | ULID |
| BeamformingResult | Timespan | `timespan_id` (Integer) → **FIX TO BigInteger** | - | `timespan_id` (BigInteger) | BigInteger |
| BeamformingResult | BeamformingParams | `beamforming_params_id` (Integer) | - | `beamforming_params_id` (Integer) | Integer |
| **PPSDResult** | PPSDFile | `ppsd_file_id` (BigInteger) | `file_ulid` (String(26)) | `ppsd_file_id` (String(26)) | ULID |
| PPSDResult | Timespan | `timespan_id` (Integer) → **FIX TO BigInteger** | - | `timespan_id` (BigInteger) | BigInteger |
| **PPSDResult** | Datachunk | `datachunk_id` (Integer) | - | `datachunk_id` (String(26)) | ULID |
| PPSDResult | PPSDParams | `ppsd_params_id` (Integer) | - | `ppsd_params_id` (Integer) | Integer |
| CCFStack | StackingTimespan | `stacking_timespan_id` (BigInteger) | - | `stacking_timespan_id` (BigInteger) | BigInteger |
| CCFStack | StackingSchema | `stacking_schema_id` (Integer) | - | `stacking_schema_id` (Integer) | Integer |
| CCFStack | ComponentPairCartesian | `componentpair_id` (Integer) | - | `componentpair_id` (Integer) | Integer |

**Summary**:
- **Total Foreign Keys**: 30
- **ULID Foreign Keys** (will be String(26)): 12
  - 6 dual FKs consolidated (file_ulid removed)
  - 6 new ULID FKs (references to Datachunk, CrosscorrelationCartesian)
- **Integer Foreign Keys** (unchanged): 15
- **BigInteger Foreign Keys** (unchanged): 3
- **Type Fixes Required**: 2 (BeamformingResult.timespan_id, PPSDResult.timespan_id)

---

## 6. Association Table Updates

### 6.1 All Association Tables

| # | Table Name | Source Model | Target Model | Current FK Types | Proposed FK Types |
|---|------------|--------------|--------------|------------------|-------------------|
| 1 | `beamforming_association_datachunks` | BeamformingResult | Datachunk | BigInteger, BigInteger | **String(26), String(26)** |
| 2 | `beamforming_result_association_avg_abspower` | BeamformingResult | BeamformingPeakAverageAbspower | BigInteger, BigInteger | **String(26), BigInteger** |
| 3 | `beamforming_result_association_avg_relpower` | BeamformingResult | BeamformingPeakAverageRelpower | BigInteger, BigInteger | **String(26), BigInteger** |
| 4 | `beamforming_result_association_all_abspower` | BeamformingResult | BeamformingPeakAllAbspower | BigInteger, BigInteger | **String(26), BigInteger** |
| 5 | `beamforming_result_association_all_relpower` | BeamformingResult | BeamformingPeakAllRelpower | BigInteger, BigInteger | **String(26), BigInteger** |
| 6 | `stacking_association` | CrosscorrelationCartesian | CCFStack | BigInteger, BigInteger | **String(26), String(26)** |
| 7 | `association_table_soh_instr` | Component | SohInstrument | BigInteger, BigInteger | BigInteger, BigInteger |
| 8 | `association_table_soh_gps` | Component | SohGps | BigInteger, BigInteger | BigInteger, BigInteger |
| 9 | `association_table_averaged_soh_gps_components` | Component | AveragedSohGps | BigInteger, BigInteger | BigInteger, BigInteger |
| 10 | `association_table_event_confirmation_result_event_detection_result` | EventConfirmationResult | EventDetectionResult | BigInteger, BigInteger | BigInteger, BigInteger |
| 11 | `association_table_event_confirmation_run_datachunk` | EventConfirmationRun | Datachunk | BigInteger, Integer | **BigInteger, String(26)** |

**Summary**:
- **Total Association Tables**: 11
- **Require Updates**: 7 (tables 1-6, 11)
- **No Changes**: 4 (tables 7-10, do not reference ULID models)

### 6.2 Detailed Association Table Changes

#### 6.2.1 beamforming_association_datachunks

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/beamforming.py` (lines 37-43)

**Before**:
```python
association_table_beamforming_results_datachunks = db.Table(
    "beamforming_association_datachunks",
    db.metadata,
    db.Column("datachunk_id", db.BigInteger, db.ForeignKey("datachunk.id")),
    db.Column("beamforming_result_id", db.BigInteger, db.ForeignKey("beamforming_result.id")),
    db.UniqueConstraint("beamforming_result_id", "datachunk_id"),
)
```

**After**:
```python
association_table_beamforming_results_datachunks = db.Table(
    "beamforming_association_datachunks",
    db.metadata,
    db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id")),
    db.Column("beamforming_result_id", db.String(26), db.ForeignKey("beamforming_result.id")),
    db.UniqueConstraint("beamforming_result_id", "datachunk_id"),
)
```

#### 6.2.2 beamforming_result_association_avg_abspower

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/beamforming.py` (lines 46-54)

**Before**:
```python
association_table_beamforming_result_avg_abspower = db.Table(
    "beamforming_result_association_avg_abspower",
    db.metadata,
    db.Column(
        "beamforming_peak_average_abspower_id", db.BigInteger, db.ForeignKey("beamforming_peak_average_abspower.id")
    ),
    db.Column("beamforming_result_id", db.BigInteger, db.ForeignKey("beamforming_result.id")),
    db.UniqueConstraint("beamforming_result_id", "beamforming_peak_average_abspower_id"),
)
```

**After**:
```python
association_table_beamforming_result_avg_abspower = db.Table(
    "beamforming_result_association_avg_abspower",
    db.metadata,
    db.Column(
        "beamforming_peak_average_abspower_id", db.BigInteger, db.ForeignKey("beamforming_peak_average_abspower.id")
    ),
    db.Column("beamforming_result_id", db.String(26), db.ForeignKey("beamforming_result.id")),
    db.UniqueConstraint("beamforming_result_id", "beamforming_peak_average_abspower_id"),
)
```

**Note**: Similar changes apply to:
- `beamforming_result_association_avg_relpower`
- `beamforming_result_association_all_abspower`
- `beamforming_result_association_all_relpower`

#### 6.2.3 stacking_association

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/stacking.py` (lines 118-123)

**Before**:
```python
ccf_ccfstack_association_table = db.Table(
    "stacking_association",
    db.metadata,
    db.Column("crosscorrelation_cartesian_id", db.BigInteger, db.ForeignKey("crosscorrelation_cartesian.id")),
    db.Column("ccfstack_id", db.BigInteger, db.ForeignKey("ccfstack.id")),
)
```

**After**:
```python
ccf_ccfstack_association_table = db.Table(
    "stacking_association",
    db.metadata,
    db.Column("crosscorrelation_cartesian_id", db.String(26), db.ForeignKey("crosscorrelation_cartesian.id")),
    db.Column("ccfstack_id", db.String(26), db.ForeignKey("ccfstack.id")),
)
```

#### 6.2.4 association_table_event_confirmation_run_datachunk

**Location**: `/Users/qsbt/noiz-group/noiz/src/noiz/models/event_detection.py` (lines 76-81)

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
    db.Column("event_confirmation_run_id", db.BigInteger, db.ForeignKey("event_confirmation_run.id")),
    db.Column("datachunk_id", db.String(26), db.ForeignKey("datachunk.id")),
)
```

---

## 7. Migration Sequence

### 7.1 Safe Migration Order

Based on dependency analysis, models must be migrated in this order:

#### Phase 1: File Models (No ULID Dependencies)
1. **DatachunkFile**
   - No dependencies on other ULID models
   - Referenced by: Datachunk

2. **ProcessedDatachunkFile**
   - No dependencies on other ULID models
   - Referenced by: ProcessedDatachunk

3. **CrosscorrelationCartesianFile**
   - No dependencies on other ULID models
   - Referenced by: CrosscorrelationCartesian

4. **CrosscorrelationCylindricalFile**
   - No dependencies on other ULID models
   - Referenced by: CrosscorrelationCylindrical

5. **BeamformingFile**
   - No dependencies on other ULID models
   - Referenced by: BeamformingResult

6. **PPSDFile**
   - No dependencies on other ULID models
   - Referenced by: PPSDResult

#### Phase 2: First-Level Result Models
7. **Datachunk**
   - Depends on: DatachunkFile (File ULID model - already migrated)
   - Referenced by: ProcessedDatachunk, PPSDResult, Association tables

8. **CrosscorrelationCartesian**
   - Depends on: CrosscorrelationCartesianFile (File ULID model - already migrated)
   - Referenced by: CrosscorrelationCylindrical, CCFStack, Association tables

9. **BeamformingResult**
   - Depends on: BeamformingFile (File ULID model - already migrated)
   - Referenced by: Association tables

10. **CCFStack**
    - No ULID dependencies
    - Referenced by: Association tables

#### Phase 3: Second-Level Result Models
11. **ProcessedDatachunk**
    - Depends on: Datachunk (migrated in Phase 2), ProcessedDatachunkFile (migrated in Phase 1)

12. **CrosscorrelationCylindrical**
    - Depends on: CrosscorrelationCartesian (migrated in Phase 2), CrosscorrelationCylindricalFile (migrated in Phase 1)

13. **PPSDResult**
    - Depends on: Datachunk (migrated in Phase 2), PPSDFile (migrated in Phase 1)

### 7.2 Migration Steps Per Model

For each model, the migration process follows these steps:

1. **Model Definition Update**
   - Remove explicit `id` column declaration
   - Update foreign key column types (Integer/BigInteger → String(26) for ULID FKs)
   - Remove dual ULID foreign key columns (file_ulid)
   - Update comments

2. **Database Schema Migration** (Alembic)
   - For SQLite (table recreation required):
     ```python
     # 1. Create new table with ULID primary key and updated FK types
     op.create_table(
         'model_name_new',
         sa.Column('id', sa.String(26), primary_key=True, nullable=False),
         # ... other columns with updated FK types ...
     )

     # 2. Copy data from old table, mapping old columns to new
     op.execute("""
         INSERT INTO model_name_new (id, other_columns...)
         SELECT ulid, other_columns...
         FROM model_name
     """)

     # 3. Drop old table
     op.drop_table('model_name')

     # 4. Rename new table to original name
     op.rename_table('model_name_new', 'model_name')
     ```

3. **Association Table Updates** (if applicable)
   - Update foreign key column types in association tables
   - Same SQLite table recreation pattern

4. **Code Updates** (processing files)
   - Change ULID generation calls from `ulid=str(ulid)` to `id=str(ulid)`
   - Change file FK references from `file_ulid=str(file_ulid)` to `file_id=str(file_ulid)`
   - Update foreign key references for models that now use ULID

### 7.3 Testing Requirements

After each migration phase:

1. **Unit Tests**
   - Test model instantiation with ULID
   - Test foreign key relationships
   - Test queries by ULID

2. **Integration Tests**
   - Test parallel object creation
   - Test parent-child relationships before database commit
   - Test association table relationships

3. **Data Integrity Tests**
   - Verify all foreign key constraints enforced
   - Verify unique constraints work
   - Verify no data loss

---

## 8. Additional Notes

### 8.1 Related Models Not Migrated

The following models also reference ULID models but are NOT themselves migrated to ULID:

- **DatachunkStats**: References Datachunk (will use String(26) FK)
- **QCOneResults**: References Datachunk (will use String(26) FK)
- **QCTwoResults**: References CrosscorrelationCartesian (will use String(26) FK)
- **EventDetectionResult**: References Datachunk (will use String(26) FK)
- **EventConfirmationResult**: References EventConfirmationRun which references Datachunk

These models will need their foreign key types updated but will retain integer primary keys.

### 8.2 Code Generation Pattern Changes

**Current pattern**:
```python
from ulid import ULID

file_ulid = ULID()
datachunk_ulid = ULID()

datachunk_file = DatachunkFile(ulid=str(file_ulid), filepath=str(filepath))
datachunk = Datachunk(
    ulid=str(datachunk_ulid),
    file_ulid=str(file_ulid),
    # ... other fields ...
)
```

**Updated pattern**:
```python
from ulid import ULID

file_ulid = ULID()
datachunk_ulid = ULID()

datachunk_file = DatachunkFile(id=str(file_ulid), filepath=str(filepath))
datachunk = Datachunk(
    id=str(datachunk_ulid),
    datachunk_file_id=str(file_ulid),
    # ... other fields ...
)
```

**Files requiring code updates**:
- `/Users/qsbt/noiz-group/noiz/src/noiz/processing/datachunk.py`
- `/Users/qsbt/noiz-group/noiz/src/noiz/processing/beamforming.py`
- `/Users/qsbt/noiz-group/noiz/src/noiz/api/crosscorrelations.py`
- `/Users/qsbt/noiz-group/noiz/src/noiz/api/stacking.py`
- `/Users/qsbt/noiz-group/noiz/src/noiz/processing/ppsd.py`

---

## Appendix A: Type Mapping Summary

| Current Type | New Type | Usage |
|--------------|----------|-------|
| `BigInteger` (PK) | `String(26)` (PK) | Primary keys for ULID models |
| `BigInteger` (FK to ULID) | `String(26)` (FK) | Foreign keys to ULID models |
| `String(26)` (ULID field) | Removed | Consolidated into `id` |
| `String(26)` (ULID FK) | Removed | Consolidated into standard FK |
| `Integer` (FK to non-ULID) | `Integer` | Unchanged |
| `BigInteger` (FK to non-ULID) | `BigInteger` | Unchanged |

---

## Appendix B: Quick Reference Checklist

- [ ] 13 models to migrate to ULID primary keys
- [ ] 1 mixin to update (ULIDMixin)
- [ ] 1 mixin to fix (FileModelMixin)
- [ ] 6 dual foreign key pairs to consolidate
- [ ] 6 new ULID foreign keys to add (Datachunk refs, CrosscorrelationCartesian refs)
- [ ] 7 association tables to update
- [ ] 2 type inconsistencies to fix (BeamformingResult.timespan_id, PPSDResult.timespan_id)
- [ ] 5 processing files to update for code generation
- [ ] 13 Alembic migrations to create

---

**End of Data Model Design Document**

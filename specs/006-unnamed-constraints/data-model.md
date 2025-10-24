# Data Model: Unnamed Constraints

**Feature**: Remove Named Database Constraints
**Date**: 2025-10-24

## Overview

This document describes the data model changes for transitioning from named to unnamed unique constraints across all SQLAlchemy models, plus the error handling enhancements for constraint violations.

## Current State

### Named Constraint Pattern

```python
class QCOneResults(db.Model):
    __tablename__ = "qcone_results"
    __table_args__ = (
        db.UniqueConstraint("datachunk_id", "qcone_config_id",
                          name="unique_qcone_results_per_config_per_datachunk"),
    )
```

### Named Constraint Upsert

```python
from noiz.database import get_dialect_insert, dialect_agnostic_on_conflict

insert_func = get_dialect_insert()
insert_stmt = insert_func(QCOneResults).values(...)

insert_command = dialect_agnostic_on_conflict(
    insert_stmt,
    constraint_name="unique_qcone_results_per_config_per_datachunk",
    set_={...}
)
```

### Translation Layer

```python
# In database.py
CONSTRAINT_TO_COLUMNS = {
    "unique_qcone_results_per_config_per_datachunk": ["datachunk_id", "qcone_config_id"],
    # ... 18 more mappings
}

def dialect_agnostic_on_conflict(insert_stmt, constraint_name=None, index_elements=None, set_=None):
    dialect_name = db.engine.dialect.name

    if dialect_name == "postgresql":
        return insert_stmt.on_conflict_do_update(constraint=constraint_name, set_=set_)
    elif dialect_name == "sqlite":
        if index_elements is None and constraint_name:
            index_elements = CONSTRAINT_TO_COLUMNS.get(constraint_name)
        return insert_stmt.on_conflict_do_update(index_elements=index_elements, set_=set_)
```

## Target State

### Unnamed Constraint Pattern

```python
class QCOneResults(db.Model):
    __tablename__ = "qcone_results"
    __table_args__ = (
        db.UniqueConstraint("datachunk_id", "qcone_config_id"),  # No name parameter
    )
```

### Unnamed Constraint Upsert

```python
from noiz.database import get_dialect_insert

insert_func = get_dialect_insert()
insert_stmt = insert_func(QCOneResults).values(...)

# Direct use of index_elements - works for both PostgreSQL and SQLite
insert_command = insert_stmt.on_conflict_do_update(
    index_elements=["datachunk_id", "qcone_config_id"],
    set_={...}
)
```

### Simplified Database Module

```python
# In database.py
# CONSTRAINT_TO_COLUMNS - REMOVED
# dialect_agnostic_on_conflict() - REMOVED

def get_dialect_insert():
    """Get the appropriate insert function for the current database dialect."""
    from sqlalchemy import insert

    dialect_name = db.engine.dialect.name

    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        return pg_insert
    elif dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        return sqlite_insert
    else:
        return insert
```

## Constraint Inventory

### Models with Named Constraints (16 to update)

#### 1. Component Models (3 constraints)

**File**: `src/noiz/models/component.py`

```python
# Device
db.UniqueConstraint("network", "station", name="unique_device_per_station")
→ db.UniqueConstraint("network", "station")

# Component
db.UniqueConstraint("network", "station", "component", name="unique_component_per_station")
→ db.UniqueConstraint("network", "station", "component")
```

**File**: `src/noiz/models/component_pair.py`

```python
# ComponentPairCartesian
db.UniqueConstraint("component_a_id", "component_b_id", name="single_component_pair")
→ db.UniqueConstraint("component_a_id", "component_b_id")
```

#### 2. Timespan Models (4 constraints)

**File**: `src/noiz/models/timespan.py`

```python
# Timespan
db.UniqueConstraint("starttime", name="unique_starttime")
→ db.UniqueConstraint("starttime")

db.UniqueConstraint("midtime", name="unique_midtime")
→ db.UniqueConstraint("midtime")

db.UniqueConstraint("endtime", name="unique_endtime")
→ db.UniqueConstraint("endtime")

db.UniqueConstraint("starttime", "midtime", "endtime", name="unique_times")
→ db.UniqueConstraint("starttime", "midtime", "endtime")
```

#### 3. Processing Results Models (6 constraints)

**File**: `src/noiz/models/ppsd.py`

```python
# PPSDResult
db.UniqueConstraint("datachunk_id", "ppsd_params_id", name="unique_ppsd_per_config_per_datachunk")
→ db.UniqueConstraint("datachunk_id", "ppsd_params_id")
```

**File**: `src/noiz/models/beamforming.py`

```python
# BeamformingResult
db.UniqueConstraint("timespan_id", "beamforming_params_id", name="unique_beam_per_config_per_timespan")
→ db.UniqueConstraint("timespan_id", "beamforming_params_id")
```

**File**: `src/noiz/models/qc.py`

```python
# QCOneResults
db.UniqueConstraint("datachunk_id", "qcone_config_id", name="unique_qcone_results_per_config_per_datachunk")
→ db.UniqueConstraint("datachunk_id", "qcone_config_id")

# QCTwoResults
db.UniqueConstraint("crosscorrelation_cartesian_id", "qctwo_config_id",
                   name="unique_qctwo_results_per_config_per_ccf")
→ db.UniqueConstraint("crosscorrelation_cartesian_id", "qctwo_config_id")
```

**File**: `src/noiz/models/crosscorrelation.py`

```python
# CrosscorrelationCartesian
db.UniqueConstraint("timespan_id", "componentpair_id", "crosscorrelation_cartesian_params_id",
                   name="unique_ccfn_per_timespan_per_componentpair_per_config")
→ db.UniqueConstraint("timespan_id", "componentpair_id", "crosscorrelation_cartesian_params_id")

# CrosscorrelationCylindrical
db.UniqueConstraint("timespan_id", "componentpair_cylindrical_id",
                   "crosscorrelation_cylindrical_params_id",
                   name="unique_ccfcylindrical_per_timespan_cylindrical_per_config")
→ db.UniqueConstraint("timespan_id", "componentpair_cylindrical_id",
                     "crosscorrelation_cylindrical_params_id")
```

#### 4. SOH Models (3 constraints)

**File**: `src/noiz/models/soh.py`

```python
# SOHInstrument
db.UniqueConstraint("datetime", "z_component_id", name="unique_timestamp_per_station_in_sohinstrument")
→ db.UniqueConstraint("datetime", "z_component_id")

# SOHGPS
db.UniqueConstraint("datetime", "z_component_id", name="unique_timestamp_per_station_in_sohgps")
→ db.UniqueConstraint("datetime", "z_component_id")

# AveragedSOHGPS
db.UniqueConstraint("timespan_id", "z_component_id", name="unique_tispan_per_station_in_avgsohgps")
→ db.UniqueConstraint("timespan_id", "z_component_id")
```

### Models with Unnamed Constraints (19 already compliant)

These models already use unnamed constraints and require no changes:

**Association Tables** (10):
- `src/noiz/models/beamforming.py`: BeamformingResultDatachunkAssociation, BeamformingPeakAverageAbspowerAssociation, BeamformingPeakAverageRelpowerAssociation, BeamformingPeakAllAbspowerAssociation, BeamformingPeakAllRelpowerAssociation
- `src/noiz/models/soh.py`: SOHInstrumentComponentAssociation, SOHGPSComponentAssociation, AveragedSOHGPSComponentAssociation
- `src/noiz/models/event_detection.py`: EventConfirmationResultDatachunkAssociation, EventDetectionResultBeamformingResultAssociation

**Other Models** (9):
- `src/noiz/models/datachunk.py`: Datachunk, DatachunkStats, ProcessedDatachunk
- `src/noiz/models/stacking.py`: StackingTimespan, CCFStack
- `src/noiz/models/event_detection.py`: EventDetectionResult

## Error Handling Enhancement

### New Exception Class

**File**: `src/noiz/exceptions.py` (add to existing file)

```python
class ConstraintViolationError(Exception):
    """Raised when a database constraint is violated with enhanced error information."""

    def __init__(self, original_error: Exception, table: str, columns: list[str], values: dict = None):
        self.original_error = original_error
        self.table = table
        self.columns = columns
        self.values = values or {}

        column_list = ", ".join(columns)
        if values:
            value_list = ", ".join(f"{col}={values.get(col, 'N/A')}" for col in columns)
            message = (
                f"Unique constraint violation on {table}({column_list}): "
                f"Duplicate values {value_list}"
            )
        else:
            message = f"Unique constraint violation on {table}({column_list})"

        super().__init__(message)
```

### Error Message Parser

**File**: `src/noiz/database.py` (add new function)

```python
import re
from sqlalchemy.exc import IntegrityError
from noiz.exceptions import ConstraintViolationError

def parse_constraint_violation(error: IntegrityError) -> tuple[str, list[str], dict]:
    """
    Parse constraint violation error to extract table name, columns, and values.

    Returns:
        tuple: (table_name, column_list, value_dict)
    """
    error_msg = str(error.orig)
    dialect_name = db.engine.dialect.name

    if dialect_name == "postgresql":
        # PostgreSQL format: 'duplicate key value violates unique constraint "name"'
        # DETAIL: Key (col1, col2)=(val1, val2) already exists.

        # Extract table from constraint name if available
        table_match = re.search(r'violates unique constraint "(\w+)"', error_msg)
        table = table_match.group(1) if table_match else "unknown"

        # Extract columns from DETAIL
        detail_match = re.search(r'Key \(([^)]+)\)=\(([^)]+)\)', error_msg)
        if detail_match:
            columns = [col.strip() for col in detail_match.group(1).split(',')]
            values_str = detail_match.group(2).split(',')
            values = {col: val.strip() for col, val in zip(columns, values_str)}
            return table, columns, values

    elif dialect_name == "sqlite":
        # SQLite format: 'UNIQUE constraint failed: table.col1, table.col2'
        match = re.search(r'UNIQUE constraint failed: (\w+)\.([\w, .]+)', error_msg)
        if match:
            table = match.group(1)
            columns_str = match.group(2)
            # Handle both 'col1, table.col2' and 'col1, col2' formats
            columns = [col.split('.')[-1].strip() for col in columns_str.split(',')]
            return table, columns, {}

    # Fallback
    return "unknown", [], {}


def enhance_constraint_error(error: IntegrityError) -> ConstraintViolationError:
    """
    Convert SQLAlchemy IntegrityError to enhanced ConstraintViolationError.
    """
    table, columns, values = parse_constraint_violation(error)
    return ConstraintViolationError(error, table, columns, values)
```

### Error Catching in Bulk Operations

**File**: `src/noiz/api/helpers.py` (update existing functions)

```python
from noiz.database import enhance_constraint_error

def bulk_add_objects(objects_to_add: Collection[BulkAddableObjects]) -> None:
    """
    Tries to perform bulk insert of objects to database.
    """
    logger.debug("Performing bulk add_all operation")
    db.session.add_all(objects_to_add)
    logger.debug("Committing")
    try:
        db.session.commit()
    except IntegrityError as e:
        # Enhance error with column information
        enhanced_error = enhance_constraint_error(e)
        logger.error(f"Constraint violation: {enhanced_error}")
        raise enhanced_error from e
    return
```

## Migration Strategy

### Baseline Replacement Approach

Instead of creating an incremental migration that drops and recreates constraints, replace the existing baseline migration.

**Justification**:
- Only one migration exists (created 2024-10-22)
- CI always uses fresh databases
- Cleaner than incremental approach for this scope
- Developers can use `flask db stamp head` on existing databases

**Steps**:

1. **Delete existing baseline**:
   ```bash
   rm migrations/versions/5333b2f172f3_baseline_mixed_ids_ulids_for_data_.py
   ```

2. **Update all model files** (remove `name=` parameters from 16 constraints)

3. **Generate new baseline**:
   ```bash
   uv run flask db revision --autogenerate -m "baseline unnamed constraints"
   ```

4. **Review and test migration**:
   ```bash
   # Test PostgreSQL
   DATABASE_BACKEND=postgresql uv run flask db upgrade

   # Test SQLite
   DATABASE_BACKEND=sqlite uv run flask db upgrade
   ```

5. **Developer migration guide** (in quickstart.md):
   - For existing development databases: `uv run flask db stamp head`
   - Or rebuild from scratch: `uv run flask db upgrade`

## Validation

### Constraint Enforcement Tests

**File**: `tests/integration/test_constraint_enforcement.py` (new file)

```python
import pytest
from sqlalchemy.exc import IntegrityError
from noiz.exceptions import ConstraintViolationError
from noiz.models import QCOneResults, Datachunk, Timespan
from noiz.database import db

@pytest.mark.parametrize("backend", ["postgresql", "sqlite"])
class TestConstraintEnforcement:

    def test_qcone_unique_constraint(self, backend, session, sample_datachunk, sample_qcone_config):
        """Test QCOneResults uniqueness on (datachunk_id, qcone_config_id)."""
        # Create first result
        result1 = QCOneResults(
            datachunk_id=sample_datachunk.id,
            qcone_config_id=sample_qcone_config.id,
            # ... other fields
        )
        session.add(result1)
        session.commit()

        # Attempt duplicate
        result2 = QCOneResults(
            datachunk_id=sample_datachunk.id,
            qcone_config_id=sample_qcone_config.id,
            # ... other fields
        )
        session.add(result2)

        with pytest.raises(ConstraintViolationError) as exc_info:
            session.commit()

        # Verify error enhancement
        error = exc_info.value
        assert error.table == "qcone_results"
        assert set(error.columns) == {"datachunk_id", "qcone_config_id"}

    # Additional tests for each constraint type...
```

### Cross-Backend Consistency Tests

Validate that constraint behavior is identical between PostgreSQL and SQLite:

```python
@pytest.mark.parametrize("backend", ["postgresql", "sqlite"])
def test_all_constraints_enforce_uniformly(backend, session):
    """Verify all unique constraints enforce across both backends."""
    # Test matrix of all constraint types
    # Ensure same violations, same error messages
    pass
```

## Summary

### Changes Required

**Models** (11 files, 16 constraints):
- Remove `name=` parameter from UniqueConstraint definitions
- No other changes needed

**API/Upsert Operations** (3 files, 6 calls):
- Replace `dialect_agnostic_on_conflict(constraint_name=...)`
- With direct `on_conflict_do_update(index_elements=[...])`

**Database Module** (1 file):
- Remove `CONSTRAINT_TO_COLUMNS` dictionary (19 mappings)
- Remove `dialect_agnostic_on_conflict()` function (30 lines)
- Add `parse_constraint_violation()` function (40 lines)
- Add `enhance_constraint_error()` function (10 lines)
- Update error handling in bulk operations

**Exceptions** (1 file):
- Add `ConstraintViolationError` class (20 lines)

**Migrations** (1 file):
- Replace baseline migration with unnamed constraints version

**Tests** (3 new files):
- Constraint enforcement tests
- Error message enhancement tests
- Cross-backend consistency tests

### Benefits

1. **Simplified Code**: No constraint name mapping needed
2. **Uniform Behavior**: Same patterns for PostgreSQL and SQLite
3. **Better Errors**: Column-level information in all constraint violations
4. **Maintainability**: Adding new constraints requires no mapping updates
5. **Type Safety**: Direct column references less error-prone

### Backward Compatibility

- **Constraint enforcement behavior**: Identical before and after
- **Data**: No data changes, only schema metadata
- **Database migrations**: Fresh baseline requires database rebuild or stamp
- **External dependencies**: None (confirmed in clarifications)

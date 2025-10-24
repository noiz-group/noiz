# Developer Quickstart: Unnamed Constraints

**Feature**: Remove Named Database Constraints
**Date**: 2025-10-24

## Overview

This guide shows developers how to work with unnamed unique constraints in the Noiz codebase after the transition from named constraints.

## What Changed

### Before: Named Constraints + Translation Layer

```python
# Model definition
class MyModel(db.Model):
    __table_args__ = (
        db.UniqueConstraint("col_a", "col_b", name="unique_cols_a_b"),
    )

# Upsert operation
from noiz.database import dialect_agnostic_on_conflict

insert_command = dialect_agnostic_on_conflict(
    insert_stmt,
    constraint_name="unique_cols_a_b",  # Required mapping in CONSTRAINT_TO_COLUMNS
    set_={...}
)
```

### After: Unnamed Constraints + Direct Column Reference

```python
# Model definition
class MyModel(db.Model):
    __table_args__ = (
        db.UniqueConstraint("col_a", "col_b"),  # No name parameter
    )

# Upsert operation
insert_command = insert_stmt.on_conflict_do_update(
    index_elements=["col_a", "col_b"],  # Direct column reference
    set_={...}
)
```

## Adding New Models with Unique Constraints

### Step 1: Define the Constraint (No Name!)

```python
from noiz.database import db

class ProcessingResult(db.Model):
    __tablename__ = "processing_results"

    id = db.Column(db.Integer, primary_key=True)
    timespan_id = db.Column(db.Integer, db.ForeignKey("timespans.id"), nullable=False)
    config_id = db.Column(db.Integer, db.ForeignKey("processing_configs.id"), nullable=False)
    result_data = db.Column(db.JSON)

    # Unnamed unique constraint - works for both PostgreSQL and SQLite
    __table_args__ = (
        db.UniqueConstraint("timespan_id", "config_id"),
    )
```

**Key Points**:
- No `name=` parameter needed
- Constraint works identically on PostgreSQL and SQLite
- No entry in `CONSTRAINT_TO_COLUMNS` needed (that dictionary no longer exists!)

### Step 2: Implement Upsert Operations

```python
from sqlalchemy.sql import Insert
from noiz.database import get_dialect_insert
from noiz.models import ProcessingResult

def _prepare_upsert_command_processing_result(result: ProcessingResult) -> Insert:
    """
    Generate upsert command for ProcessingResult.

    :param result: ProcessingResult instance to upsert
    :return: SQLAlchemy Insert statement with conflict resolution
    """
    # Get dialect-specific insert function
    insert_func = get_dialect_insert()

    # Create insert statement
    insert_stmt = insert_func(ProcessingResult).values(
        timespan_id=result.timespan_id,
        config_id=result.config_id,
        result_data=result.result_data,
    )

    # Add conflict resolution using column names directly
    insert_command = insert_stmt.on_conflict_do_update(
        index_elements=["timespan_id", "config_id"],  # Must match UniqueConstraint columns
        set_={
            "result_data": result.result_data,  # Update these fields on conflict
        },
    )

    return insert_command
```

**Key Points**:
- `index_elements` must exactly match the columns in `UniqueConstraint`
- Works for both PostgreSQL and SQLite without translation
- Order of columns doesn't matter, but names must match exactly

### Step 3: Use Upsert in Bulk Operations

```python
from noiz.api.helpers import bulk_add_or_upsert_objects

def process_results(results: list[ProcessingResult]) -> None:
    """Process and upsert results to database."""
    bulk_add_or_upsert_objects(
        objects_to_add=results,
        upserter_callable=_prepare_upsert_command_processing_result,
        bulk_insert=True
    )
```

## Common Patterns

### Single-Column Uniqueness

```python
class Component(db.Model):
    __tablename__ = "components"

    code = db.Column(db.String(10), nullable=False)

    __table_args__ = (
        db.UniqueConstraint("code"),
    )

# Upsert
insert_command = insert_stmt.on_conflict_do_update(
    index_elements=["code"],
    set_={"updated_at": datetime.utcnow()}
)
```

### Multi-Column Uniqueness

```python
class CrossCorrelation(db.Model):
    __tablename__ = "crosscorrelations"

    timespan_id = db.Column(db.Integer, db.ForeignKey("timespans.id"))
    componentpair_id = db.Column(db.Integer, db.ForeignKey("componentpairs.id"))
    params_id = db.Column(db.Integer, db.ForeignKey("params.id"))

    __table_args__ = (
        db.UniqueConstraint("timespan_id", "componentpair_id", "params_id"),
    )

# Upsert
insert_command = insert_stmt.on_conflict_do_update(
    index_elements=["timespan_id", "componentpair_id", "params_id"],
    set_={"ccf": result.ccf, "file_id": result.file_id}
)
```

### Multiple Separate Constraints

```python
class Timespan(db.Model):
    __tablename__ = "timespans"

    starttime = db.Column(db.DateTime, nullable=False)
    midtime = db.Column(db.DateTime, nullable=False)
    endtime = db.Column(db.DateTime, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("starttime"),
        db.UniqueConstraint("midtime"),
        db.UniqueConstraint("endtime"),
        db.UniqueConstraint("starttime", "midtime", "endtime"),
    )

# Upsert on composite constraint
insert_command = insert_stmt.on_conflict_do_update(
    index_elements=["starttime", "midtime", "endtime"],
    set_={}  # Do nothing on conflict
)
```

## Error Handling

### Enhanced Constraint Violations

Constraint violations now provide detailed column information:

```python
from noiz.exceptions import ConstraintViolationError

try:
    db.session.add(duplicate_result)
    db.session.commit()
except ConstraintViolationError as e:
    # Enhanced error with column details
    print(f"Table: {e.table}")
    print(f"Conflicting columns: {e.columns}")
    print(f"Values: {e.values}")
    print(f"Message: {e}")
    # Example output:
    # Table: processing_results
    # Conflicting columns: ['timespan_id', 'config_id']
    # Values: {'timespan_id': '123', 'config_id': '456'}
    # Message: Unique constraint violation on processing_results(timespan_id, config_id): ...
```

### Graceful Fallback

If column extraction fails, the original database error is still available:

```python
try:
    db.session.commit()
except ConstraintViolationError as e:
    # Original error preserved
    original_db_error = e.original_error
    logger.error(f"Original error: {original_db_error}")
```

## Testing Constraints

### Unit Test Pattern

```python
import pytest
from sqlalchemy.exc import IntegrityError
from noiz.exceptions import ConstraintViolationError

def test_processing_result_uniqueness(session, sample_timespan, sample_config):
    """Test ProcessingResult unique constraint on (timespan_id, config_id)."""
    # Create first result
    result1 = ProcessingResult(
        timespan_id=sample_timespan.id,
        config_id=sample_config.id,
        result_data={"value": 1.0}
    )
    session.add(result1)
    session.commit()

    # Attempt duplicate
    result2 = ProcessingResult(
        timespan_id=sample_timespan.id,
        config_id=sample_config.id,
        result_data={"value": 2.0}
    )
    session.add(result2)

    # Should raise enhanced error
    with pytest.raises(ConstraintViolationError) as exc_info:
        session.commit()

    # Verify error details
    error = exc_info.value
    assert error.table == "processing_results"
    assert set(error.columns) == {"timespan_id", "config_id"}
```

### Cross-Backend Testing

Test constraints work identically on both databases:

```python
@pytest.mark.parametrize("backend", ["postgresql", "sqlite"])
def test_constraint_enforcement(backend, session):
    """Verify constraint works on both PostgreSQL and SQLite."""
    # Same test logic runs on both backends
    # Validates uniform behavior
    pass
```

## Migration Guide for Existing Developers

### Fresh Install (Recommended)

If you're starting fresh or can rebuild your development database:

```bash
# Delete existing database
rm noiz_dev.db  # SQLite
# or drop PostgreSQL database: dropdb noiz_dev

# Run migrations
uv run flask db upgrade

# Re-import any test data
```

### Existing Database

If you want to keep your existing development database:

```bash
# Mark database as up-to-date with new baseline
uv run flask db stamp head

# Verify
uv run flask db current
```

**Note**: The `stamp` approach keeps your data but your constraints will still have names in the database. This is fine for development, as unnamed constraints in code work with both named and unnamed database constraints.

## Troubleshooting

### Error: "Unknown constraint name"

**Problem**: You're using the old pattern with `constraint_name=`.

**Solution**: Use `index_elements=` instead:

```python
# Old (no longer works)
dialect_agnostic_on_conflict(constraint_name="my_constraint", ...)

# New
insert_stmt.on_conflict_do_update(index_elements=["col_a", "col_b"], ...)
```

### Error: "index_elements must match constraint columns"

**Problem**: Column names in `index_elements` don't match `UniqueConstraint`.

**Solution**: Check your model's `__table_args__` and ensure exact match:

```python
# Model says:
db.UniqueConstraint("timespan_id", "config_id")

# Upsert must say:
index_elements=["timespan_id", "config_id"]  # Exact match required
```

### Error: "constraint violation but no column information"

**Problem**: Error message parsing failed for your database version.

**Solution**: This is expected fallback behavior. The error will still show:
- The original database error message
- The table name (if extractable)
- You can still debug using the database error

File an issue if this happens frequently with specific constraint types.

## Best Practices

### 1. Keep Constraint Columns Minimal

```python
# Good: Only columns that truly define uniqueness
db.UniqueConstraint("timespan_id", "component_id", "params_id")

# Bad: Including unnecessary columns
db.UniqueConstraint("timespan_id", "component_id", "params_id", "created_at")
```

### 2. Match index_elements to Constraint Exactly

```python
# Model
db.UniqueConstraint("col_a", "col_b", "col_c")

# Upsert - same order preferred (not required, but clearer)
index_elements=["col_a", "col_b", "col_c"]
```

### 3. Document Complex Constraints

```python
class MyModel(db.Model):
    __table_args__ = (
        # Ensure one result per configuration per timespan
        db.UniqueConstraint("timespan_id", "config_id"),

        # Ensure globally unique start times across all configurations
        db.UniqueConstraint("starttime"),
    )
```

### 4. Test Constraint Enforcement

Every model with unique constraints should have a test verifying:
- Duplicate inserts are rejected
- Error messages include column information
- Both PostgreSQL and SQLite behave identically

## Quick Reference

### Constraint Definition

```python
# Single column
db.UniqueConstraint("column_name")

# Multiple columns
db.UniqueConstraint("col_a", "col_b", "col_c")

# Multiple separate constraints
__table_args__ = (
    db.UniqueConstraint("col_a"),
    db.UniqueConstraint("col_b", "col_c"),
)
```

### Upsert Pattern

```python
from noiz.database import get_dialect_insert

# 1. Get dialect-specific insert
insert_func = get_dialect_insert()

# 2. Create insert statement
insert_stmt = insert_func(MyModel).values(...)

# 3. Add conflict resolution
insert_command = insert_stmt.on_conflict_do_update(
    index_elements=["col_a", "col_b"],  # Match constraint columns
    set_={"updated_field": new_value}   # Fields to update on conflict
)

# 4. Execute
db.session.execute(insert_command)
db.session.commit()
```

### Testing Pattern

```python
import pytest
from noiz.exceptions import ConstraintViolationError

def test_my_model_uniqueness(session):
    # Create first instance
    obj1 = MyModel(col_a=1, col_b=2)
    session.add(obj1)
    session.commit()

    # Attempt duplicate
    obj2 = MyModel(col_a=1, col_b=2)
    session.add(obj2)

    # Verify constraint enforcement
    with pytest.raises(ConstraintViolationError) as exc_info:
        session.commit()

    assert exc_info.value.table == "my_table"
    assert set(exc_info.value.columns) == {"col_a", "col_b"}
```

## Further Reading

- [data-model.md](./data-model.md) - Complete data model documentation
- [SQLAlchemy on_conflict_do_update](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#insert-on-conflict-upsert) - Official documentation
- [Flask-SQLAlchemy](https://flask-sqlalchemy.palletsprojects.com/) - Flask integration
- [pytest fixtures](https://docs.pytest.org/en/stable/fixture.html) - Testing setup

## Questions?

If you encounter issues not covered in this guide:
1. Check [data-model.md](./data-model.md) for detailed examples
2. Review existing upsert operations in `src/noiz/api/` for patterns
3. Run tests to see working examples: `just unit_tests`

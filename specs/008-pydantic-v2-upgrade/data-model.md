# Data Model: Pydantic v2 Upgrade

**Feature**: 008-pydantic-v2-upgrade
**Date**: 2025-10-25

## Overview

This document identifies all pydantic dataclass entities affected by the v1→v2 upgrade and documents their migration requirements.

## Affected Entities

### Entity 1: DatachunkParams (processing_params.py)

**Purpose**: Configuration parameters for datachunk processing stage

**Current v1 Pattern**:
```python
from pydantic.dataclasses import dataclass

@dataclass
class DatachunkParams:
    # Type hints with defaults
    field_name: Optional[str] = None
    numeric_field: int = 100
    # etc.
```

**v2 Migration Requirements**:
- Import statement: NO CHANGE (remains `from pydantic.dataclasses import dataclass`)
- Type annotations: Verify all `Optional[T]` fields have `= None` defaults
- Field definitions: NO CHANGE (simple type hints compatible with v2)
- Config class: NOT APPLICABLE (no Config class used)
- Validators: NOT APPLICABLE (uses `__post_init__` if any custom validation)

**Validation Behavior Preservation**:
- TOML→dataclass instantiation must produce identical results
- Numeric type coercion: Verify float-to-int conversions in TOML files (v2 stricter)
- Required vs optional: All Optional fields have defaults, should work identically

**Testing Approach**:
- Unit test: Instantiate DatachunkParams with valid/invalid data
- TOML test: Load config_examples/datachunk_params.toml and verify parsing
- Edge cases: Missing required fields, type mismatches, numeric conversions

---

### Entity 2: CrosscorrelationCartesianParams (processing_params.py)

**Purpose**: Configuration parameters for Cartesian cross-correlation processing

**Current v1 Pattern**: Same as DatachunkParams (simple dataclass with type hints)

**v2 Migration Requirements**: Same as DatachunkParams

**Validation Behavior Preservation**: Same as DatachunkParams

**Testing Approach**:
- Unit test: Instantiate with valid/invalid data
- TOML test: Load config_examples/crosscorrelation_cartesian_params.toml
- Edge cases: Required field validation

---

### Entity 3: CrosscorrelationCylindricalParams (processing_params.py)

**Purpose**: Configuration parameters for cylindrical cross-correlation processing

**Current v1 Pattern**: Same as DatachunkParams

**v2 Migration Requirements**: Same as DatachunkParams

**Validation Behavior Preservation**: Same as DatachunkParams

**Testing Approach**:
- Unit test: Instantiate with valid/invalid data
- TOML test: Load config_examples/crosscorrelation_cylindrical_params.toml
- Edge cases: Required field validation

---

### Entity 4: BeamformingParams (processing_params.py)

**Purpose**: Configuration parameters for beamforming analysis

**Current v1 Pattern**: Same as DatachunkParams

**v2 Migration Requirements**: Same as DatachunkParams

**Validation Behavior Preservation**: Same as DatachunkParams

**Testing Approach**:
- Unit test: Instantiate with valid/invalid data
- TOML test: Load config_examples/beamforming_params.toml
- Edge cases: Required field validation

---

### Entity 5: PPSDParams (processing_params.py)

**Purpose**: Configuration parameters for Power Spectral Density calculations

**Current v1 Pattern**: Same as DatachunkParams

**v2 Migration Requirements**: Same as DatachunkParams

**Validation Behavior Preservation**: Same as DatachunkParams

**Testing Approach**:
- Unit test: Instantiate with valid/invalid data
- TOML test: Load config_examples/ppsd_params.toml
- Edge cases: Required field validation

---

### Entity 6: StackingParams (stacking.py)

**Purpose**: Configuration parameters for time-domain stacking

**Current v1 Pattern**: Same as DatachunkParams

**v2 Migration Requirements**: Same as DatachunkParams

**Validation Behavior Preservation**: Same as DatachunkParams

**Testing Approach**:
- Unit test: Instantiate with valid/invalid data
- TOML test: Load config_examples/stacking_params.toml
- Edge cases: Required field validation

---

### Entity 7: QC-related dataclasses (qc.py)

**Purpose**: Quality control validation models

**Current v1 Pattern**: Same as DatachunkParams, may include nested dataclasses

**v2 Migration Requirements**:
- Same as DatachunkParams
- Special attention to nested dataclass validation if present

**Validation Behavior Preservation**:
- Same as DatachunkParams
- Nested dataclass validation must work identically

**Testing Approach**:
- Unit test: Instantiate with valid/invalid data
- Nested dataclass test: Verify nested validation works
- Edge cases: Required field validation, nested structure

---

## Special Considerations

### `__post_init__` Execution Order

**Research Finding**: In v2, `__post_init__` runs AFTER validation (was BEFORE in v1)

**Affected Entities**:
- DatachunkParamsHolder (if exists)
- EventDetectionParamsHolder (if exists)
- Any other Holder classes with `__post_init__`

**Impact**: LOW RISK - Functionality should be preserved or improved

**Testing**: Verify `__post_init__` methods still execute correctly after validation

### Optional Field Semantics

**Research Finding**: `Optional[T]` in v2 means "required field accepting None"

**Impact**: LOW RISK - All Noiz Optional fields have `= None` defaults

**Action**: Scan all 3 files for `Optional[T]` without defaults (should find none)

### Numeric Type Coercion

**Research Finding**: v2 stricter about float→int conversion (3.0 OK, 3.7 fails)

**Impact**: MEDIUM RISK - TOML files may contain floats for integer fields

**Action**:
- Review TOML files for numeric fields
- Ensure integers are specified as integers, not floats
- Test TOML loading with v2 to catch issues early

## Entity Relationships

```
TOML File → toml.load() → dict → **DataclassParams(...) → Database
                                       ↑
                                  pydantic validation
                                  (v1→v2 upgrade here)
```

No changes to database models or relationships. Pydantic is used only for TOML validation layer.

## Migration Strategy Summary

**Uniform Approach**: All 7 entities use identical migration pattern:
1. No code changes required (simple dataclass pattern compatible with v2)
2. Verify all Optional fields have defaults
3. Test TOML parsing with v2
4. Verify `__post_init__` behavior if present
5. Check numeric type conversions in TOML files

**Low-Risk Migration**: Simple dataclass patterns in Noiz are highly compatible with v2. Main risk is stricter TOML validation catching existing data quality issues.

## Testing Checklist

- [ ] Unit test: Instantiate each dataclass directly with dict data
- [ ] TOML test: Load and parse each example TOML file from config_examples/
- [ ] Edge case test: Missing required fields trigger validation errors
- [ ] Edge case test: Type mismatches trigger validation errors
- [ ] `__post_init__` test: Verify execution order and behavior
- [ ] Nested dataclass test: Verify nested validation (qc.py)
- [ ] Numeric conversion test: Verify float/int handling in TOML files
- [ ] Performance test: Record baseline vs v2 validation time

## References

- Research document: research.md (Section 5: Type Annotations, Section 6: TOML Compatibility, Section 9: `__post_init__`)
- Source files: src/noiz/models/processing_params.py, src/noiz/models/qc.py, src/noiz/models/stacking.py
- TOML examples: config_examples/ directory

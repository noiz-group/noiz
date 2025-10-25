# Research: Pydantic v1 to v2 Migration for Dataclasses

## Migration Overview

This research document provides a comprehensive guide for upgrading the Noiz seismic processing project from pydantic v1.8 to v2.x (>=2.0,<3.0).
The project uses pydantic exclusively for dataclass-based validation of TOML configuration files in three files:

- `src/noiz/models/processing_params.py` - DatachunkParams, CrosscorrelationParams, BeamformingParams, PPSDParams, EventDetectionParams, EventConfirmationParams
- `src/noiz/models/qc.py` - QCOneConfigHolder, QCTwoConfigHolder, and related dataclasses
- `src/noiz/models/stacking.py` - StackingSchemaHolder

All dataclasses use `from pydantic.dataclasses import dataclass` (not BaseModel).
The TOML files are loaded using Python's `toml` library and then validated by instantiating these pydantic dataclasses with `**kwargs`.

Pydantic v2 represents a major rewrite with a Rust core providing significant performance improvements but introducing several breaking changes.
The migration requires careful attention to dataclass-specific patterns, as many v2 examples focus on BaseModel.

## Section 1: Import Patterns

### Current (v1) Pattern

```python
from pydantic.dataclasses import dataclass
```

### v2 Pattern - NO CHANGES REQUIRED

The import statement remains identical in v2:

```python
from pydantic.dataclasses import dataclass
```

However, additional imports will be needed for configuration and validators:

```python
from pydantic import ConfigDict, field_validator, model_validator
from pydantic.dataclasses import dataclass
```

### Migration Notes

- The `@dataclass` decorator from `pydantic.dataclasses` continues to work in v2
- No import statement changes required for basic dataclass usage
- Validator decorators must be imported from `pydantic` (not `pydantic.dataclasses`)
- ConfigDict is the new configuration mechanism

## Section 2: Config Class Migration

### Current (v1) Pattern - NOT USED IN NOIZ

Pydantic v1 dataclasses could use an inner `Config` class (though Noiz doesn't currently use this):

```python
@dataclass
class MyDataclass:
    field: str

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True
```

### v2 Pattern - Two Options

Pydantic v2 dataclasses support configuration via `ConfigDict` in two ways:

**Option 1 - Decorator argument (recommended):**

```python
from pydantic import ConfigDict
from pydantic.dataclasses import dataclass

@dataclass(config=ConfigDict(validate_assignment=True))
class MyDataclass:
    field: str
```

**Option 2 - Class attribute:**

```python
@dataclass
class MyDataclass:
    field: str
    __pydantic_config__ = ConfigDict(validate_assignment=True)
```

### Key Configuration Changes

- `allow_mutation` is removed - replaced by `frozen` parameter
- Config inheritance behavior changed: vanilla dataclasses used as fields no longer inherit parent config
- `extra='allow'` is no longer supported for pydantic dataclasses (extra fields are omitted from representation)

### Impact on Noiz

Current Noiz dataclasses do NOT use Config classes, so minimal impact.
However, if validation behavior needs customization during migration, use `ConfigDict` with the decorator argument.

## Section 3: Validator Migration

### Current (v1) Pattern - NOT USED IN NOIZ

Noiz currently uses `__post_init__` for validation logic rather than pydantic validators.
However, understanding validator migration is important for potential future enhancements.

v1 validators used `@validator`:

```python
from pydantic.dataclasses import dataclass
from pydantic import validator

@dataclass
class MyDataclass:
    field: str

    @validator('field')
    def validate_field(cls, v):
        return v
```

### v2 Pattern - @field_validator and @model_validator

v2 introduces two new decorators replacing `@validator` and `@root_validator`:

**Field-level validation:**

```python
from pydantic import field_validator
from pydantic.dataclasses import dataclass

@dataclass
class MyDataclass:
    product_id: str

    @field_validator('product_id', mode='before')
    @classmethod
    def convert_int_serial(cls, v):
        if isinstance(v, int):
            v = str(v).zfill(5)
        return v
```

**Model-level validation:**

```python
from pydantic import model_validator
from pydantic.dataclasses import dataclass

@dataclass
class MyDataclass:
    field_a: int
    field_b: int

    @model_validator(mode='after')
    def check_model(self) -> Self:
        if self.field_a > self.field_b:
            raise ValueError('field_a must be <= field_b')
        return self
```

### Key Differences

- `@validator` is deprecated - use `@field_validator`
- `@root_validator` is deprecated - use `@model_validator`
- `field` and `config` arguments removed from validator signatures
- Must use `@classmethod` decorator for field validators
- `each_item` keyword argument removed - use `Annotated` types instead
- Three modes: `'before'` (before validation), `'after'` (after validation), `'wrap'` (full control)
- **TypeError is no longer converted to ValidationError** - programming errors propagate directly

### Impact on Noiz

Current Noiz dataclasses use `__post_init__` instead of validators.
Two examples in the codebase:

1. **EventDetectionParamsHolder.__post_init__** - Validates that required fields are present based on detection_type
2. **DatachunkParamsHolder.__post_init__** - Modifies remove_response based on response_constant_coefficient

These `__post_init__` methods will continue to work but with changed execution order (see Section 9).

## Section 4: Field Definitions

### Current (v1) Pattern

Noiz dataclasses use standard Python dataclass field definitions with type hints and defaults:

```python
@dataclass
class DatachunkParamsHolder:
    sampling_rate: float
    prefiltering_low: float
    remove_response: bool
    response_constant_coefficient: Optional[float] = None
```

### v2 Pattern - Mostly Compatible

Basic field definitions remain compatible, but `Field()` usage has changes:

```python
from pydantic import Field
from pydantic.dataclasses import dataclass

@dataclass
class MyDataclass:
    # Simple fields - no changes
    sampling_rate: float
    remove_response: bool

    # Optional with default - no changes
    response_constant_coefficient: Optional[float] = None

    # Using Field() - parameter name changes
    height: int = Field(default=None, ge=50, le=300)  # v2
    # OLD v1: height: int = Field(default=None, min_value=50, max_value=300)
```

### Removed/Renamed Field() Parameters

**Removed entirely:**
- `const` - no direct replacement
- `allow_mutation` - use `frozen` at ConfigDict level
- `regex` - replaced by `pattern`
- `final` - no replacement

**Renamed parameters:**
- `min_items` → `min_length`
- `max_items` → `max_length`
- `min_value` → `ge` (greater than or equal)
- `max_value` → `le` (less than or equal)

### Generic Container Constraints

v1 allowed: `my_list: list[str] = Field(pattern='.*')`
v2 requires: `my_list: list[Annotated[str, Field(pattern=".*")]]`

Constraints must be explicitly applied to generic type arguments using `Annotated`.

### Impact on Noiz

Current Noiz dataclasses do NOT use `Field()` - they only use type hints and default values.
No changes required to existing field definitions.
If future development adds validation constraints, use v2 parameter names.

## Section 5: Type Annotation Changes

### Optional Field Semantics - BREAKING CHANGE

**v1 behavior:**
- `Optional[str]` - not required, defaults to None
- `Optional[str] = None` - not required, defaults to None

**v2 behavior (matches standard Python dataclasses):**
- `Optional[str]` - **REQUIRED field that accepts None as a value**
- `Optional[str] = None` - not required, defaults to None

This is a significant semantic change.

### Examples in Noiz Codebase

From `DatachunkParamsHolder`:
```python
response_constant_coefficient: Optional[float] = None  # Not required - OK in both v1 and v2
```

From `BeamformingParamsHolder`:
```python
window_length_minimum_periods: Optional[Union[int, float]] = None
window_length: Optional[Union[int, float]] = None
smin1: Optional[float] = None
# All have explicit = None defaults, so behavior unchanged
```

### Union Type Handling

v2 preserves input types when possible:
- v1: `Union[int, str]` might coerce "123" → 123 based on order
- v2: `Union[int, str]` preserves "123" as str if it's a valid str

### Float-to-Int Conversion

v2 restricts conversion:
- v1: `int_field: int = 3.7` would accept and convert
- v2: `int_field: int = 3.7` raises ValidationError unless decimal part is exactly zero (e.g., 3.0 is OK)

### Impact on Noiz

**LOW RISK** - All Optional fields in Noiz have explicit `= None` defaults.
Review all `Optional[T]` annotations to ensure they have defaults.

**TOML Loading Pattern:**
The current pattern `DatachunkParamsHolder(**loaded_dict)` from TOML will work correctly because:
1. All Optional fields have explicit defaults
2. Missing keys in TOML dict will use field defaults
3. Present keys will be validated against their types

## Section 6: TOML Parsing Compatibility

### Current Pattern in Noiz

TOML files are loaded and validated as follows:

```python
import toml

with open(file=filepath, mode="r") as f:
    loaded_dict: Dict = toml.load(f=f)

# Validate by instantiating dataclass
validated = DatachunkParamsHolder(**loaded_dict)
```

### v2 Changes Affecting Dict Loading

1. **Tuples no longer coerce to dicts**
   - v1: Iterables of pairs accepted for dict fields
   - v2: Only actual dicts accepted
   - Impact: NONE (TOML always produces dicts)

2. **Stricter type validation**
   - v2 is stricter about type coercion
   - Example: float-to-int only works for exact integers (3.0 OK, 3.7 fails)

3. **Nested dataclasses must use dicts**
   - When dataclasses are used as field types, they must receive dict input (not tuples)
   - Example in Noiz: `QCOneConfigHolder` contains `rejected_times: Tuple[QCOneConfigRejectedTimeHolder, ...]`

### Nested Dataclass Pattern in Noiz

From `src/noiz/processing/configs.py`:

```python
def validate_dict_as_qcone_holder(loaded_dict: Dict) -> QCOneConfigHolder:
    processed_dict = loaded_dict.copy()

    if "rejected_times" in loaded_dict.keys():
        validated_forbidden_channels = []
        for forb_chn in loaded_dict["rejected_times"]:
            # Each forb_chn is a dict from TOML
            validated_forbidden_channels.append(QCOneConfigRejectedTimeHolder(**forb_chn))
        processed_dict["rejected_times"] = validated_forbidden_channels

    return QCOneConfigHolder(**processed_dict)
```

This pattern will continue to work in v2 because:
- TOML produces dicts (not tuples)
- Each nested config is instantiated as a dataclass
- The validated dataclass instances are passed to the parent

### v2 ValidationError Structure

Error reporting improved but TypeError no longer wraps:
- v1: TypeError in validator → ValidationError
- v2: TypeError propagates directly (better debugging)

### Testing TOML Compatibility

The existing test pattern should reveal any issues:

```python
# Load TOML
with open(toml_path) as f:
    config_dict = toml.load(f)

# Validate - this will raise ValidationError if issues exist
validated = DatachunkParamsHolder(**config_dict)
```

### Impact on Noiz

**MEDIUM RISK** - The TOML → dict → dataclass pattern is fundamentally compatible but:

1. **Stricter type validation** may catch existing issues in TOML files
2. **Float-to-int conversion** may affect numeric fields
3. **Nested dataclass validation** should work but needs testing

**Recommended Testing:**
- Load all example TOML configs and validate against v2 dataclasses
- Check for any ValidationError exceptions
- Pay attention to numeric type coercion

## Section 7: Performance Characteristics

### Rust Core

Pydantic v2 is built on a Rust core (`pydantic-core`) providing:

- **Significantly faster validation** - benchmarks show 5-50x speedup depending on model complexity
- **Linear-time regex** - uses Rust's regex crate preventing exponential-time attacks on untrusted input
- **Optimized type checking** - removed input type preservation overhead from v1

### Memory Efficiency

v2 is more memory-efficient:
- Removed various metadata-tracking mechanisms
- Simplified decorator implementation
- Better handling of large models

### Compilation Overhead

First instantiation of a model may be slightly slower due to Rust compilation, but subsequent validations are much faster.

### Impact on Noiz

**HIGH BENEFIT** - Configuration validation will be faster, especially when:

1. Loading multiple TOML config files (system test scenarios)
2. Validating nested dataclasses (QCOne/QCTwo with rejected_times)
3. Processing BeamformingParamsHolder with many Optional fields

The performance improvement should be noticeable when:
- CLI commands load config files
- System tests create many param objects
- Beamforming generates multiple configs (see `generate_multiple_beamforming_configs_based_on_single_holder`)

**No performance regression expected** - v2 is strictly faster than v1 for validation tasks.

## Section 8: Testing Strategy

### Phase 1: Unit Testing - Dataclass Instantiation

Test that all dataclasses can be instantiated with valid data:

```python
def test_datachunk_params_holder_instantiation():
    # Minimal valid config
    holder = DatachunkParamsHolder(
        sampling_rate=24.0,
        prefiltering_low=0.01,
        prefiltering_high=12.0,
        prefiltering_order=4,
        preprocessing_taper_type="cosine",
        preprocessing_taper_side="both",
        preprocessing_taper_max_length=5.0,
        preprocessing_taper_max_percentage=0.1,
        remove_response=True,
        datachunk_sample_tolerance=0.02,
        zero_padding_method="tapered_padded",
        padding_taper_type="cosine",
        padding_taper_max_length=5.0,
        padding_taper_max_percentage=0.1,
    )
    assert holder.sampling_rate == 24.0
```

### Phase 2: TOML Loading Tests

Test all example TOML files load correctly:

```python
import toml
from pathlib import Path

def test_all_toml_configs_load():
    config_dir = Path("config_examples")

    for toml_file in config_dir.glob("*.toml"):
        with open(toml_file) as f:
            config_dict = toml.load(f)

        # This should not raise ValidationError
        validated = parse_single_config_toml(toml_file)
        assert validated is not None
```

### Phase 3: __post_init__ Behavior Testing

Verify __post_init__ execution order changed correctly:

```python
def test_datachunk_params_holder_post_init():
    # Test that response_constant_coefficient modifies remove_response
    holder = DatachunkParamsHolder(
        # ... required fields ...
        response_constant_coefficient=1.0,
        remove_response=True,  # Should be set to False by __post_init__
    )
    # In v2, __post_init__ runs AFTER validation
    assert holder.remove_response is False

def test_event_detection_params_holder_validation():
    # Test that __post_init__ validation still works
    with pytest.raises(ValueError, match="StaLta detection is missing"):
        EventDetectionParamsHolder(
            minimum_frequency=0.1,
            maximum_frequency=10.0,
            output_margin_length_sec=5.0,
            datachunk_params_id=1,
            detection_type="sta_lta",
            # Missing required fields for sta_lta
        )
```

### Phase 4: Type Coercion Tests

Test edge cases for type coercion:

```python
def test_float_to_int_coercion():
    # Should work - exact integer
    holder = ProcessedDatachunkParamsHolder(
        # ...
        filtering_order=4.0,  # Should work in v2
    )
    assert holder.filtering_order == 4

    # Should fail - non-integer float
    with pytest.raises(ValidationError):
        ProcessedDatachunkParamsHolder(
            # ...
            filtering_order=4.5,  # Should fail in v2
        )
```

### Phase 5: Nested Dataclass Tests

Test nested dataclass validation:

```python
def test_qcone_holder_with_rejected_times():
    config_dict = {
        "datachunk_params_id": 1,
        "rejected_times": [
            {
                "network": "XX",
                "station": "STA1",
                "component": "Z",
                "starttime": "2020-01-01",
                "endtime": "2020-12-31",
            }
        ],
    }

    holder = validate_dict_as_qcone_holder(config_dict)
    assert len(holder.rejected_times) == 1
    assert holder.rejected_times[0].network == "XX"
```

### Phase 6: Integration Testing

Run existing system tests to ensure:
1. Config loading in CLI works
2. Database model creation from holders works
3. Processing pipelines using configs work

### Testing Checklist

- [ ] All dataclass instantiations pass
- [ ] All TOML example files load without ValidationError
- [ ] __post_init__ execution order correct
- [ ] Type coercion behaves as expected
- [ ] Nested dataclass validation works
- [ ] Optional field handling correct (all have defaults)
- [ ] Existing unit tests pass
- [ ] Existing system tests pass
- [ ] CLI commands load configs correctly

### Test Fixtures

Create fixtures for common test data:

```python
@pytest.fixture
def valid_datachunk_params_dict():
    return {
        "sampling_rate": 24.0,
        "prefiltering_low": 0.01,
        # ... all required fields
    }

@pytest.fixture
def example_toml_dir():
    return Path(__file__).parent.parent / "config_examples"
```

## Section 9: __post_init__ Execution Order

### Critical Change: Execution Order Reversed

**v1 behavior:**
- `__post_init__` runs BEFORE pydantic validation
- Could modify fields before validation
- Used `__post_init_post_parse__` to run after validation

**v2 behavior:**
- `__post_init__` runs AFTER pydantic validation
- Fields are already validated when __post_init__ runs
- `__post_init_post_parse__` removed (no longer needed)

### Execution Sequence in v2

1. `@model_validator(mode='before')` - runs first
2. Pydantic field validation
3. `__post_init__()` - runs here
4. `@model_validator(mode='after')` - runs last

### Impact on Noiz

Two dataclasses use `__post_init__`:

#### 1. DatachunkParamsHolder.__post_init__

```python
def __post_init__(self):
    if self.response_constant_coefficient is not None:
        self.remove_response = False
```

**Analysis:**
- Modifies `remove_response` based on `response_constant_coefficient`
- In v1: ran before validation
- In v2: runs after validation

**Impact:** LOW RISK
- Field modification after validation is acceptable
- No validation depends on the modified value
- Behavior unchanged from user perspective

#### 2. EventDetectionParamsHolder.__post_init__

```python
def __post_init__(self):
    if self.detection_type == "sta_lta":
        if None in (
            self.n_short_time_average,
            self.n_long_time_average,
            self.trigger_value,
            self.detrigger_value,
        ):
            raise ValueError(
                "EventDetectionParams is invalid: "
                "At least one parameter required for a StaLta detection is missing. "
                "n_short_time_average, n_long_time_average, trigger_value "
                "and detrigger_value are all required."
            )
    elif self.detection_type == "amplitude_spike":
        if self.peak_ground_velocity_threshold is None:
            raise ValueError(
                "EventDetectionParams is invalid: "
                "peak_ground_velocity_threshold is required for an AmplitudeSpike detection."
            )
    else:
        raise ValueError(
            "EventDetectionParams is invalid: detection_type is neither 'sta_lta' nor 'amplitude_spike'."
        )
```

**Analysis:**
- Cross-field validation based on detection_type
- Raises ValueError for invalid combinations
- In v1: ran before validation
- In v2: runs after validation

**Impact:** LOW RISK
- Validation logic still executes
- ValueError still raised for invalid configs
- May need to catch ValidationError instead of ValueError in some contexts
- Actually BETTER in v2: all fields are validated before cross-field checks

### Migration Strategy for __post_init__

**Option 1: Keep as-is (recommended for Noiz)**
- Current __post_init__ methods will work in v2
- Execution order change doesn't break functionality
- Validation errors still raised appropriately

**Option 2: Migrate to @model_validator (if needed in future)**

If more complex validation is needed, convert to explicit validators:

```python
from pydantic import model_validator

@dataclass
class EventDetectionParamsHolder:
    # fields...

    @model_validator(mode='after')
    def validate_detection_params(self) -> Self:
        if self.detection_type == "sta_lta":
            if None in (
                self.n_short_time_average,
                self.n_long_time_average,
                self.trigger_value,
                self.detrigger_value,
            ):
                raise ValueError("StaLta detection requires all parameters")
        return self
```

### Testing __post_init__ Changes

```python
def test_post_init_execution_order():
    # Test field modification after validation
    holder = DatachunkParamsHolder(
        # ... required fields ...
        response_constant_coefficient=1.0,
    )
    # __post_init__ should have modified remove_response
    assert holder.remove_response is False

def test_post_init_validation_errors():
    # Test that validation errors still raise
    with pytest.raises((ValueError, ValidationError)):
        EventDetectionParamsHolder(
            detection_type="sta_lta",
            # Missing required params
        )
```

## Section 10: Error Messages

### v2 Error Reporting Improvements

**ValidationError Structure:**
- More structured error information
- Better context through `ValidationInfo` objects
- Clearer field path reporting for nested models

**TypeError Handling:**
- v1: TypeError in validators wrapped into ValidationError
- v2: TypeError propagates directly (clearer debugging)
- Programming errors no longer masked as validation errors

### Example Error Comparison

**v1 validation error:**
```
ValidationError: 1 validation error for DatachunkParamsHolder
sampling_rate
  field required (type=value_error.missing)
```

**v2 validation error:**
```
ValidationError: 1 validation error for DatachunkParamsHolder
sampling_rate
  Field required [type=missing, input_value={...}, input_type=dict]
```

v2 provides more context: input_value and input_type help debugging.

### Impact on Noiz

**Error Handling in configs.py:**

Current pattern:
```python
try:
    config_type_read = DefinedConfigs(read_value)
except ValueError as e:
    raise ValueError(f"Wrong config_type value...") from e
```

This pattern remains valid in v2.
ValidationError should be caught explicitly if needed:

```python
from pydantic import ValidationError

try:
    validated = DatachunkParamsHolder(**loaded_dict)
except ValidationError as e:
    logger.error(f"Config validation failed: {e}")
    raise
```

**CLI Error Messages:**

Better error messages will help users debug TOML config issues.
Consider catching ValidationError in CLI commands to provide user-friendly messages.

## Key Decisions

### Decision 1: Minimal Migration Approach

**Rationale:**
- Current Noiz dataclasses use simple patterns (type hints + defaults)
- No validators, no Config classes, minimal Field() usage
- Changes required are minimal

**Strategy:**
- Keep existing dataclass structure unchanged
- Update only import statements if needed
- Leverage v2's backward compatibility for dataclasses
- Test thoroughly with existing TOML files

### Decision 2: Retain __post_init__ Pattern

**Rationale:**
- Two dataclasses use __post_init__ for validation
- Execution order change doesn't break functionality
- More explicit than migrating to @model_validator
- Aligns with standard Python dataclass patterns

**Strategy:**
- Keep __post_init__ methods as-is
- Document that they run after validation in v2
- Add tests confirming expected behavior

### Decision 3: Phased Testing Approach

**Rationale:**
- TOML loading is critical path for application
- Need confidence that all configs load correctly
- Existing system tests provide integration coverage

**Strategy:**
1. Unit test dataclass instantiation
2. Test TOML file loading
3. Test __post_init__ behavior
4. Run existing test suite
5. Manual CLI testing

### Decision 4: Documentation Updates

**Rationale:**
- Team needs to understand v2 differences
- Future development should use v2 patterns
- TOML config examples should be validated

**Strategy:**
- Document Optional field semantics
- Document __post_init__ execution order
- Add examples of v2 validator patterns (for future use)
- Validate all example TOML files

### Decision 5: Type Coercion Validation

**Rationale:**
- v2 stricter about float-to-int conversion
- May catch existing issues in TOML configs
- Need to verify numeric types in config files

**Strategy:**
- Review all integer fields in dataclasses
- Check TOML files for accidental floats (e.g., `4.0` for order)
- Add tests for type coercion edge cases

## References

### Official Pydantic v2 Documentation

1. **Migration Guide**: https://docs.pydantic.dev/latest/migration/
   - Comprehensive breaking changes
   - v1 to v2 migration patterns
   - Deprecation warnings

2. **Dataclasses Documentation**: https://docs.pydantic.dev/latest/concepts/dataclasses/
   - v2 dataclass usage
   - ConfigDict patterns
   - Field definitions

3. **Validators Documentation**: https://docs.pydantic.dev/latest/concepts/validators/
   - @field_validator usage
   - @model_validator usage
   - Validation modes

4. **Fields Documentation**: https://docs.pydantic.dev/latest/concepts/fields/
   - Field() parameter reference
   - v2 field constraints

### Additional Resources

5. **bump-pydantic Tool**: https://github.com/pydantic/bump-pydantic
   - Automated migration assistance
   - Code transformation patterns

6. **Pydantic v2 Announcement**: https://docs.pydantic.dev/latest/blog/pydantic-v2/
   - Performance benchmarks
   - Design philosophy

7. **pydantic-core (Rust)**: https://github.com/pydantic/pydantic-core
   - Rust validation implementation
   - Performance characteristics

### Project-Specific Files

8. Noiz dataclass definitions:
   - `/Users/qsbt/noiz-group/noiz/src/noiz/models/processing_params.py`
   - `/Users/qsbt/noiz-group/noiz/src/noiz/models/qc.py`
   - `/Users/qsbt/noiz-group/noiz/src/noiz/models/stacking.py`

9. TOML loading and validation:
   - `/Users/qsbt/noiz-group/noiz/src/noiz/processing/configs.py`

10. Example TOML configs:
    - `/Users/qsbt/noiz-group/noiz/config_examples/`

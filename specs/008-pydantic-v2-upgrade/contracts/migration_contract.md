# Migration Contract: Pydantic v1→v2 Upgrade

**Feature**: 008-pydantic-v2-upgrade
**Date**: 2025-10-25
**Version**: 1.0.0

## Purpose

This contract defines the guarantees and constraints for the pydantic v1→v2 migration to ensure backward compatibility and quality standards.

## API Stability Contract

### Function Signatures

**Guarantee**: All dataclass constructors must remain unchanged

**Details**:
- `DatachunkParams(**kwargs)` signature unchanged
- `CrosscorrelationParams(**kwargs)` signature unchanged
- All other params classes signatures unchanged
- Keyword argument names unchanged
- Default values unchanged

**Validation**: Unit tests instantiating each dataclass must pass without modification

### Return Types

**Guarantee**: Dataclass instances must behave identically to v1

**Details**:
- Field access: `params.field_name` returns same type and value
- Serialization: `asdict(params)` returns same structure
- String representation: `str(params)` format may differ (acceptable)

**Validation**: Integration tests using dataclass instances must pass unchanged

### Import Paths

**Guarantee**: Import statements remain unchanged

**Details**:
- `from pydantic.dataclasses import dataclass` - NO CHANGE
- All downstream imports work identically

**Validation**: No import errors after upgrade

## Validation Behavior Contract

### TOML Parsing

**Guarantee**: Existing valid TOML files parse identically or better

**Details**:
- `toml.load()` → `**dict` → `DataclassParams(**dict)` pattern unchanged
- Valid TOML files produce identical dataclass instances
- Invalid TOML files trigger validation errors (may be different errors in v2, but still fail)

**Exception**: If v2's stricter validation rejects previously accepted data, FIX THE TOML FILES (per spec clarification)

**Validation**:
- Load all TOML files in config_examples/ directory
- Verify all valid files parse successfully
- If v2 rejects any files, investigate and fix TOML data (don't relax validation)

### Required vs Optional Fields

**Guarantee**: Field optionality preserved

**Details**:
- Required fields (no default) - still required in v2
- Optional fields (`Optional[T] = None`) - still optional in v2
- Default values preserved

**Validation**: Test instantiation with missing optional fields (should succeed)

### Type Coercion

**Guarantee**: Reasonable type coercion preserved

**Details**:
- String → int/float: Should work if parseable
- Float → int: More strict in v2 (3.0 OK, 3.7 fails)
- None handling: `Optional[T] = None` works identically

**Exception**: Stricter float→int coercion may require TOML fixes

**Validation**:
- Test numeric fields with int and float values
- Fix TOML files if float→int coercion fails (e.g., change 100.0 to 100)

### Nested Dataclasses

**Guarantee**: Nested validation works identically

**Details**:
- Dataclasses containing other dataclasses validate correctly
- Nested field access works identically

**Validation**: Test QC dataclasses with nested structures

## Performance Contract

### Validation Speed

**Guarantee**: Test execution time must not increase by more than 5%

**Details**:
- Baseline: Record `just unit_tests` execution time before upgrade
- Post-upgrade: Measure `just unit_tests` execution time
- Threshold: ≤105% of baseline

**Expected Outcome**: Performance should IMPROVE (v2 is faster due to rust core)

**Validation**: Compare baseline vs post-upgrade test execution times

### Memory Usage

**Guarantee**: No significant memory regression

**Details**:
- Validation does not materially increase memory usage
- No memory leaks introduced

**Validation**: Observe test execution memory usage (informal check)

## Testing Contract

### Test Pass Rate

**Guarantee**: 100% of existing unit tests pass without modification

**Details**:
- No changes to test expectations
- No changes to test assertions
- Test fixture setup unchanged (if fixtures create dataclasses)

**Exception**: Test execution time measurement tests may need baseline updates

**Validation**: `just unit_tests` returns exit code 0 with no test modifications

### Coverage

**Guarantee**: Code coverage maintained or improved

**Details**:
- Coverage percentage unchanged
- No new uncovered code paths introduced

**Validation**: pytest-cov report shows maintained coverage

### System Tests

**Guarantee**: CLI and API system tests pass

**Details**:
- All `@pytest.mark.cli` tests pass
- All `@pytest.mark.api` tests pass
- End-to-end TOML config loading via CLI works

**Validation**: `just run_system_tests` passes (if available)

## TOML Compatibility Contract

### File Format

**Guarantee**: TOML file format unchanged

**Details**:
- Existing TOML files remain valid format
- No structural changes required
- Users don't need to modify their TOML files (unless v2 catches data quality issues)

**Exception**: If v2 validation is stricter, fix TOML files to meet standards (per spec clarification)

**Validation**: All TOML files in config_examples/ load successfully

### Error Messages

**Guarantee**: Validation errors are clear and actionable

**Details**:
- v2 error messages may differ from v1 (acceptable, often better)
- Error messages must not regress in clarity
- Users can understand and fix validation errors

**Validation**: Trigger intentional validation errors and verify error messages are clear

## Type Checking Contract

### mypy Compliance

**Guarantee**: Zero new mypy type errors

**Details**:
- `just mypy` passes with no additional errors
- No new `# type: ignore` comments required
- Existing type hints remain valid

**Exception**: If v2 reveals latent type errors, fix them (don't suppress with ignores)

**Validation**: `just mypy` returns exit code 0

### Type Annotations

**Guarantee**: All type hints remain valid

**Details**:
- `Optional[T]` annotations work correctly with v2 semantics
- Required field types unchanged
- Forward references (if any) resolve correctly

**Validation**: mypy validates all type annotations

## Error Message Contract

### Validation Errors

**Guarantee**: Error messages do not regress in clarity

**Details**:
- v2 error messages expected to be better (show input_value, input_type)
- Users can understand what went wrong
- Error messages reference correct field names

**Exception**: Error message format may change (acceptable, not a breaking change)

**Validation**: Manual inspection of validation errors during testing

### Exception Types

**Guarantee**: Exception types remain compatible

**Details**:
- `pydantic.ValidationError` still raised for validation failures
- Exception structure may differ (acceptable)
- Catching `ValidationError` still works

**Validation**: Test exception handling code (if any)

## Rollback Contract

### Revert Capability

**Guarantee**: Upgrade can be reverted if critical issues found

**Details**:
- Single atomic commit makes revert straightforward
- `git revert <commit>` restores v1 behavior
- No data migrations required (pydantic not in database)

**Validation**: Document rollback procedure in quickstart.md

### Failure Criteria

**Triggers for Rollback**:
- >5% performance regression that cannot be optimized
- Critical validation behavior differences that cannot be reconciled
- Widespread TOML file incompatibility that cannot be fixed
- Type checking failures that cannot be resolved

**Decision Authority**: Project maintainer

## Compliance Verification

### Pre-Merge Checklist

All items must pass before merging upgrade:

- [ ] `just unit_tests` - 100% pass rate
- [ ] `just mypy` - Zero new errors
- [ ] `just ruff_check_ci` - No new violations
- [ ] `just ruff_format_check` - Formatting correct
- [ ] `just lint_docs` - Docs lint cleanly
- [ ] `just docs` - Docs build successfully
- [ ] All config_examples/*.toml files parse successfully
- [ ] Performance ≤105% of baseline
- [ ] System tests pass (if run)
- [ ] CHANGELOG.rst updated

### Post-Merge Verification

- [ ] Feature 007 (config system refactor) can proceed
- [ ] `uv add "pydantic-settings>=2.0,<3.0"` succeeds
- [ ] CI pipeline passes on main branch

## Version History

- **1.0.0** (2025-10-25): Initial contract definition

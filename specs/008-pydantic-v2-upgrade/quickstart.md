# Quickstart: Pydantic v2 Upgrade

**Feature**: 008-pydantic-v2-upgrade
**Branch**: `008-pydantic-v2-upgrade`
**Date**: 2025-10-25

## Overview

This guide provides a quick reference for implementing the pydantic v1→v2 upgrade. Use this alongside the detailed plan, research, and contracts.

## Pre-Flight Check

Before starting, verify:
- [ ] On branch `008-pydantic-v2-upgrade`
- [ ] All existing tests pass with v1: `just unit_tests`
- [ ] Current pydantic version: `uv pip show pydantic` (should show ~1.10.22)

## Implementation Checklist

### Phase 1: Baseline Recording

- [ ] **1.1 Record test execution time**
  ```bash
  time just unit_tests
  # Record total time for performance comparison
  ```

- [ ] **1.2 Verify test pass rate**
  ```bash
  just unit_tests
  # Should be 100% pass
  ```

### Phase 2: Dependency Upgrade

- [ ] **2.1 Update pyproject.toml**
  - Change line 34: `pydantic ~=1.8` → `pydantic >=2.0,<3.0`

- [ ] **2.2 Sync dependencies**
  ```bash
  uv sync --all-groups
  # Should complete without errors
  ```

- [ ] **2.3 Verify pydantic v2 installed**
  ```bash
  uv pip show pydantic
  # Should show version 2.x
  ```

### Phase 3: Code Review

- [ ] **3.1 Review processing_params.py**
  ```bash
  grep -n "from pydantic" src/noiz/models/processing_params.py
  grep -n "Optional\[" src/noiz/models/processing_params.py
  grep -n "def __post_init__" src/noiz/models/processing_params.py
  ```
  - Verify: Import unchanged (`from pydantic.dataclasses import dataclass`)
  - Verify: All `Optional[T]` fields have `= None` defaults
  - Note: Any `__post_init__` methods for testing

- [ ] **3.2 Review qc.py**
  ```bash
  grep -n "from pydantic" src/noiz/models/qc.py
  grep -n "Optional\[" src/noiz/models/qc.py
  grep -n "def __post_init__" src/noiz/models/qc.py
  ```
  - Same checks as processing_params.py

- [ ] **3.3 Review stacking.py**
  ```bash
  grep -n "from pydantic" src/noiz/models/stacking.py
  grep -n "Optional\[" src/noiz/models/stacking.py
  grep -n "def __post_init__" src/noiz/models/stacking.py
  ```
  - Same checks as processing_params.py

### Phase 4: Initial Testing

- [ ] **4.1 Run unit tests**
  ```bash
  just unit_tests
  ```
  - **Expected**: 100% pass (no code changes needed for simple dataclasses)
  - **If failures**: Check error messages for validation issues

- [ ] **4.2 Check for validation errors**
  - If tests fail with `ValidationError`, investigate:
    - Missing required fields?
    - Type mismatches?
    - Float→int conversion issues?

### Phase 5: TOML Compatibility Testing

- [ ] **5.1 List TOML example files**
  ```bash
  ls -la config_examples/*.toml
  ```

- [ ] **5.2 Test TOML parsing**
  ```bash
  # Test datachunk params
  uv run python -c "
  from noiz.models.processing_params import DatachunkParams
  import toml
  params = DatachunkParams(**toml.load(open('config_examples/datachunk_params.toml')))
  print('DatachunkParams: OK')
  "
  ```
  - Repeat for each TOML file type
  - **Expected**: All files parse successfully
  - **If failures**: Review TOML file for v2 incompatibilities

- [ ] **5.3 Fix TOML files if needed**
  - Common issues:
    - Float values for integer fields (change `100.0` to `100`)
    - Missing required fields
    - Type mismatches
  - Strategy: Fix TOML files to meet v2 standards (don't relax validation)

### Phase 6: Type Checking

- [ ] **6.1 Run mypy**
  ```bash
  just mypy
  ```
  - **Expected**: Zero new type errors
  - **If failures**: Review type annotations for v2 compatibility

- [ ] **6.2 Fix type errors (if any)**
  - Common issues:
    - `Optional[T]` semantics (ensure defaults present)
    - Forward references
    - Stricter type checking

### Phase 7: Linting

- [ ] **7.1 Run ruff check**
  ```bash
  just ruff_check_ci
  ```
  - **Expected**: No new violations

- [ ] **7.2 Run ruff format check**
  ```bash
  just ruff_format_check
  ```
  - **Expected**: Formatting correct

### Phase 8: Documentation

- [ ] **8.1 Update CHANGELOG.rst**
  - Add entry under "Unreleased" section:
  ```rst
  Dependencies
  ~~~~~~~~~~~~

  - Upgraded pydantic from v1.10.22 to v2.x for improved performance and stricter validation
  - No user-facing changes expected; existing TOML configuration files remain compatible
  ```

- [ ] **8.2 Build documentation**
  ```bash
  just docs
  ```
  - **Expected**: Builds successfully

- [ ] **8.3 Lint documentation**
  ```bash
  just lint_docs
  ```
  - **Expected**: No doc8 violations

### Phase 9: Manual CLI Testing

- [ ] **9.1 Test CLI help**
  ```bash
  uv run noiz --help
  uv run noiz configs --help
  ```
  - **Expected**: Commands work identically

- [ ] **9.2 Test config ingestion** (if database available)
  ```bash
  # Test adding a config (requires database)
  uv run noiz configs add_datachunk_params -f config_examples/datachunk_params.toml -p 1
  ```
  - **Expected**: Config ingests successfully

### Phase 10: Performance Verification

- [ ] **10.1 Measure post-upgrade test time**
  ```bash
  time just unit_tests
  # Compare to baseline from Phase 1
  ```

- [ ] **10.2 Calculate performance change**
  - Formula: `(new_time / baseline_time) * 100`
  - **Expected**: ≤105% (may be faster with v2)
  - **If >105%**: Investigate and profile

### Phase 11: System Tests (Optional)

- [ ] **11.1 Run system tests** (if available)
  ```bash
  just run_system_tests
  ```
  - **Expected**: All tests pass

### Phase 12: Final Validation

- [ ] **12.1 Run all quality gates**
  ```bash
  just unit_tests && just mypy && just ruff_check_ci && just ruff_format_check && just lint_docs && just docs
  ```
  - **Expected**: All gates pass

- [ ] **12.2 Verify contracts**
  - Review contracts/migration_contract.md
  - Confirm all guarantees met

### Phase 13: Commit

- [ ] **13.1 Review changes**
  ```bash
  git diff
  ```
  - Should see:
    - pyproject.toml (pydantic version)
    - CHANGELOG.rst (upgrade note)
    - Possibly: Fixed TOML files (if v2 caught issues)

- [ ] **13.2 Commit changes**
  ```bash
  git add pyproject.toml CHANGELOG.rst
  # Add any fixed TOML files if needed
  git commit -m "$(cat <<'EOF'
  feat(deps): Upgrade pydantic from v1 to v2

  Upgrade pydantic dependency from ~=1.8 (v1.10.22) to >=2.0,<3.0 to unblock
  feature 007 (configuration system refactor) which requires pydantic-settings v2.

  Changes:
  - Update pyproject.toml pydantic version constraint
  - Update CHANGELOG.rst with upgrade note
  - [List any TOML file fixes if needed]

  Testing:
  - All unit tests pass (100% pass rate)
  - mypy type checking passes
  - ruff linting passes
  - Documentation builds successfully
  - Performance maintained (≤5% of baseline)
  - All TOML configuration files parse correctly

  No user-facing changes. Existing TOML configuration files remain compatible.

  Generated with Claude Code (https://claude.com/claude-code)

  Co-Authored-By: Claude <noreply@anthropic.com>
  EOF
  )"
  ```

## Common Migration Patterns

### Pattern 1: Simple Dataclass (No Changes Needed)

```python
# This pattern works identically in v1 and v2
from pydantic.dataclasses import dataclass

@dataclass
class SimpleParams:
    required_field: str
    optional_field: Optional[int] = None
    with_default: int = 100
```

**Migration**: NO CODE CHANGES NEEDED

### Pattern 2: `__post_init__` Method

```python
# v1 pattern (still works in v2, execution order changes)
@dataclass
class ParamsWithPostInit:
    field: int

    def __post_init__(self):
        # Runs AFTER validation in v2 (was BEFORE in v1)
        # Functionality preserved
        self.derived_field = self.field * 2
```

**Migration**: NO CODE CHANGES NEEDED (test behavior)

### Pattern 3: Optional Fields

```python
# v1 pattern (works identically in v2)
@dataclass
class ParamsWithOptional:
    required: str
    optional: Optional[str] = None  # Default is critical
```

**Migration**: NO CODE CHANGES NEEDED (verify all Optional fields have defaults)

## Troubleshooting

### Issue: Unit tests fail with ValidationError

**Symptoms**:
```
pydantic.ValidationError: 1 validation error for DatachunkParams
  field_name
    Field required [type=missing, input_value=...]
```

**Solution**:
1. Check if field was previously optional but missing default
2. Add `= None` default to Optional fields
3. Or provide value in TOML file

### Issue: TOML parsing fails with type error

**Symptoms**:
```
ValidationError: 1 validation error for DatachunkParams
  numeric_field
    Input should be a valid integer [type=int_type, input_value=100.5]
```

**Solution**:
1. Check TOML file for float values in integer fields
2. Change `100.0` to `100` in TOML file
3. Or change field type to `float` if decimals are valid

### Issue: mypy reports new type errors

**Symptoms**:
```
error: Incompatible types in assignment [assignment]
```

**Solution**:
1. Review type annotations for v2 compatibility
2. Ensure `Optional[T]` fields have `= None` defaults
3. Fix type mismatches (don't suppress with `# type: ignore`)

### Issue: Performance regression >5%

**Symptoms**: Test execution time increased by more than 5%

**Solution**:
1. Profile test execution to identify bottleneck
2. Check if specific tests are slower (may indicate validation issue)
3. Review v2 migration guide for performance optimization tips
4. Document findings and consider acceptable threshold

### Issue: `__post_init__` behavior changed

**Symptoms**: Logic that depended on pre-validation execution doesn't work

**Solution**:
1. Review research.md Section 9 on execution order
2. Adjust logic to work with post-validation execution
3. Or migrate to `@model_validator(mode='after')` if needed

## Rollback Procedure

If critical issues found:

```bash
# Revert the upgrade commit
git log --oneline  # Find commit hash
git revert <commit-hash>

# Sync dependencies back to v1
uv sync --all-groups

# Verify rollback
uv pip show pydantic  # Should show v1.10.22
just unit_tests  # Should pass
```

## Key Files Reference

| File | Purpose | Changes Expected |
|------|---------|------------------|
| `pyproject.toml` | Dependency specification | Update pydantic version |
| `src/noiz/models/processing_params.py` | Param dataclasses | NO CHANGES (verify) |
| `src/noiz/models/qc.py` | QC dataclasses | NO CHANGES (verify) |
| `src/noiz/models/stacking.py` | Stacking dataclasses | NO CHANGES (verify) |
| `config_examples/*.toml` | Example configs | POSSIBLY fix if v2 rejects |
| `CHANGELOG.rst` | Change documentation | Add upgrade note |

## Success Criteria Checklist

Before considering upgrade complete:

- [ ] SC-001: `uv sync --all-groups` completes without errors
- [ ] SC-002: 100% unit tests pass
- [ ] SC-003: `just mypy` passes with zero new errors
- [ ] SC-004: `just ruff_check_ci` and `just ruff_format_check` pass
- [ ] SC-005: All CI stages would pass (verify locally)
- [ ] SC-006: System tests pass (if run)
- [ ] SC-007: All TOML files parse correctly
- [ ] SC-008: CLI commands work identically
- [ ] SC-009: `just docs` and `just lint_docs` pass
- [ ] SC-010: Performance ≤105% of baseline
- [ ] SC-011: Can run `uv add "pydantic-settings>=2.0,<3.0"` (verify after merge)

## Next Steps

After successful upgrade:

1. Push branch to GitLab
2. Create merge request to `devel`
3. Verify CI pipeline passes
4. Request code review
5. Merge to `devel`
6. Verify feature 007 can proceed with pydantic-settings v2 installation

## Resources

- **Specification**: `spec.md` (requirements and success criteria)
- **Research**: `research.md` (v1→v2 migration patterns)
- **Data Model**: `data-model.md` (affected entities)
- **Contract**: `contracts/migration_contract.md` (guarantees)
- **Plan**: `plan.md` (overall implementation plan)
- **Pydantic v2 Migration Guide**: https://docs.pydantic.dev/latest/migration/
- **Pydantic v2 Dataclasses**: https://docs.pydantic.dev/latest/concepts/dataclasses/

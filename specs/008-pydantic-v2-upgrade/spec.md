# Feature Specification: Pydantic v2 Upgrade

**Feature Branch**: `008-pydantic-v2-upgrade`
**Created**: 2025-10-25
**Status**: Draft
**Input**: User description: "In order to unblock other developments, we need to update pydantic dependency. I would like to update to version 2. All code where pydantic is used needs to be changed to be supporting v2. Ideally, no change from user perspective should be visible."

## User Scenarios & Testing

### User Story 1 - Developer Upgrades Pydantic Dependency (Priority: P1)

A developer needs to upgrade the pydantic dependency from v1 to v2 to enable installation of pydantic-settings v2, which is required for the configuration system refactor (feature 007).

**Why this priority**: This is a blocking prerequisite for feature 007 (configuration system refactor). Without this upgrade, pydantic-settings v2 cannot be installed due to dependency conflicts.

**Independent Test**: Can be fully tested by running `uv sync --all-groups` successfully after updating pyproject.toml to pydantic v2, and verifying all existing unit tests pass without modification to test code.

**Acceptance Scenarios**:

1. **Given** pyproject.toml with pydantic v1 dependency, **When** developer updates to pydantic >=2.0,<3.0, **Then** uv resolves dependencies successfully
2. **Given** updated pydantic v2 dependency, **When** developer runs `uv sync --all-groups`, **Then** installation completes without errors
3. **Given** pydantic v2 installed, **When** developer runs `uv add "pydantic-settings>=2.0,<3.0"`, **Then** pydantic-settings installs without dependency conflicts

---

### User Story 2 - Code Compatibility with Pydantic v2 (Priority: P1)

All existing code using pydantic v1 APIs must work with pydantic v2 without breaking existing functionality or changing user-facing behavior.

**Why this priority**: Core requirement for backward compatibility. Any breaking changes would affect data ingestion, validation, and processing pipelines.

**Independent Test**: Can be fully tested by running the complete unit test suite (`just unit_tests`) and verifying 100% pass rate with no modifications to test expectations.

**Acceptance Scenarios**:

1. **Given** processing_params.py using pydantic.dataclasses, **When** code is updated for v2 compatibility, **Then** all DatachunkParams, CrosscorrelationParams, etc. continue to validate correctly
2. **Given** qc.py using pydantic.dataclasses, **When** code is updated for v2 compatibility, **Then** QC validation continues to work identically
3. **Given** stacking.py using pydantic.dataclasses, **When** code is updated for v2 compatibility, **Then** stacking parameter validation continues to work identically
4. **Given** existing TOML configuration files, **When** parsed by updated pydantic v2 code, **Then** all configurations load and validate as before
5. **Given** CLI commands using pydantic models, **When** executed after upgrade, **Then** all commands function identically with same inputs/outputs

---

### User Story 3 - CI Verification (Priority: P1)

All CI pipeline stages must pass with pydantic v2, including unit tests, type checking, linting, documentation builds, and system tests.

**Why this priority**: Ensures no regressions across the entire codebase and validates that the upgrade is safe for production deployment.

**Independent Test**: Can be fully tested by pushing the branch and observing all GitLab CI stages pass (testing, linting, documentation, system-testing).

**Acceptance Scenarios**:

1. **Given** updated code with pydantic v2, **When** CI runs `just unit_tests`, **Then** all tests pass with maintained coverage
2. **Given** updated code with pydantic v2, **When** CI runs `just mypy`, **Then** type checking passes without new errors
3. **Given** updated code with pydantic v2, **When** CI runs `just ruff_check_ci`, **Then** linting passes without new violations
4. **Given** updated code with pydantic v2, **When** CI runs `just docs`, **Then** documentation builds successfully
5. **Given** updated code with pydantic v2, **When** CI runs system tests, **Then** all CLI and API system tests pass

---

### Edge Cases

- **TOML validation strictness**: If v2 rejects TOML configuration data that v1 accepted, fix the TOML configuration files to meet v2 validation standards (embrace stricter validation for improved type safety)
- How does the system handle dataclass inheritance patterns that changed between v1 and v2?
- What happens if mypy type checking reveals type errors due to stricter v2 type annotations?
- How does the upgrade affect serialization/deserialization of existing database records?
- What happens if existing test fixtures rely on v1-specific behavior?

## Clarifications

### Session 2025-10-25

- Q: Pydantic v2 has stricter validation by default. If v2 rejects TOML configuration data that v1 previously accepted during upgrade testing, what should the implementation strategy be? → A: Fix the TOML configuration files to meet v2 validation standards (recommended approach - embrace stricter validation)
- Q: NFR-001 states "Upgrade MUST NOT introduce performance regressions in validation or data loading" but no baseline or threshold is defined. How should performance be measured and what constitutes acceptable performance? → A: Test execution time unchanged (monitor that `just unit_tests` execution time does not increase by more than 5%)

## Requirements

### Functional Requirements

- **FR-001**: System MUST upgrade pydantic dependency to version >=2.0,<3.0 in pyproject.toml
- **FR-002**: System MUST update all imports from `pydantic.dataclasses` to use pydantic v2 API
- **FR-003**: System MUST preserve all existing validation behavior in processing_params.py for DatachunkParams, CrosscorrelationParams, BeamformingParams, PPSDParams, and StackingParams
- **FR-004**: System MUST preserve all existing validation behavior in qc.py for QC-related dataclasses
- **FR-005**: System MUST preserve all existing validation behavior in stacking.py for stacking-related dataclasses
- **FR-006**: System MUST maintain compatibility with existing TOML configuration files; if v2's stricter validation rejects previously accepted data, fix TOML files to meet v2 standards
- **FR-007**: System MUST pass all existing unit tests without changing test expectations or assertions
- **FR-008**: System MUST pass mypy type checking without introducing new type errors
- **FR-009**: System MUST maintain API compatibility - no changes to function signatures, class constructors, or return types visible to users

### Non-Functional Requirements

- **NFR-001**: Upgrade MUST NOT introduce performance regressions in validation or data loading; test execution time (`just unit_tests`) must not increase by more than 5%
- **NFR-002**: Upgrade MUST NOT change user-facing error messages (unless improving clarity)
- **NFR-003**: Upgrade MUST be completed in a single atomic commit (or minimal commits) following project constitution

### Key Entities

- **DatachunkParams**: Configuration for datachunk processing stage (in processing_params.py)
- **CrosscorrelationCartesianParams**: Configuration for Cartesian cross-correlation (in processing_params.py)
- **CrosscorrelationCylindricalParams**: Configuration for cylindrical cross-correlation (in processing_params.py)
- **BeamformingParams**: Configuration for beamforming analysis (in processing_params.py)
- **PPSDParams**: Configuration for Power Spectral Density calculations (in processing_params.py)
- **StackingParams**: Configuration for time-domain stacking (in stacking.py)
- **QC-related dataclasses**: Quality control validation models (in qc.py)

## Success Criteria

### Measurable Outcomes

- **SC-001**: Dependency resolution succeeds - `uv sync --all-groups` completes without errors with pydantic >=2.0,<3.0
- **SC-002**: Unit tests pass - 100% of existing unit tests pass without modification to test expectations
- **SC-003**: Type checking passes - `just mypy` completes with zero new type errors
- **SC-004**: Linting passes - `just ruff_check_ci` and `just ruff_format_check` pass without new violations
- **SC-005**: CI pipeline passes - All GitLab CI stages (testing, linting, documentation, system-testing) pass
- **SC-006**: System tests pass - All CLI and API system tests execute successfully with pydantic v2
- **SC-007**: Configuration compatibility - Existing TOML configuration files load and validate identically
- **SC-008**: No user-visible changes - CLI commands produce identical outputs with same inputs
- **SC-009**: Documentation builds - `just docs` and `just lint_docs` complete successfully
- **SC-010**: Performance maintained - Test execution time (`just unit_tests`) does not increase by more than 5% compared to pre-upgrade baseline
- **SC-011**: Unblocks feature 007 - After merge, developer can successfully run `uv add "pydantic-settings>=2.0,<3.0"`

## Technical Context

### Current State

**Dependency Version**:
```toml
# pyproject.toml line 34
pydantic ~=1.8
```

**Installed Version**: pydantic 1.10.22 (latest in v1.x line)

**Files Using Pydantic** (3 total):
1. `src/noiz/models/processing_params.py` - Uses `from pydantic.dataclasses import dataclass`
2. `src/noiz/models/qc.py` - Uses `from pydantic.dataclasses import dataclass`
3. `src/noiz/models/stacking.py` - Uses `from pydantic.dataclasses import dataclass`

### Blocking Issue

**Problem**: Feature 007 (configuration system refactor) requires pydantic-settings >=2.0,<3.0, which depends on pydantic >=2.0. Current project has pydantic ~=1.8.

**Error**:
```
× No solution found when resolving dependencies:
  Because your project depends on pydantic>=1.8,<2.dev0 and pydantic-settings>=2.0,
  we can conclude that your project's requirements are unsatisfiable.
```

### Migration Strategy

**Pydantic v1 → v2 Breaking Changes to Address**:

1. **Import changes**:
   - v1: `from pydantic.dataclasses import dataclass`
   - v2: `from pydantic.dataclasses import dataclass` (import path unchanged, but behavior may differ)

2. **Config class changes**:
   - v1: Nested `Config` class with attributes
   - v2: `model_config` attribute with `ConfigDict` (for BaseModel) or config dict (for dataclasses)

3. **Validation behavior**:
   - v2 has stricter validation by default
   - May need `model_rebuild()` calls if forward references used
   - Validator decorators may have changed signature

4. **Field definitions**:
   - v1: `Field()` with certain parameters
   - v2: Some Field() parameters renamed or removed

5. **Type annotations**:
   - v2 stricter about Optional vs required fields
   - May need explicit `= None` defaults

### Migration Steps

1. **Record baseline**: Run `just unit_tests` and record execution time for performance comparison
2. **Update pyproject.toml**: Change `pydantic ~=1.8` to `pydantic >=2.0,<3.0`
3. **Sync dependencies**: Run `uv sync --all-groups`
4. **Review dataclass usage**: Check all 3 files for v1-specific patterns
5. **Update Config classes**: If any `Config` nested classes exist, convert to v2 pattern
6. **Update validators**: If any custom validators exist, update to v2 decorator syntax
7. **Run tests**: Execute `just unit_tests` and fix any validation failures
8. **Fix TOML files**: If v2 rejects TOML configs that v1 accepted, fix the TOML files to meet v2 validation standards
9. **Run type checking**: Execute `just mypy` and fix any type errors
10. **Run linting**: Execute `just ruff_check_ci` and `just ruff_format_check`
11. **Test CLI commands**: Run sample CLI commands to verify behavior unchanged
12. **Verify performance**: Compare test execution time to baseline (must not exceed +5%)
13. **Commit**: Single atomic commit with message like `feat(deps): Upgrade pydantic from v1 to v2`

## Risks and Mitigation

### Risk 1: Validation Behavior Changes

**Risk**: Pydantic v2 has stricter validation that may reject previously accepted data.

**Likelihood**: Medium - v2 is stricter by default

**Impact**: High - Could break existing workflows

**Mitigation**:
- Run full unit test suite to catch validation changes
- Test with real TOML configuration files from `config_examples/`
- Fix TOML files to meet v2 validation standards (embrace stricter validation for improved type safety)

### Risk 2: Type Checking Failures

**Risk**: Pydantic v2 has stricter type annotations that may reveal latent type errors.

**Likelihood**: Medium - mypy may report new errors

**Impact**: Medium - May require type annotation fixes

**Mitigation**:
- Run `just mypy` early in migration
- Fix type errors incrementally
- Use `# type: ignore` only if necessary with explanation comment

### Risk 3: Hidden Dependencies on v1 Behavior

**Risk**: Code may rely on undocumented v1 behavior that changed in v2.

**Likelihood**: Low - Limited pydantic usage (3 files)

**Impact**: High - Could cause subtle bugs

**Mitigation**:
- Run system tests (`just run_system_tests`) to verify CLI behavior
- Manual testing of representative workflows
- Review pydantic v2 migration guide for breaking changes

### Risk 4: Performance Regression

**Risk**: Pydantic v2 performance characteristics may differ from v1.

**Likelihood**: Low - v2 generally faster than v1

**Impact**: Low - Validation is not a bottleneck in noiz

**Mitigation**:
- Record baseline test execution time before upgrade (`just unit_tests`)
- Verify post-upgrade execution time does not increase by more than 5%
- If regression exceeds threshold, profile and optimize

## Documentation Requirements

### Code Documentation

- Update docstrings in affected files if validation behavior changes
- Add migration notes in comments if v2-specific patterns used

### User Documentation

- **No user documentation required** - This is an internal dependency upgrade with no user-visible changes

### Developer Documentation

- Update CHANGELOG.rst with pydantic v2 upgrade note
- Add note to CLAUDE.md about pydantic v2 usage (if needed)

## Testing Strategy

### Unit Tests

- Record baseline execution time before upgrade
- Run full test suite: `just unit_tests`
- Expected result: 100% pass rate with no test modifications
- Coverage must be maintained
- Post-upgrade execution time must not increase by more than 5%

### Type Checking

- Run `just mypy`
- Expected result: Zero new type errors
- Fix any errors revealed by stricter v2 type checking

### Linting

- Run `just ruff_check_ci`
- Run `just ruff_format_check`
- Expected result: No new violations

### System Tests

- Run `just run_system_tests`
- Expected result: All CLI and API tests pass
- Validates end-to-end behavior unchanged

### Manual Testing

1. Test TOML config loading:
   ```bash
   # Verify existing config files still parse correctly
   uv run python -c "from noiz.models.processing_params import DatachunkParams; import toml; DatachunkParams(**toml.load(open('config_examples/datachunk_params.toml')))"
   ```

2. Test CLI command:
   ```bash
   # Verify CLI still works
   uv run noiz --help
   uv run noiz configs --help
   ```

### CI Verification

- Push branch and verify all CI stages pass:
  - testing: unit tests
  - linting: ruff, mypy, doc8
  - documentation: Sphinx build
  - system-testing: CLI/API tests

## Dependencies and Blockers

### Prerequisites

- None - This is a prerequisite for other features

### Blocked Features

- **Feature 007** (configuration system refactor): Cannot add pydantic-settings v2 until this upgrade completes

### Follow-up Work

- After merge to devel, feature 007 implementation can proceed with Task T001 (add pydantic-settings dependency)

## Open Questions

None. Upgrade path is well-documented in pydantic v2 migration guide.

## References

- [Pydantic v2 Migration Guide](https://docs.pydantic.dev/latest/migration/)
- [Pydantic v2 Dataclasses](https://docs.pydantic.dev/latest/concepts/dataclasses/)
- [Pydantic v2 Validators](https://docs.pydantic.dev/latest/concepts/validators/)
- Project constitution: `/Users/qsbt/noiz-group/noiz/specs/CONSTITUTION.md`
- Blocked feature: `/Users/qsbt/noiz-group/noiz/specs/007-config-system-refactor/spec.md`

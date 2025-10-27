# Quickstart: Configuration System Refactor

**Feature**: Configuration System Refactor
**Branch**: `007-config-system-refactor`
**Date**: 2025-10-25

## Overview

This guide provides a quick reference for implementing the configuration system refactor. Use this alongside the detailed plan, research, and contracts.

## Implementation Checklist

### Phase 1: Core Configuration Module

- [ ] **1.1 Add pydantic-settings dependency**
  - Command: `uv add "pydantic-settings>=2.0,<3.0"`
  - Verify: `uv sync --all-groups`

- [ ] **1.2 Create `src/noiz/config.py`**
  - Implement `NoizConfig(BaseSettings)`
  - Implement `DatabaseConfig(BaseModel)` nested
  - Implement `ProcessingConfig(BaseModel)` nested
  - Add `get_config(reload: bool = False)` function
  - See: `research.md` sections 1-4 for patterns
  - Contract: `contracts/config_module.md`

- [ ] **1.3 Add validation logic**
  - Database: URI precedence, backend requirements
  - Processing: directory validation (auto-create, file/non-empty checks)
  - Add structured logging (INFO/WARN/ERROR)
  - Mask sensitive fields (SecretStr for passwords)

- [ ] **1.4 Write unit tests** (`tests/unit/test_config.py`)
  - Valid PostgreSQL config (DATABASE_URL and POSTGRES_*)
  - Valid SQLite config
  - URI precedence behavior
  - Directory validation scenarios
  - Missing required fields
  - Secret masking in repr/logs

### Phase 2: Flask Integration

- [ ] **2.1 Update `src/noiz/app.py`**
  - Import `get_config()` from `noiz.config`
  - Call `get_config(reload=True)` in `create_app()` (supports notebook pattern)
  - Use `config.to_flask_config()` to populate `app.config`
  - Store config in `app.config['NOIZ_CONFIG']` for direct access
  - Remove old `app.config.from_object("noiz.settings")` call

- [ ] **2.2 Write Flask integration tests** (`tests/integration/test_config_integration.py`)
  - Test `create_app()` with valid config
  - Test config reload between `create_app()` calls
  - Test Flask config keys populated correctly
  - Test database connection using config

### Phase 3: Backward Compatibility

- [ ] **3.1 Update `src/noiz/settings.py`**
  - Import `get_config()` from `noiz.config`
  - Expose module-level variables for backward compat
  - Example: `PROCESSED_DATA_DIR = str(get_config().processing.processed_data_dir)`
  - Keep all existing variable names
  - Remove old `environs` usage
  - Remove old environment variable loading logic

- [ ] **3.2 Update `src/noiz/database.py`**
  - Remove environment variable reading for database config
  - Config now comes from Flask app context
  - Keep dialect-specific logic (get_dialect_insert, etc.)

- [ ] **3.3 Update `src/noiz/globals.py`**
  - Review `PROCESSED_DATA_DIR` proxy pattern
  - May need updates for new config system
  - Ensure pytest fixture pattern still works

- [ ] **3.4 Test backward compatibility**
  - Verify `from noiz.settings import PROCESSED_DATA_DIR` works
  - Verify all existing imports still function
  - Run full test suite

### Phase 4: Test Infrastructure

- [ ] **4.1 Update `tests/conftest.py`**
  - Update environment variable names to use `NOIZ_` prefix
  - Example: `PROCESSED_DATA_DIR` → `NOIZ_PROCESSED_DATA_DIR`
  - Update pytest fixtures that set env vars
  - Ensure test isolation preserved

- [ ] **4.2 Run full test suite**
  - Command: `just unit_tests`
  - Fix any breakages
  - Ensure coverage maintained

### Phase 5: CI Updates

- [ ] **5.1 Update `.gitlab-ci.yml`**
  - Replace env var names with `NOIZ_` prefix
  - Update all stages: testing, linting, documentation, system-testing
  - Example: `DATABASE_URL` → `NOIZ_DATABASE_URL`

- [ ] **5.2 Update GitLab CI templates**
  - Update `.gitlab/templates/linting.yml`
  - Update `.gitlab/templates/documentation.yml`
  - Check other templates for env var usage

- [ ] **5.3 Test CI pipeline**
  - Push to branch and verify all CI stages pass
  - Check logs for proper config loading
  - Verify no password leaks in logs

### Phase 6: Documentation

- [ ] **6.1 Create RST documentation**
  - Create `docs/content/user_guide/configuration.rst` (or appropriate path)
  - Follow structure in `spec.md` "Documentation Requirements" section
  - Include:
    - Overview of config system
    - Required vs optional variables
    - Database backend options
    - `.env` file guide with shell commands
    - Flask integration for notebooks
    - CI/CD configuration guide
    - Migration guide from old env var names

- [ ] **6.2 Add documentation to Sphinx toctree**
  - Update appropriate `index.rst` to include new config doc

- [ ] **6.3 Build and validate documentation**
  - Command: `just docs`
  - Command: `just lint_docs`
  - Fix any warnings/errors
  - Review rendered HTML

### Phase 7: Quality Gates

- [ ] **7.1 Type checking**
  - Command: `just mypy`
  - Fix any type errors

- [ ] **7.2 Linting**
  - Command: `just ruff_check_ci`
  - Command: `just ruff_format_check`
  - Fix any linting issues

- [ ] **7.3 Documentation linting**
  - Command: `just lint_docs`
  - Fix any doc8 warnings

- [ ] **7.4 Full test suite**
  - Command: `just unit_tests`
  - Verify 100% pass rate
  - Check coverage maintained

- [ ] **7.5 System tests** (optional but recommended)
  - Command: `just run_system_tests`
  - Verify CLI and API functionality

### Phase 8: Commit Strategy

Recommended incremental commits (one per):

1. `feat(config): Add pydantic-settings dependency`
2. `feat(config): Create config module with NoizConfig, DatabaseConfig, ProcessingConfig`
3. `feat(config): Add config validation logic and structured logging`
4. `test(config): Add unit tests for config module`
5. `feat(app): Integrate new config system with Flask app factory`
6. `test(config): Add Flask integration tests`
7. `refactor(settings): Update settings.py for backward compatibility`
8. `refactor(database): Remove env var loading from database.py`
9. `test(config): Update test fixtures to use NOIZ_ prefix`
10. `ci: Update GitLab CI to use NOIZ_* environment variables`
11. `ci: Update CI templates with new env var names`
12. `docs(config): Add configuration system documentation`
13. `docs(config): Add .env guide and migration instructions`

## Common Patterns

### Pattern 1: Access Config in Application Code

```python
# Option 1: From Flask app context (preferred in web/CLI)
from flask import current_app

config = current_app.config['NOIZ_CONFIG']
data_dir = config.processing.processed_data_dir

# Option 2: Direct (for library usage, testing)
from noiz.config import get_config

config = get_config()
db_uri = config.database.sqlalchemy_database_uri.get_secret_value()
```

### Pattern 2: Flask Integration (Notebook)

```python
# In notebook cell
from noiz.app import create_app

# Create Flask app with config
app = create_app()

# Access within app context
with app.app_context():
    from noiz.database import db
    # ... use database, models, etc.
```

### Pattern 3: Testing with Custom Config

```python
# In test file
import pytest
from noiz.config import NoizConfig

@pytest.fixture
def custom_config(tmp_path, monkeypatch):
    """Test config with SQLite."""
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()

    monkeypatch.setenv("NOIZ_DATABASE_BACKEND", "sqlite")
    monkeypatch.setenv("NOIZ_DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("NOIZ_PROCESSED_DATA_DIR", str(processed_dir))
    monkeypatch.setenv("NOIZ_MSEEDINDEX_EXECUTABLE", "mseedindex")

    return NoizConfig()

def test_something(custom_config):
    assert custom_config.database.backend == "sqlite"
```

### Pattern 4: CLI with Config

```python
# CLI commands automatically get config via Flask app context
# No changes needed to cli.py - it uses create_app()

# In CLI command implementation:
@click.command()
def process_data():
    """Process seismic data."""
    from flask import current_app
    config = current_app.config['NOIZ_CONFIG']

    # Use config values
    output_dir = config.processing.processed_data_dir
    # ...
```

## Troubleshooting

### Issue: Config validation fails with "field required"

**Solution**: Check environment variables are set with `NOIZ_` prefix
```bash
# Wrong
export PROCESSED_DATA_DIR=/path/to/data

# Correct
export NOIZ_PROCESSED_DATA_DIR=/path/to/data
```

### Issue: Password visible in logs

**Solution**: Ensure using `SecretStr` type and custom `__repr__`
- Check `config.py` uses `SecretStr` for password fields
- Check `to_flask_config()` masks sensitive fields

### Issue: Tests fail with config errors

**Solution**: Update test fixtures to use `NOIZ_` prefix
- Check `conftest.py` for env var setup
- Use `monkeypatch.setenv()` in test fixtures

### Issue: Flask app can't find config

**Solution**: Ensure `create_app()` calls `get_config()`
- Check `app.py` imports and calls `get_config()`
- Check `app.config.from_mapping(config.to_flask_config())`

### Issue: Backward compat imports fail

**Solution**: Check `settings.py` exposes all variables
- Verify `get_config()` called in settings.py
- Verify all original variable names exposed

## Key Files Reference

| File | Purpose | Phase |
|------|---------|-------|
| `src/noiz/config.py` | Core config module | Phase 1 |
| `src/noiz/settings.py` | Backward compat | Phase 3 |
| `src/noiz/app.py` | Flask integration | Phase 2 |
| `src/noiz/database.py` | Database config (cleanup) | Phase 3 |
| `src/noiz/globals.py` | Globals review | Phase 3 |
| `tests/unit/test_config.py` | Config unit tests | Phase 1 |
| `tests/integration/test_config_integration.py` | Flask integration tests | Phase 2 |
| `tests/conftest.py` | Test fixtures | Phase 4 |
| `.gitlab-ci.yml` | CI pipeline | Phase 5 |
| `.gitlab/templates/*.yml` | CI templates | Phase 5 |
| `docs/content/user_guide/configuration.rst` | User documentation | Phase 6 |

## Next Steps

After completing all phases:

1. Run full test suite: `just unit_tests`
2. Run type checking: `just mypy`
3. Run linting: `just ruff_check_ci && just ruff_format_check`
4. Build docs: `just docs && just lint_docs`
5. Test CI pipeline (push to branch)
6. Run system tests: `just run_system_tests` (if available)
7. Create pull/merge request with summary of changes

## Resources

- **Detailed Plan**: `plan.md`
- **Research**: `research.md` (pydantic-settings patterns)
- **Data Model**: `data-model.md` (config structure)
- **API Contract**: `contracts/config_module.md`
- **Feature Spec**: `spec.md` (requirements and success criteria)

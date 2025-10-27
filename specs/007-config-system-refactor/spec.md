# Feature Specification: Configuration System Refactor

**Feature Branch**: `007-config-system-refactor`
**Created**: 2025-10-25
**Status**: Draft
**Input**: User description: "Currently, config variables of Noiz are managed through a mess of environmental variables. It's not that those env vars are a problem but rather the part of how they are ingested, held and passed around through the application. They are not in a single place (noiz.settings and noiz.db contain some of them) and there is no clear documentation of what they are used for. I would like to refactor it to make it more sane. Ideally, I would like to use something like pydantic-settings for pulling values from environment and whole management. All values should be contained within a single config. all environment values should be prepended by NOIZ_ to distinguish them from others - this can be accomplished with a setting in pydantic-settings. Make sure that all the env vars you find being fetched are collected but then also verify whether all those variables are used. if they are not, mark them for deprecation. The migration path, don't worry about it. I am in contact with all the users and also, we are still in zero ver. We can do breaking changes like this.

IMPORTANT CONTEXT: Make sure that this nicely plays with Flask and that part of noiz. Noiz is currently mostly used as a CLI application, or from a notebook as an interface with use of flask's create_app to handle all the configs and database connections. Hence, make sure to preserve it."

## Clarifications

### Session 2025-10-25

- Q: When both `NOIZ_DATABASE_URL` and individual `NOIZ_POSTGRES_*` variables are provided, which should take precedence? → A: `NOIZ_DATABASE_URL` takes precedence if set (URI-first)
- Q: When `NOIZ_PROCESSED_DATA_DIR` points to a non-existent directory, should the system auto-create it or fail with a clear error? → A: Auto-create if missing (warn if parent directories also created); if path exists as file, fail with error; if path exists as non-empty directory, fail with error (prevents naming conflicts from reprocessing same data); if path exists as empty directory, continue without warning
- Q: Should passwords and secrets (e.g., `NOIZ_POSTGRES_PASSWORD`, database URIs with passwords) be masked in logs and config display to prevent accidental credential exposure? → A: Mask all passwords/secrets in logs and config display (show as ******)
- Q: What version range should be specified for the pydantic-settings dependency to ensure compatibility and stability? → A: Pin to v2.x range (e.g., `pydantic-settings>=2.0,<3.0`) allowing minor updates
- Q: Should the configuration validation process emit structured logs (with log levels and context) for observability, or keep logging minimal to reduce startup noise? → A: Structured logging - INFO for success, WARN for warnings, ERROR for failures

## User Scenarios & Testing *(mandatory)*

### User Story 1 - CLI User Configures Application (Priority: P1)

A data scientist uses Noiz via CLI commands to process seismic data.
They need to configure database connection, data directories, and processing tools through environment variables.
The configuration must be validated at startup with clear error messages if required values are missing.

**Why this priority**: This is the primary usage pattern for Noiz. Without working CLI configuration, the application is unusable.

**Independent Test**: Can be fully tested by running any CLI command (e.g., `noiz processing prepare_datachunks`) with valid NOIZ_* environment variables and verifying it executes successfully with proper error messages for invalid/missing configurations.

**Acceptance Scenarios**:

1. **Given** a user sets `NOIZ_POSTGRES_HOST`, `NOIZ_POSTGRES_PORT`, `NOIZ_POSTGRES_USER`, `NOIZ_POSTGRES_PASSWORD`, `NOIZ_POSTGRES_DB`, `NOIZ_PROCESSED_DATA_DIR`, and `NOIZ_MSEEDINDEX_EXECUTABLE` environment variables, **When** they run any Noiz CLI command, **Then** the application starts successfully and uses these configuration values
2. **Given** a user sets only `NOIZ_DATABASE_URL` (PostgreSQL URI) instead of individual NOIZ_POSTGRES_* variables, **When** they run a CLI command, **Then** the application accepts this alternative configuration method
3. **Given** a user omits a required configuration variable like `NOIZ_PROCESSED_DATA_DIR`, **When** they run a CLI command, **Then** the application fails immediately with a clear error message identifying the missing variable
4. **Given** a user provides invalid configuration (e.g., invalid directory path), **When** they run a CLI command, **Then** the application validates the configuration and provides actionable error messages

---

### User Story 2 - Notebook User Initializes Flask App (Priority: P1)

A researcher uses Noiz from Jupyter notebooks by calling `create_app()` to initialize the Flask application context.
They need the configuration system to work seamlessly with Flask's app factory pattern, loading all settings from environment variables.
The Flask app must be properly configured for database connections and have all Noiz settings accessible.

**Why this priority**: Notebook usage via Flask app factory is a critical usage pattern. Breaking this would prevent programmatic access to Noiz functionality.

**Independent Test**: Can be fully tested by creating a notebook that calls `from noiz.app import create_app; app = create_app()` and verifying all configuration is loaded and database connection works within the app context.

**Acceptance Scenarios**:

1. **Given** a notebook user sets environment variables with NOIZ_ prefix, **When** they call `create_app()`, **Then** Flask app is initialized with all Noiz configuration loaded and accessible
2. **Given** a notebook user has a Flask app context active, **When** they access database models or processing functions, **Then** all configuration values are available without re-reading environment variables
3. **Given** a notebook user changes environment variables after initial import, **When** they call `create_app()` again, **Then** the new configuration values are picked up (preserving current reload behavior)
4. **Given** a notebook user wants to inspect current configuration, **When** they access the config object, **Then** they can see all active settings with sensitive values masked (passwords shown as ******)

---

### User Story 3 - Developer Documents Available Configuration (Priority: P2)

A new developer or system administrator needs to understand what configuration options are available and what each one does.
They need comprehensive documentation of all environment variables, their purposes, types, defaults, and whether they are required.

**Why this priority**: Documentation improves onboarding and reduces configuration errors, but existing users already know the current variables.

**Independent Test**: Can be fully tested by reviewing generated documentation or config class docstrings and verifying all environment variables are documented with clear descriptions.

**Acceptance Scenarios**:

1. **Given** a developer examines the configuration module, **When** they read the code or docstrings, **Then** they find clear documentation for each configuration variable including its purpose, type, default value, and whether it's required
2. **Given** a system administrator needs to deploy Noiz, **When** they review configuration documentation, **Then** they can identify all required NOIZ_* environment variables and understand what values to provide
3. **Given** a developer wants to add a new configuration option, **When** they examine the config system, **Then** they find clear patterns and examples for adding new validated settings

---

### User Story 4 - CI Pipeline Uses New Configuration (Priority: P1)

The CI/CD pipeline runs automated tests, linting, documentation builds, and system tests.
It must be updated to use the new `NOIZ_` prefixed environment variables in all stages.
All CI jobs must pass with the new configuration system.

**Why this priority**: Without working CI, the migration cannot be verified and merged. CI is critical infrastructure.

**Independent Test**: Can be fully tested by running the full CI pipeline (all stages) and verifying all jobs pass with new NOIZ_* environment variables configured.

**Acceptance Scenarios**:

1. **Given** CI configuration is updated with `NOIZ_` prefixed variables, **When** the CI pipeline runs unit tests, **Then** all tests pass successfully
2. **Given** CI configuration uses new variables, **When** system tests run, **Then** they execute successfully with proper database and data directory configuration
3. **Given** CI uses new configuration, **When** documentation build stage runs, **Then** Sphinx builds successfully
4. **Given** CI environment variables are updated, **When** any stage requires database connection, **Then** it connects successfully using NOIZ_DATABASE_URL or NOIZ_POSTGRES_* variables

---

### User Story 5 - User Reads Configuration Documentation (Priority: P2)

A new user or system administrator needs to configure Noiz for the first time.
They need clear documentation that explains what configuration is required, what is optional, and how to set up a `.env` file for local development.
The documentation should provide concrete examples and shell commands.

**Why this priority**: Good documentation reduces support burden and improves user onboarding, but existing users already know configuration.

**Independent Test**: Can be fully tested by following the documentation from scratch to set up Noiz configuration and verifying a new user can get started in under 15 minutes.

**Acceptance Scenarios**:

1. **Given** a user reads the configuration documentation, **When** they review the "Required Variables" section, **Then** they understand exactly which environment variables must be set to run Noiz
2. **Given** a user wants to use `.env` file for local development, **When** they follow the documentation, **Then** they find example `.env` file content and instructions on how to use it
3. **Given** a user reads the `.env` documentation, **When** they look for shell commands, **Then** they find commands like `set -a; source .env; set +a` or recommendations for tools like direnv
4. **Given** a user reviews configuration options, **When** they distinguish between basic and advanced config, **Then** they can identify minimal required config versus optional performance/logging settings
5. **Given** a user examines a config variable in documentation, **When** they read its description, **Then** they understand its purpose, type, default value (if any), and whether it's required

---

### User Story 6 - System Identifies Unused Environment Variables (Priority: P3)

A developer maintaining Noiz wants to clean up deprecated or unused configuration variables.
They need a clear audit of which environment variables are actually used in the codebase versus which are defined but never referenced.

**Why this priority**: Cleanup of unused variables improves maintainability but doesn't affect functionality for end users.

**Independent Test**: Can be fully tested by reviewing the specification documentation or code comments that identify deprecated/unused variables and their usage status.

**Acceptance Scenarios**:

1. **Given** the configuration system is refactored, **When** a developer reviews the config module or related documentation, **Then** they find a list of variables marked as deprecated or unused with notes about their status
2. **Given** an unused variable is identified, **When** it's marked for deprecation, **Then** the documentation clearly states this and provides migration guidance if applicable
3. **Given** a user sets a deprecated variable, **When** the application starts, **Then** a warning is logged indicating the variable is deprecated (optional - may be too intrusive for zero-version breaking changes)

---

### Edge Cases

- What happens when both `NOIZ_DATABASE_URL` and individual `NOIZ_POSTGRES_*` variables are provided? `NOIZ_DATABASE_URL` takes precedence (URI-first rule)
- What happens when `NOIZ_PROCESSED_DATA_DIR` points to a non-existent directory? Auto-create (warn if parents needed)
- What happens when `NOIZ_PROCESSED_DATA_DIR` exists as a file? Fail with clear error
- What happens when `NOIZ_PROCESSED_DATA_DIR` exists as non-empty directory? Fail with error about naming conflicts
- What happens when `NOIZ_PROCESSED_DATA_DIR` exists as empty directory? Continue without warning
- What happens when SQLite backend is used but PostgreSQL variables are still set? (Should be ignored gracefully)
- What happens when a path-based config value contains spaces or special characters? (Should handle properly)
- What happens if `NOIZ_LOGLEVEL` is set to an invalid value? (Should fall back to default with warning)
- What happens when running tests that need to override config values dynamically? (Should support pytest fixture pattern)
- What happens when Flask app is created multiple times in same process with different env vars? (Should reload config as it currently does)
- What happens in CI if a required environment variable is missing from one stage but present in others? (Should fail that stage with clear error)
- What happens when a user follows `.env` documentation but their shell doesn't support `set -a`? (Documentation should provide alternatives)
- What happens when a user's `.env` file contains syntax errors? (Should provide clear guidance on debugging)

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST consolidate all environment variable loading into a single configuration module using pydantic-settings
- **FR-002**: System MUST prefix all environment variables with `NOIZ_` to distinguish them from other applications' variables
- **FR-003**: System MUST support both PostgreSQL and SQLite database backends through configuration
- **FR-004**: System MUST validate all configuration at application startup and fail fast with clear error messages for missing or invalid required values. For `NOIZ_PROCESSED_DATA_DIR`: auto-create if missing (warn if parent directories created); fail if path is file or non-empty directory (prevents naming conflicts); accept empty directories silently
- **FR-005**: System MUST preserve Flask app factory pattern compatibility, loading configuration through Flask's `app.config.from_object()` mechanism
- **FR-006**: System MUST support CLI usage pattern where configuration is loaded once at command invocation
- **FR-007**: System MUST support notebook usage pattern where `create_app()` can reload configuration if environment variables change between calls
- **FR-008**: System MUST maintain backward compatibility with existing configuration module imports during migration (e.g., `from noiz.settings import PROCESSED_DATA_DIR`)
- **FR-009**: System MUST provide type-safe configuration with validation (using Pydantic validators)
- **FR-010**: System MUST document each configuration variable with its purpose, type, default value (if any), and whether it's required
- **FR-020**: System MUST mask sensitive configuration values (passwords, secrets in database URIs) in all logs, error messages, and config display output (display as "******")
- **FR-021**: Configuration validation MUST emit structured logs using appropriate log levels: INFO for successful configuration load, WARN for non-critical issues (e.g., parent directory creation), ERROR for validation failures
- **FR-011**: System MUST identify and document any environment variables that are currently defined but not actually used in the codebase
- **FR-012**: System MUST support both explicit database connection parameters (`NOIZ_POSTGRES_HOST`, etc.) and connection URI (`NOIZ_DATABASE_URL`). If both are provided, `NOIZ_DATABASE_URL` takes precedence (URI-first precedence rule)
- **FR-013**: System MUST handle the `NOIZ_PROCESSED_DATA_DIR` proxy pattern that allows dynamic environment variable access for pytest fixtures
- **FR-014**: System MUST support optional configuration variables with sensible defaults (e.g., `NOIZ_FLASK_ENV`, `NOIZ_LOGLEVEL`)
- **FR-015**: System MUST support Flask-specific configuration variables (e.g., `DEBUG`, `SQLALCHEMY_TRACK_MODIFICATIONS`, `CACHE_TYPE`) that integrate with Flask's config system
- **FR-016**: CI pipeline MUST be updated to use new `NOIZ_` prefixed environment variables in all stages (testing, linting, documentation, system-testing)
- **FR-017**: System MUST provide comprehensive user documentation describing all configuration variables, their purposes, types, defaults, and whether they are required or optional
- **FR-018**: Documentation MUST include guidance on using `.env` files for local development, including shell commands to export variables and recommendations for automatic .env loading tools
- **FR-019**: Documentation MUST clearly distinguish between minimal required configuration for basic usage versus optional configuration for advanced features

### Configuration Variables to Support

Based on code analysis, these variables must be supported:

**Required variables**:
- `NOIZ_PROCESSED_DATA_DIR` - Directory for processed data output (auto-created if missing with warning for parent dirs; must be empty directory or non-existent to prevent naming conflicts)
- `NOIZ_MSEEDINDEX_EXECUTABLE` - Path to mseedindex executable
- `NOIZ_DATABASE_URL` OR `NOIZ_POSTGRES_HOST`, `NOIZ_POSTGRES_PORT`, `NOIZ_POSTGRES_USER`, `NOIZ_POSTGRES_PASSWORD`, `NOIZ_POSTGRES_DB` - Database connection

**Optional variables**:
- `NOIZ_DATABASE_BACKEND` - Backend selection: "postgresql" or "sqlite" (default: "postgresql")
- `NOIZ_FLASK_ENV` - Flask environment: "development" or "production" (default: "development")
- `NOIZ_LOGLEVEL` - Logging level (default: "INFO")

**Flask integration variables** (used in Flask config but not directly loaded from env):
- `DEBUG` - Derived from `NOIZ_FLASK_ENV`
- `SQLALCHEMY_TRACK_MODIFICATIONS` - Always False
- `CACHE_TYPE` - Always "simple"
- `DEBUG_TB_ENABLED` - Derived from DEBUG
- `DEBUG_TB_INTERCEPT_REDIRECTS` - Always False

**Variables to audit for usage**:
- `CELERY_BROKER_URL` - Defined in settings.py but Celery integration not found in codebase
- `CELERY_RESULT_BACKEND` - Defined in settings.py but Celery integration not found in codebase
- `SECRET_KEY` - Commented out in settings.py
- `BCRYPT_LOG_ROUNDS` - Commented out in settings.py
- `WEBPACK_MANIFEST_PATH` - Commented out in settings.py

### Key Entities

- **NoizConfig**: Central configuration object that holds all application settings loaded from environment variables with NOIZ_ prefix, validated by Pydantic, and exposable to Flask's config system
- **DatabaseConfig**: Sub-configuration for database connection settings supporting both PostgreSQL and SQLite with validation for required connection parameters
- **ProcessingConfig**: Sub-configuration for data processing settings including data directory and external tool paths
- **FlaskConfig**: Flask-specific configuration values derived from or compatible with Flask's config system

### Documentation Requirements

The configuration documentation must be added to the Sphinx documentation system in `docs/` and should include:

**Content Structure**:
1. **Overview section**: Explaining the configuration system and NOIZ_ prefix convention
2. **Required Variables section**: List of minimal required environment variables for basic usage with descriptions
3. **Optional Variables section**: Advanced configuration options with defaults and use cases
4. **Database Configuration section**: Detailed explanation of database backend options (PostgreSQL vs SQLite) and connection methods
5. **Using .env Files section**: Practical guide for local development including:
   - Example `.env` file template with all common variables
   - Shell commands for different shells (bash, zsh, fish)
   - Tool recommendations (direnv, dotenv, etc.)
   - Troubleshooting common issues
6. **Flask Integration section**: How configuration works with `create_app()` for notebook users
7. **CI/CD Configuration section**: Guidance for setting environment variables in CI/CD systems
8. **Migration Guide section**: How to migrate from old environment variable names to new NOIZ_ prefixed names

**Documentation Format**: RST (reStructuredText) following project standards with "one sentence per line" formatting

**Shell Command Examples to Include**:
- `set -a; source .env; set +a` (POSIX sh/bash/zsh)
- `export $(cat .env | xargs)` (alternative for bash)
- `set -o allexport; source .env; set +o allexport` (explicit bash)
- Tool-based approaches: direnv, python-dotenv, etc.

**Example .env Content**:
```
NOIZ_DATABASE_BACKEND=postgresql
NOIZ_POSTGRES_HOST=localhost
NOIZ_POSTGRES_PORT=5432
NOIZ_POSTGRES_USER=noiz
NOIZ_POSTGRES_PASSWORD=secret
NOIZ_POSTGRES_DB=noiz
NOIZ_PROCESSED_DATA_DIR=/path/to/processed/data
NOIZ_MSEEDINDEX_EXECUTABLE=mseedindex
NOIZ_FLASK_ENV=development
NOIZ_LOGLEVEL=INFO
```

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All existing Noiz CLI commands execute successfully with environment variables prefixed with `NOIZ_` (100% of commands work)
- **SC-002**: Flask app factory pattern (`create_app()`) continues to work in notebooks with configuration properly loaded
- **SC-003**: Configuration validation provides clear error messages - users can identify and fix configuration issues in under 1 minute
- **SC-004**: All configuration variables are documented with purpose and type - developers can understand full configuration in under 10 minutes
- **SC-005**: Configuration loading adds negligible overhead - application startup time increases by less than 100ms
- **SC-006**: Zero test failures after migration - all existing tests pass with new configuration system
- **SC-007**: Unused configuration variables are identified and documented - technical debt is clearly visible for future cleanup
- **SC-008**: CI pipeline passes all stages with new configuration - 100% of CI jobs succeed with NOIZ_* environment variables
- **SC-009**: New users can configure Noiz from documentation alone in under 15 minutes - no external support needed for basic setup
- **SC-010**: Documentation includes working `.env` examples and shell commands - users can copy-paste to get started

## Assumptions

- Pydantic Settings v2.x will be used with version range `pydantic-settings>=2.0,<3.0` (compatible with Pydantic v2, allows minor updates within v2)
- All current Noiz users can update their environment variables to use `NOIZ_` prefix (breaking change acceptable in v0.x)
- The existing `from noiz.settings import X` pattern can be maintained through aliasing during migration period
- PostgreSQL remains the primary database backend with SQLite as alternative for testing
- Flask's config object remains the source of truth for Flask-specific settings
- The `environs` library currently used can be replaced with pydantic-settings
- Configuration validation should happen eagerly at startup rather than lazily on first access
- The pytest fixture pattern for dynamic environment variable injection must continue to work

## Non-Functional Requirements

- **NFR-001**: Configuration module must maintain compatibility with existing imports for gradual migration
- **NFR-002**: Configuration errors must be clear and actionable (specify which variable is missing/invalid and how to fix it), but must never expose sensitive values (passwords, secrets)
- **NFR-003**: Configuration system must support both eager loading (CLI) and reload scenarios (notebooks with multiple create_app() calls)
- **NFR-004**: Type hints must be comprehensive to support IDE autocomplete and static type checking
- **NFR-005**: Security - All password and secret fields must be masked in logs, config display, and error output to prevent credential leakage
- **NFR-006**: Observability - Configuration validation must use structured logging (loguru) with appropriate log levels respecting `NOIZ_LOGLEVEL` setting

## Dependencies

- **pydantic-settings** library (new dependency, version range: `>=2.0,<3.0`)
- Existing Flask-SQLAlchemy integration
- Existing Flask-Migrate integration
- Current pytest fixture patterns in conftest.py
- GitLab CI configuration (`.gitlab-ci.yml` and templates in `.gitlab/templates/`)
- Sphinx documentation system in `docs/` directory

## Out of Scope

- Automatic migration of environment variables in deployment scripts (users must manually update)
- Configuration file support (TOML/YAML/JSON) - only environment variables
- Runtime configuration changes (except for the existing Flask reload pattern)
- Configuration encryption or secret management
- Deprecation warnings for old variable names (acceptable breaking change in v0.x)
- Migration of processing parameter configs stored in database (separate concern from app-level config)

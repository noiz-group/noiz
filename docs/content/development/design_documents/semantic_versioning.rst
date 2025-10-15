.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

========================================================
Semantic Commits and Automated Changelog Generation
========================================================

:Status: Proposed
:Author: Development Team
:Created: 2025-10-15
:Updated: 2025-10-15
:Priority: Medium
:Estimated Effort: 1-2 weeks

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

This proposal introduces Conventional Commits for merge commits and automated
changelog generation between versions.
This provides clear project history, automated release notes, and semantic
versioning based on commit types.

Problem Statement
=================

Current State
-------------

**Inconsistent Commit Messages**

Current merge commits have varied formats:

::

    Merge branch 'rebuild_postgres_image' into 'main'
    Update 2 files
    Chore(CI): specify amd64 as a platform to build on
    chore(ci): trigger rebuilding postgres image

**Issues**:

* No standard format for merge commits
* Cannot determine change type from message
* Hard to generate changelogs
* No semantic versioning automation

**Manual Changelog Maintenance**

* CHANGELOG must be manually updated
* Easy to forget or miss changes
* Inconsistent formatting
* Duplicate effort (commit + changelog entry)

**Unclear Release Impact**

* Hard to determine if release is major/minor/patch
* Breaking changes not clearly marked
* No automation for version bumps
* Manual decision-making error-prone

Real-World Impact
-----------------

**Scenario 1: Release Preparation**

Maintainer preparing v1.5.0 release:

1. Must read through all commits since v1.4.0
2. Manually categorize changes (features, fixes, breaking)
3. Write changelog entries
4. Decide version number
5. Risk: Missing important changes or miscategorizing

**Scenario 2: Understanding Changes**

User reviewing changes between versions:

1. Reads git log - inconsistent messages
2. Cannot quickly identify breaking changes
3. No clear categorization
4. Must read code to understand impact

**Scenario 3: CI/CD Automation**

Attempting to automate releases:

1. Cannot parse commit messages reliably
2. Cannot determine semantic version bump
3. Cannot generate meaningful changelog
4. Must be done manually

Proposed Solution
=================

Conventional Commits Specification
-----------------------------------

Adopt Conventional Commits v1.0.0 specification.

**Commit Message Format**:

::

    <type>[optional scope]: <description>

    [optional body]

    [optional footer(s)]

**Examples**:

.. code-block:: text

    feat(beamforming): add FK analysis with custom slowness grid

    Implements frequency-wavenumber analysis with user-defined slowness
    ranges. Includes basis caching for improved performance.

    Closes #123

.. code-block:: text

    fix(crosscorrelation): correct time window overlap calculation

    Previous implementation incorrectly handled edge cases when overlap
    was exactly 50%. Now uses consistent rounding.

    Fixes #456

.. code-block:: text

    docs(api): update beamforming parameter descriptions

    Clarifies frequency_min and frequency_max parameters. Adds examples
    for common use cases.

.. code-block:: text

    feat(api)!: change CrosscorrelationParams API signature

    BREAKING CHANGE: CrosscorrelationParams now requires config_id
    parameter. This enables the new transferrable config system.

    Migration: Add config_id when creating params:
        params = CrosscorrelationParams(config_id="ccf_v1", ...)

Commit Types
~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 15 20 15 50

   * - Type
     - Description
     - Version Bump
     - Examples
   * - ``feat``
     - New feature
     - MINOR
     - Add beamforming method
   * - ``fix``
     - Bug fix
     - PATCH
     - Fix time window calculation
   * - ``docs``
     - Documentation only
     - PATCH
     - Update API docs
   * - ``style``
     - Code style changes
     - PATCH
     - Format with ruff
   * - ``refactor``
     - Code refactoring
     - PATCH
     - Extract helper function
   * - ``perf``
     - Performance improvement
     - PATCH
     - Optimize correlation loop
   * - ``test``
     - Add/update tests
     - PATCH
     - Add beamforming tests
   * - ``build``
     - Build system changes
     - PATCH
     - Update dependencies
   * - ``ci``
     - CI configuration
     - PATCH
     - Add GitHub Actions workflow
   * - ``chore``
     - Maintenance tasks
     - PATCH
     - Update gitignore
   * - ``revert``
     - Revert previous commit
     - (depends)
     - Revert "feat: add X"

**Breaking Changes**:

Any commit with ``!`` after type or ``BREAKING CHANGE:`` in footer triggers
MAJOR version bump:

.. code-block:: text

    feat(api)!: remove deprecated QCOne API

    BREAKING CHANGE: QCOneParams renamed to DatachunkQCParams.
    Old API removed completely.

Scopes
~~~~~~

Scopes indicate which part of codebase changed:

**Core Modules**:

* ``core`` - Core framework
* ``cli`` - Command-line interface
* ``api`` - API layer
* ``processing`` - Processing algorithms
* ``models`` - Database models
* ``database`` - Database infrastructure

**Processing Plugins** (after plugin architecture):

* ``datachunk`` - Datachunk processing
* ``beamforming`` - Beamforming analysis
* ``ppsd`` - Power Spectral Density
* ``crosscorrelation`` - Cross-correlation
* ``stacking`` - Stacking operations
* ``qc`` - Quality control

**Infrastructure**:

* ``ci`` - Continuous integration
* ``build`` - Build system
* ``docker`` - Docker configurations
* ``docs`` - Documentation

Examples in Context
~~~~~~~~~~~~~~~~~~~

**Feature Additions**:

.. code-block:: text

    feat(beamforming): add plane wave beamforming method
    feat(cli): add config visualization command
    feat(ppsd): support temporal variation plots

**Bug Fixes**:

.. code-block:: text

    fix(crosscorrelation): handle NaN values in correlation
    fix(database): prevent connection pool exhaustion
    fix(api): validate timespan date ranges

**Documentation**:

.. code-block:: text

    docs(tutorials): add beamforming tutorial
    docs(api): improve type hint documentation
    docs(contributing): update PR guidelines

**Refactoring**:

.. code-block:: text

    refactor(beamforming): split into multiple modules
    refactor(processing): extract common validation logic
    refactor(models): use mixins for timestamp fields

**Breaking Changes**:

.. code-block:: text

    feat(config)!: implement transferrable config system

    BREAKING CHANGE: Config IDs now required for all ProcessingParams.
    Database schema includes new config_id columns.

    Migration guide: docs/migrations/config_ids.rst

Automated Changelog Generation
-------------------------------

Generate CHANGELOG.md automatically from commits.

**Tool**: conventional-changelog or git-cliff

**Changelog Format**:

.. code-block:: markdown

    # Changelog

    All notable changes to this project will be documented in this file.

    The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
    and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

    ## [Unreleased]

    ### Added
    - feat(beamforming): add FK analysis with custom slowness grid (#123)
    - feat(cli): add config visualization command (#125)

    ### Fixed
    - fix(crosscorrelation): correct time window overlap calculation (#456)

    ### Changed
    - refactor(beamforming): split into multiple modules (#789)

    ## [1.5.0] - 2025-10-15

    ### Added
    - feat(ppsd): support temporal variation plots (#120)
    - feat(api): add bulk datachunk operations (#121)

    ### Fixed
    - fix(database): prevent connection pool exhaustion (#450)
    - fix(api): validate timespan date ranges (#451)

    ### Breaking Changes
    - feat(config)!: implement transferrable config system (#100)
      - BREAKING: Config IDs now required for all ProcessingParams
      - See migration guide: docs/migrations/config_ids.rst

    ## [1.4.0] - 2025-09-01
    ...

**Generation Command**:

.. code-block:: bash

    # Generate changelog for version range
    git-cliff --tag v1.5.0 > CHANGELOG.md

    # Generate unreleased changes
    git-cliff --unreleased --prepend CHANGELOG.md

    # Generate with specific config
    git-cliff --config cliff.toml

Semantic Versioning Automation
-------------------------------

**Automatic Version Bumps**:

Based on commit types since last release:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Commits Include
     - Version Bump
   * - Any with ``BREAKING CHANGE`` or ``!``
     - MAJOR (1.x.x → 2.0.0)
   * - Any ``feat`` (no breaking)
     - MINOR (1.4.x → 1.5.0)
   * - Only ``fix``, ``docs``, etc.
     - PATCH (1.4.5 → 1.4.6)

**Example Workflow**:

.. code-block:: bash

    # Current version: 1.4.5

    # Commits since last release:
    # - feat(ppsd): new feature
    # - fix(api): bug fix
    # - docs: update docs

    # Result: 1.5.0 (MINOR bump due to feat)

    # If there was also:
    # - feat(api)!: breaking change

    # Result: 2.0.0 (MAJOR bump due to breaking)

Commit Message Enforcement
---------------------------

**Pre-commit Hook**:

.. code-block:: yaml

    # .pre-commit-config.yaml
    - repo: https://github.com/compilerla/conventional-pre-commit
      rev: v3.0.0
      hooks:
        - id: conventional-pre-commit
          stages: [commit-msg]
          args: []

**Validation**:

.. code-block:: bash

    # Valid commits pass
    $ git commit -m "feat(api): add new endpoint"
    ✓ Commit message follows Conventional Commits

    # Invalid commits fail
    $ git commit -m "added new feature"
    ✗ Commit message does not follow Conventional Commits
    Expected format: <type>[optional scope]: <description>

**GitLab CI Validation**:

.. code-block:: yaml

    # .gitlab-ci.yml
    validate-commits:
      stage: test
      script:
        - pip install commitizen
        - cz check --rev-range origin/main..HEAD
      only:
        - merge_requests

Interactive Commit Tool
-----------------------

**Commitizen**: Interactive commit message builder

.. code-block:: bash

    $ cz commit

    ? Select the type of change you are committing
    ❯ feat:     A new feature
      fix:      A bug fix
      docs:     Documentation only changes
      style:    Changes that do not affect code meaning
      refactor: Code change that neither fixes bug nor adds feature
      perf:     Performance improvement
      test:     Adding missing tests

    ? What is the scope of this change? (class or file name): beamforming

    ? Write a short, imperative description:
    add FK analysis with custom slowness grid

    ? Provide a longer description (press enter to skip):
    Implements frequency-wavenumber analysis with user-defined slowness
    ranges. Includes basis caching for improved performance.

    ? Are there any breaking changes? No

    ? Does this change affect any open issues? Yes

    ? Add issue references (e.g. "fix #123", "re #123".):
    closes #123

    feat(beamforming): add FK analysis with custom slowness grid

    Implements frequency-wavenumber analysis with user-defined slowness
    ranges. Includes basis caching for improved performance.

    Closes #123

    ? Is this correct? Yes

**Configuration**: ``.cz.toml``

.. code-block:: toml

    [tool.commitizen]
    name = "cz_conventional_commits"
    version = "1.4.5"
    version_files = [
        "pyproject.toml:version",
        "src/noiz/__init__.py:__version__"
    ]
    tag_format = "v$version"
    update_changelog_on_bump = true
    bump_message = "chore(release): bump version $current_version → $new_version"

Release Workflow
----------------

**Automated Release Process**:

.. code-block:: bash

    # 1. Bump version based on commits
    cz bump --changelog

    # This automatically:
    # - Analyzes commits since last tag
    # - Determines version bump (major/minor/patch)
    # - Updates version in files
    # - Generates/updates CHANGELOG.md
    # - Creates git tag
    # - Creates release commit

    # 2. Push release
    git push --follow-tags

    # 3. CI builds and publishes to PyPI (if configured)

**GitLab CI Release Job**:

.. code-block:: yaml

    # .gitlab-ci.yml
    release:
      stage: release
      script:
        - pip install commitizen
        - cz bump --changelog --yes
        - git push --follow-tags
      only:
        - main
      when: manual

**GitHub Actions Alternative**:

.. code-block:: yaml

    # .github/workflows/release.yml
    name: Release

    on:
      push:
        branches:
          - main

    jobs:
      release:
        runs-on: ubuntu-latest
        steps:
          - uses: actions/checkout@v3
            with:
              fetch-depth: 0

          - name: Create Release
            uses: cycjimmy/semantic-release-action@v3
            with:
              semantic_version: 19
              extra_plugins: |
                @semantic-release/changelog
                @semantic-release/git
            env:
              GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}

Changelog Configuration
-----------------------

**git-cliff Configuration**: ``cliff.toml``

.. code-block:: toml

    [changelog]
    header = """
    # Changelog

    All notable changes to this project will be documented in this file.

    The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
    and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
    """
    body = """
    {% if version %}\
        ## [{{ version | trim_start_matches(pat="v") }}] - {{ timestamp | date(format="%Y-%m-%d") }}
    {% else %}\
        ## [Unreleased]
    {% endif %}\
    {% for group, commits in commits | group_by(attribute="group") %}
        ### {{ group | upper_first }}
        {% for commit in commits %}
            - {{ commit.message | upper_first }} ({{ commit.id | truncate(length=7, end="") }})\
        {% endfor %}
    {% endfor %}
    """
    trim = true

    [git]
    conventional_commits = true
    filter_unconventional = false
    split_commits = false
    commit_parsers = [
        { message = "^feat", group = "Added" },
        { message = "^fix", group = "Fixed" },
        { message = "^doc", group = "Documentation" },
        { message = "^perf", group = "Performance" },
        { message = "^refactor", group = "Refactored" },
        { message = "^style", group = "Styling" },
        { message = "^test", group = "Testing" },
        { message = "^chore\\(release\\)", skip = true },
        { message = "^chore", group = "Miscellaneous" },
        { body = ".*BREAKING CHANGE.*", group = "Breaking Changes" },
    ]
    filter_commits = false
    tag_pattern = "v[0-9]*"

    [bump]
    features_always_bump_minor = true
    breaking_always_bump_major = true

Implementation Plan
===================

Phase 1: Setup (Week 1)
------------------------

**Day 1-2: Tool Installation**

.. code-block:: bash

    # Add to pyproject.toml dev dependencies
    [project.optional-dependencies]
    dev = [
        "commitizen>=3.0.0",
        "pre-commit>=3.0.0",
        # ... existing deps
    ]

    # Install tools
    uv sync --group dev

**Day 3-4: Configuration**

* Create ``.cz.toml`` configuration
* Create ``cliff.toml`` for git-cliff
* Configure pre-commit hook
* Update ``.gitlab-ci.yml`` with validation

**Day 5: Initial Changelog**

* Generate initial CHANGELOG.md from git history
* Review and clean up generated content
* Commit initial changelog

Phase 2: Team Training (Week 1)
--------------------------------

**Documentation**:

* Write commit message guide
* Add to ``docs/content/development/contributing.rst``
* Create examples for each commit type
* Document commitizen usage

**Team Workshop**:

* Present Conventional Commits
* Demo commitizen tool
* Practice writing good commit messages
* Q&A session

Phase 3: Enforcement (Week 2)
------------------------------

**Gradual Rollout**:

1. **Soft Launch**: Warnings only, no failures
2. **Education Period**: 2 weeks with reminders
3. **Strict Mode**: Pre-commit hook blocks invalid commits
4. **CI Validation**: MR checks enforce format

**Monitoring**:

* Track commit message compliance
* Collect feedback from team
* Adjust scopes and types as needed

Phase 4: Automation (Week 2)
-----------------------------

**Release Automation**:

* Setup automated changelog generation
* Configure version bumping
* Test release workflow
* Document release process

**CI/CD Integration**:

* Automated releases on main branch
* Changelog updates on every release
* Version tagging automation

Migration Strategy
==================

Existing Commits
----------------

**No Retroactive Changes**:

* Existing commits remain as-is
* New convention starts from implementation date
* Changelog generation works forward from cutover

**Initial Changelog**:

Option 1: Manual creation for existing versions:

.. code-block:: markdown

    ## [1.4.5] - 2025-10-01

    Historical release. For detailed changes, see git log.

    ## [1.5.0] - 2025-11-01

    First release with automated changelog.

    ### Added
    ...

Option 2: Best-effort automatic generation with cleanup.

Gradual Adoption
----------------

**Week 1-2: Soft Enforcement**

* Pre-commit hook warns but doesn't fail
* Education and examples
* Help developers learn format

**Week 3-4: Strict Enforcement**

* Pre-commit hook fails on invalid commits
* CI validates MR commits
* All new commits must comply

**Week 5+: Full Automation**

* Automated changelog generation
* Automated version bumping
* Streamlined release process

Alternatives Considered
=======================

Alternative 1: Keep Current Approach
-------------------------------------

**Pros**:

* No change required
* Team already familiar

**Cons**:

* Manual changelog maintenance
* No automation possible
* Inconsistent commit messages
* Hard to track changes

**Rejected**: Doesn't solve core problems

Alternative 2: Jira/Linear Integration
---------------------------------------

**Approach**: Require ticket numbers in commits

.. code-block:: text

    [NOIZ-123] Add beamforming feature

**Pros**:

* Links to issue tracker
* Trackable work items

**Cons**:

* Requires external service
* Less semantic meaning
* Harder to parse for automation
* Not standard

**Rejected**: Less standard, harder to automate

Alternative 3: Simple Prefix Only
----------------------------------

**Approach**: Just use type prefix, no full spec

.. code-block:: text

    feat: add beamforming
    fix: correct calculation

**Pros**:

* Simpler than full Conventional Commits
* Easier adoption

**Cons**:

* No scope information
* Limited automation
* No breaking change indication
* Less structured

**Rejected**: Too limited for automation needs

Risks and Mitigation
=====================

Risk 1: Developer Resistance
-----------------------------

**Risk**: Team finds format too restrictive

**Impact**: Low adoption, inconsistent usage

**Mitigation**:

* Provide interactive tool (commitizen)
* Show clear benefits (automated changelog)
* Start with soft enforcement
* Collect feedback and adjust

Risk 2: Wrong Commit Types
---------------------------

**Risk**: Developers use incorrect type

**Impact**: Wrong version bumps, incorrect changelog

**Mitigation**:

* Clear documentation with examples
* Pre-commit validation
* Code review process
* Easy to fix with rebase

Risk 3: Breaking Changes Missed
--------------------------------

**Risk**: Breaking change not marked with ``!``

**Impact**: Incorrect version bump (minor instead of major)

**Mitigation**:

* Clear guidelines on what constitutes breaking change
* Reviewer checklist includes breaking change check
* Can manually bump major version if missed

Risk 4: Tool Maintenance
-------------------------

**Risk**: Tools become unmaintained

**Impact**: Broken automation

**Mitigation**:

* Use well-established tools (commitizen, git-cliff)
* Have fallback manual process
* Can switch tools if needed (standard format)

Benefits
========

Automation
----------

* Automatic changelog generation
* Semantic version bumping
* Release note creation
* No manual changelog maintenance

Clarity
-------

* Clear commit history
* Easy to understand changes
* Consistent formatting
* Categorized changes

Developer Experience
--------------------

* Interactive commit tool
* Pre-commit validation
* Less cognitive load
* Clear guidelines

Project Management
------------------

* Easy release preparation
* Clear version history
* Automated release notes
* Better change tracking

Success Criteria
================

1. **95%+ Compliance**: 95% of commits follow Conventional Commits
2. **Automated Releases**: Changelog generated automatically
3. **Developer Satisfaction**: Team finds process helpful
4. **Time Savings**: Release preparation time reduced by 50%
5. **Clear History**: External contributors understand changes easily

Example Workflows
=================

Feature Development
-------------------

.. code-block:: bash

    # Create feature branch
    git checkout -b feature/fk-beamforming

    # Make changes and commit with commitizen
    cz commit
    # Select: feat
    # Scope: beamforming
    # Description: add FK analysis with custom slowness grid
    # Body: Implements frequency-wavenumber analysis...
    # Issues: closes #123

    # Push and create MR
    git push origin feature/fk-beamforming

    # CI validates commit format
    # Reviewer checks breaking changes
    # Merge to main

Bug Fix
-------

.. code-block:: bash

    git checkout -b fix/correlation-nan-handling

    cz commit
    # Select: fix
    # Scope: crosscorrelation
    # Description: handle NaN values in correlation
    # Body: Previous implementation crashed on NaN. Now filters them.
    # Issues: fixes #456

    git push origin fix/correlation-nan-handling

Release Preparation
-------------------

.. code-block:: bash

    # On main branch
    git checkout main
    git pull

    # Review unreleased changes
    git-cliff --unreleased

    # Bump version and generate changelog
    cz bump --changelog

    # This creates:
    # - Updated CHANGELOG.md
    # - Updated version in pyproject.toml
    # - Git tag (e.g., v1.5.0)
    # - Release commit

    # Push release
    git push --follow-tags

    # CI builds and publishes to PyPI

References
==========

* **Conventional Commits**: https://www.conventionalcommits.org/
* **Keep a Changelog**: https://keepachangelog.com/
* **Semantic Versioning**: https://semver.org/
* **Commitizen**: https://commitizen-tools.github.io/commitizen/
* **git-cliff**: https://git-cliff.org/
* **Angular Convention**: https://github.com/angular/angular/blob/main/CONTRIBUTING.md

See Also
========

* :doc:`../coding_standards` - Code conventions
* :doc:`refactoring_roadmap` - Overall modernization plan

Document History
================

:Version: 1.0
:Last Updated: 2025-10-15
:Status: Proposed - Awaiting review

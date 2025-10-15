.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

===============================================
Pluggable Architecture and Module System
===============================================

:Status: Proposed
:Author: Development Team
:Created: 2025-10-15
:Updated: 2025-10-15
:Priority: High
:Estimated Effort: 16-20 weeks
:Dependencies: Requires config_system.rst implementation

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

This proposal transforms Noiz from a monolithic application into a modular
platform where processing steps are self-contained plugins.
Each plugin contains its own models, API, processing logic, and CLI,
enabling external developers to create and distribute their own processing
modules.

Problem Statement
=================

Current Monolithic Structure
-----------------------------

The current codebase has shared infrastructure:

::

    noiz/
    ├── models/           # ALL models in one place
    │   ├── datachunk.py
    │   ├── beamforming.py
    │   ├── ppsd.py
    │   └── ...
    ├── api/              # ALL API functions
    │   ├── datachunk.py
    │   ├── beamforming.py
    │   ├── ppsd.py
    │   └── ...
    ├── processing/       # ALL processing code
    │   ├── datachunk.py
    │   ├── beamforming.py
    │   ├── ppsd.py
    │   └── ...
    └── cli.py            # Single CLI with all commands

Limitations
-----------

**Tight Coupling**

* Processing steps depend on shared infrastructure
* Changes to core affect all processing
* Cannot update one module without affecting others
* Difficult to isolate bugs

**No Extension Mechanism**

* External developers cannot add new processing methods
* Must fork entire project to add functionality
* Cannot distribute custom processing as separate package
* No plugin ecosystem

**Development Bottleneck**

* All changes require core team review
* Cannot have specialized maintainers per module
* Hard to parallelize development
* Slows innovation

**Deployment Inflexibility**

* Cannot install only needed processing modules
* Large deployment footprint
* All dependencies required even if unused
* Cannot mix-and-match processing steps

Real-World Scenarios
--------------------

**Scenario 1: Custom Processing Method**

A research group develops novel ambient noise processing:

* Currently: Must fork Noiz, modify core code
* Impact: Diverges from upstream, hard to maintain
* Problem: Cannot share with community as package

**Scenario 2: Specialized Analysis**

User needs only beamforming, not PPSD:

* Currently: Must install entire Noiz
* Impact: Large dependency footprint
* Problem: Cannot create minimal installation

**Scenario 3: Industry Customization**

Company wants to add proprietary processing:

* Currently: Must maintain private fork
* Impact: Merge conflicts with upstream
* Problem: Cannot keep proprietary and open source separate

Proposed Solution
=================

Plugin-Based Architecture
-------------------------

Transform into modular system where each processing step is a self-contained
plugin:

::

    noiz/
    ├── core/                    # Core framework only
    │   ├── plugin_system/       # Plugin loading
    │   ├── database/            # DB connection
    │   ├── cli_framework/       # CLI base
    │   └── web_framework/       # Flask base
    │
    ├── plugins/                 # Built-in plugins
    │   ├── datachunk/
    │   │   ├── models.py        # Datachunk-specific models
    │   │   ├── api.py           # Datachunk API
    │   │   ├── processing.py    # Processing logic
    │   │   ├── cli.py           # CLI commands
    │   │   ├── routes.py        # Flask routes
    │   │   └── plugin.py        # Plugin definition
    │   │
    │   ├── beamforming/
    │   │   ├── models.py
    │   │   ├── api.py
    │   │   ├── processing.py
    │   │   ├── cli.py
    │   │   ├── routes.py
    │   │   └── plugin.py
    │   │
    │   ├── ppsd/
    │   │   └── ...
    │   │
    │   └── crosscorrelation/
    │       └── ...
    │
    └── external_plugins/        # Third-party plugins
        └── custom_method/
            └── ...

Each Plugin is Self-Contained
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # noiz/plugins/beamforming/plugin.py
    from noiz.core.plugin_system import Plugin

    class BeamformingPlugin(Plugin):
        """Beamforming analysis plugin."""

        name = "beamforming"
        version = "1.0.0"
        author = "Noiz Team"

        # Dependencies on other plugins
        requires = ["datachunk", "crosscorrelation"]

        # What this plugin provides
        provides = ["beamforming"]

        def register_models(self):
            """Register database models."""
            from .models import (
                BeamformingResult,
                BeamformingParams,
                BeamformingFile
            )
            return [BeamformingResult, BeamformingParams, BeamformingFile]

        def register_cli(self, cli_group):
            """Register CLI commands."""
            from .cli import beamforming_group
            cli_group.add_command(beamforming_group)

        def register_routes(self, app):
            """Register Flask routes."""
            from .routes import bp
            app.register_blueprint(bp, url_prefix='/beamforming')

        def register_api(self):
            """Register API functions."""
            from . import api
            return api

Plugin Discovery and Loading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # noiz/core/plugin_system/loader.py
    from typing import List, Dict
    import importlib
    from pathlib import Path

    class PluginLoader:
        """Discover and load plugins."""

        def __init__(self):
            self.plugins: Dict[str, Plugin] = {}
            self.plugin_paths = [
                Path(__file__).parent.parent / "plugins",  # Built-in
                Path.home() / ".noiz" / "plugins",  # User plugins
            ]

        def discover_plugins(self) -> List[str]:
            """Find all available plugins."""
            plugins = []

            for path in self.plugin_paths:
                if not path.exists():
                    continue

                for plugin_dir in path.iterdir():
                    if plugin_dir.is_dir():
                        plugin_file = plugin_dir / "plugin.py"
                        if plugin_file.exists():
                            plugins.append(plugin_dir.name)

            return plugins

        def load_plugin(self, name: str) -> Plugin:
            """Load a specific plugin."""
            # Import plugin module
            module = importlib.import_module(f"noiz.plugins.{name}.plugin")

            # Find Plugin class
            for attr in dir(module):
                obj = getattr(module, attr)
                if isinstance(obj, type) and issubclass(obj, Plugin):
                    if obj is not Plugin:
                        plugin = obj()
                        self.plugins[name] = plugin
                        return plugin

            raise ValueError(f"No plugin class found in {name}")

        def resolve_dependencies(self) -> List[Plugin]:
            """
            Sort plugins by dependencies.
            Returns load order.
            """
            # Topological sort
            pass

Plugin Interface Definition
---------------------------

**Base Plugin Class**:

.. code-block:: python

    # noiz/core/plugin_system/base.py
    from abc import ABC, abstractmethod
    from typing import List, Any, Optional

    class Plugin(ABC):
        """Base class for all plugins."""

        # Plugin metadata
        name: str
        version: str
        author: str
        description: str = ""
        license: str = "CECILL-B"

        # Dependencies
        requires: List[str] = []
        provides: List[str] = []

        # Optional hooks
        def on_load(self) -> None:
            """Called when plugin is loaded."""
            pass

        def on_unload(self) -> None:
            """Called when plugin is unloaded."""
            pass

        # Required implementations
        @abstractmethod
        def register_models(self) -> List[Any]:
            """Return list of SQLAlchemy models."""
            pass

        @abstractmethod
        def register_cli(self, cli_group) -> None:
            """Register CLI commands."""
            pass

        @abstractmethod
        def register_routes(self, app) -> None:
            """Register Flask routes."""
            pass

        @abstractmethod
        def register_api(self) -> Any:
            """Return API module."""
            pass

        # Optional: config validation
        def validate_config(self, config: dict) -> bool:
            """Validate plugin configuration."""
            return True

        # Optional: health check
        def health_check(self) -> bool:
            """Check if plugin is functioning correctly."""
            return True

Example Plugin Implementation
------------------------------

**Beamforming Plugin**:

.. code-block:: python

    # noiz/plugins/beamforming/plugin.py
    from noiz.core.plugin_system import Plugin

    class BeamformingPlugin(Plugin):
        name = "beamforming"
        version = "1.0.0"
        author = "Noiz Team"
        description = "FK and plane wave beamforming"

        requires = ["datachunk", "crosscorrelation"]
        provides = ["beamforming"]

        def register_models(self):
            from .models import (
                BeamformingResult,
                BeamformingParams,
                BeamformingFile,
                BeamformingBasisFile
            )
            return [
                BeamformingResult,
                BeamformingParams,
                BeamformingFile,
                BeamformingBasisFile
            ]

        def register_cli(self, cli_group):
            from .cli import beamforming_commands
            cli_group.add_command(beamforming_commands)

        def register_routes(self, app):
            from .routes import beamforming_bp
            app.register_blueprint(
                beamforming_bp,
                url_prefix='/beamforming'
            )

        def register_api(self):
            from . import api
            return api

    # noiz/plugins/beamforming/models.py
    from noiz.core.database import db

    class BeamformingParams(db.Model):
        """Beamforming parameters - owned by this plugin."""
        __tablename__ = "beamforming_params"

        id = db.Column(db.BigInteger, primary_key=True)
        config_id = db.Column(db.Unicode(255), unique=True)
        method = db.Column(db.Unicode(50))  # "fk", "planewave"
        # ... other params

    class BeamformingResult(db.Model):
        """Beamforming results."""
        __tablename__ = "beamforming_result"
        # ...

    # noiz/plugins/beamforming/api.py
    def run_beamforming(
        timespan_ids: List[int],
        params_id: int,
        parallel: bool = False
    ) -> None:
        """
        Run beamforming analysis.
        This is the public API for this plugin.
        """
        from .processing import calculate_beamforming_results
        # Implementation...

    # noiz/plugins/beamforming/cli.py
    import click

    @click.group()
    def beamforming_commands():
        """Beamforming analysis commands."""
        pass

    @beamforming_commands.command()
    @click.option('--timespan-ids', required=True)
    @click.option('--params-id', required=True)
    def run(timespan_ids, params_id):
        """Run beamforming analysis."""
        from .api import run_beamforming
        run_beamforming(timespan_ids, params_id)

Core Framework
--------------

**Minimal Core**: ``noiz/core/``

.. code-block:: python

    core/
    ├── __init__.py
    ├── plugin_system/
    │   ├── __init__.py
    │   ├── base.py          # Plugin base class
    │   ├── loader.py        # Plugin discovery
    │   └── registry.py      # Plugin registry
    ├── database/
    │   ├── __init__.py
    │   └── base.py          # SQLAlchemy setup
    ├── cli_framework/
    │   ├── __init__.py
    │   └── base.py          # Click CLI framework
    └── web_framework/
        ├── __init__.py
        └── app.py           # Flask app factory

**Core provides**:

* Plugin loading mechanism
* Database connection management
* CLI framework (Click groups)
* Web framework (Flask app factory)
* Common utilities (logging, config)

**Core does NOT provide**:

* Processing algorithms (in plugins)
* Data models (in plugins)
* API functions (in plugins)

Plugin CLI Integration
----------------------

Plugins register their commands dynamically:

.. code-block:: bash

    $ noiz --help
    Usage: noiz [OPTIONS] COMMAND [ARGS]...

    Noiz: Ambient seismic noise processing

    Options:
      --help  Show this message and exit.

    Commands:
      datachunk        Datachunk operations (plugin: datachunk)
      beamforming      Beamforming analysis (plugin: beamforming)
      ppsd             Power Spectral Density (plugin: ppsd)
      crosscorrelation Cross-correlation (plugin: crosscorrelation)
      plugins          Plugin management

.. code-block:: bash

    $ noiz plugins list
    Installed plugins:
      datachunk        v1.0.0  [core]
      beamforming      v1.0.0  [core]
      ppsd             v1.0.0  [core]
      crosscorrelation v1.0.0  [core]
      custom_method    v0.1.0  [external]

    $ noiz plugins info beamforming
    Name: beamforming
    Version: 1.0.0
    Author: Noiz Team
    Description: FK and plane wave beamforming
    Dependencies: datachunk, crosscorrelation
    Status: Active

External Plugin Development
----------------------------

**Plugin Template**:

.. code-block:: bash

    noiz plugin create my_custom_method

Generates:

.. code-block:: python

    my_custom_method/
    ├── setup.py
    ├── README.md
    ├── my_custom_method/
    │   ├── __init__.py
    │   ├── plugin.py       # Plugin definition
    │   ├── models.py       # Database models
    │   ├── api.py          # API functions
    │   ├── processing.py   # Processing logic
    │   ├── cli.py          # CLI commands
    │   └── routes.py       # Flask routes
    └── tests/
        └── test_plugin.py

**Installation**:

.. code-block:: bash

    # Install from PyPI
    pip install noiz-plugin-my-custom-method

    # Install from local
    pip install -e ./my_custom_method

    # Noiz auto-discovers installed plugins

**setup.py** for external plugins:

.. code-block:: python

    from setuptools import setup, find_packages

    setup(
        name="noiz-plugin-my-custom-method",
        version="0.1.0",
        packages=find_packages(),
        install_requires=[
            "noiz>=1.0.0",
            # Plugin-specific dependencies
        ],
        entry_points={
            "noiz.plugins": [
                "my_custom_method = my_custom_method.plugin:MyCustomMethodPlugin"
            ]
        }
    )

Plugin Registry
---------------

**Centralized Registry**: Track installed plugins

.. code-block:: python

    # noiz/core/plugin_system/registry.py
    from typing import Dict, List, Optional

    class PluginRegistry:
        """Central registry for all plugins."""

        def __init__(self):
            self._plugins: Dict[str, Plugin] = {}
            self._load_order: List[str] = []

        def register(self, plugin: Plugin) -> None:
            """Register a plugin."""
            if plugin.name in self._plugins:
                raise ValueError(f"Plugin {plugin.name} already registered")

            self._plugins[plugin.name] = plugin

        def get(self, name: str) -> Optional[Plugin]:
            """Get plugin by name."""
            return self._plugins.get(name)

        def get_all(self) -> List[Plugin]:
            """Get all registered plugins."""
            return list(self._plugins.values())

        def get_by_provides(self, capability: str) -> List[Plugin]:
            """Find plugins that provide a capability."""
            return [
                p for p in self._plugins.values()
                if capability in p.provides
            ]

        def check_dependencies(self) -> List[str]:
            """
            Check all plugin dependencies are satisfied.
            Returns list of errors.
            """
            errors = []
            for plugin in self._plugins.values():
                for dep in plugin.requires:
                    if dep not in self._plugins:
                        errors.append(
                            f"{plugin.name} requires {dep} but it's not installed"
                        )
            return errors

Inter-Plugin Communication
--------------------------

Plugins can depend on and use other plugins:

.. code-block:: python

    # noiz/plugins/stacking/processing.py
    from noiz.core.plugin_system import get_plugin

    def stack_crosscorrelations(...):
        """Stack cross-correlations - needs crosscorrelation plugin."""

        # Get crosscorrelation plugin API
        ccf_plugin = get_plugin("crosscorrelation")
        ccf_api = ccf_plugin.register_api()

        # Use crosscorrelation API
        ccfs = ccf_api.fetch_crosscorrelations(...)

        # Stack them
        result = perform_stacking(ccfs)
        return result

Implementation Plan
===================

Phase 0: Preparation (2 weeks)
-------------------------------

**Week 1: Core Framework Design**

* Finalize Plugin base class API
* Design plugin discovery mechanism
* Plan database migration strategy
* Document plugin development guide

**Week 2: Proof of Concept**

* Implement minimal core framework
* Create one example plugin (datachunk)
* Test plugin loading
* Validate approach

Phase 1: Core Framework (4 weeks)
----------------------------------

**Week 3-4: Plugin System**

* Implement ``Plugin`` base class
* Implement ``PluginLoader``
* Implement ``PluginRegistry``
* Dependency resolution
* Comprehensive tests

**Week 5-6: Core Infrastructure**

* Refactor database module
* Create CLI framework
* Create Flask app factory
* Plugin discovery via entry points
* Integration tests

Phase 2: Migrate Existing Code (6-8 weeks)
-------------------------------------------

**Week 7-8: Datachunk Plugin**

* Extract datachunk code to plugin
* Create plugin structure
* Register models, API, CLI
* Test in isolation

**Week 9-10: Crosscorrelation Plugin**

* Extract crosscorrelation code
* Handle dependencies on datachunk
* Test inter-plugin communication

**Week 11-12: Beamforming Plugin**

* Extract beamforming code
* Complex dependencies (datachunk + crosscorrelation)
* Test full chain

**Week 13-14: Remaining Plugins**

* PPSD plugin
* QC plugins
* Stacking plugin
* Event detection plugin

Phase 3: External Plugin Support (2 weeks)
-------------------------------------------

**Week 15: Developer Tools**

* Plugin template generator
* Documentation generator
* Testing utilities
* Example plugins

**Week 16: Plugin Marketplace**

* Plugin discovery service
* Installation helpers
* Validation tools

Phase 4: Migration and Polish (2 weeks)
----------------------------------------

**Week 17: Backwards Compatibility**

* Compatibility layer for old imports
* Migration guide
* Update all examples
* Deprecation warnings

**Week 18: Documentation**

* Plugin development guide
* API reference
* Migration guide
* Tutorial: Create your first plugin

**Total Duration**: 16-20 weeks

Migration Strategy
==================

Backwards Compatibility
-----------------------

**Phase 1: Dual Import Paths**

Old imports still work:

.. code-block:: python

    # Old way (deprecated)
    from noiz.api.beamforming import run_beamforming

    # New way
    from noiz.plugins.beamforming.api import run_beamforming

**Phase 2: Compatibility Shims**

.. code-block:: python

    # noiz/api/__init__.py
    import warnings
    from noiz.core.plugin_system import get_plugin

    def __getattr__(name):
        """Compatibility shim for old imports."""
        warnings.warn(
            f"Importing from noiz.api.{name} is deprecated. "
            f"Use noiz.plugins.{name}.api instead",
            DeprecationWarning
        )

        plugin = get_plugin(name)
        if plugin:
            return plugin.register_api()

        raise AttributeError(f"No plugin named {name}")

**Phase 3: Removal**

* Major version bump (v2.0.0)
* Remove compatibility layer
* Pure plugin-based system

Gradual Migration Path
----------------------

Users can migrate incrementally:

1. **Update Noiz**: Install new version with plugin system
2. **Code Still Works**: Old imports use compatibility layer
3. **Update Imports**: Gradually switch to new plugin imports
4. **Install External Plugins**: Add custom processing
5. **Remove Deprecated**: Clean up old imports before v2.0

Alternatives Considered
=======================

Alternative 1: Namespace Packages
----------------------------------

**Approach**: Use Python namespace packages

.. code-block:: python

    noiz.datachunk
    noiz.beamforming
    noiz.custom_method  # External package

**Pros**:

* Standard Python approach
* Natural import structure

**Cons**:

* No control over registration order
* Cannot enforce dependencies
* No plugin metadata
* Harder to discover capabilities

**Rejected**: Insufficient control and metadata

Alternative 2: Entry Points Only
---------------------------------

**Approach**: Use setuptools entry points without Plugin class

**Pros**:

* Standard Python packaging

**Cons**:

* No standardized interface
* Cannot validate plugins
* No dependency resolution
* Hard to query capabilities

**Rejected**: Too loose, no guarantees

Alternative 3: Microservices
-----------------------------

**Approach**: Each plugin as separate service with REST API

**Pros**:

* Complete isolation
* Language-independent

**Cons**:

* Massive complexity
* Network overhead
* Deployment nightmare
* Overkill for scientific software

**Rejected**: Too complex for use case

Risks and Mitigation
=====================

Risk 1: Breaking Changes
-------------------------

**Risk**: Refactoring breaks existing code

**Impact**: Users cannot upgrade

**Mitigation**:

* Comprehensive test suite
* Compatibility layer
* Long deprecation period
* Clear migration guide

Risk 2: Performance Overhead
-----------------------------

**Risk**: Plugin loading adds overhead

**Impact**: Slower startup

**Mitigation**:

* Lazy loading of plugins
* Cache plugin metadata
* Profile and optimize loader
* Acceptable trade-off for flexibility

Risk 3: Complex Dependencies
-----------------------------

**Risk**: Circular plugin dependencies

**Impact**: Cannot load plugins

**Mitigation**:

* Topological sort during loading
* Detect cycles early
* Clear error messages
* Enforce acyclic dependencies

Risk 4: Plugin Quality
----------------------

**Risk**: Low-quality external plugins

**Impact**: Bad user experience

**Mitigation**:

* Plugin validation tools
* Testing requirements
* Code review for "official" plugins
* Community ratings

Risk 5: Security
----------------

**Risk**: Malicious plugins

**Impact**: Security vulnerabilities

**Mitigation**:

* Sandboxing for external plugins
* Code signing for official plugins
* Security audit tools
* Clear warnings for unverified plugins

Benefits
========

Extensibility
-------------

* External developers can add processing methods
* No need to fork Noiz
* Plugins distributed as PyPI packages
* Community-driven development

Maintainability
---------------

* Isolated code per processing step
* Easier to test individual plugins
* Specialized maintainers per plugin
* Parallel development

Deployment Flexibility
----------------------

* Install only needed plugins
* Smaller deployment footprint
* Custom plugin combinations
* Cloud-friendly architecture

Innovation
----------

* Lower barrier to experimentation
* Rapid prototyping of new methods
* A/B testing different approaches
* Faster iteration cycles

Success Criteria
================

1. **External Plugin Works**: Third-party plugin installs and functions
2. **Backwards Compatible**: Old code runs with warnings
3. **Performance**: Startup time under 2 seconds with all plugins
4. **Documentation**: Complete plugin development guide
5. **Migration**: All existing code migrated to plugins

Future Enhancements
===================

* **Plugin Marketplace**: Web UI for browsing plugins
* **Hot Reload**: Load/unload plugins without restart
* **Plugin Versioning**: Multiple versions of same plugin
* **Remote Plugins**: Load plugins from URLs
* **Plugin Configuration UI**: Visual plugin configuration
* **Sandboxing**: Isolate untrusted plugins
* **Performance Monitoring**: Track per-plugin resource usage

Integration with Other Proposals
=================================

Works With Config System
-------------------------

Plugins integrate with :doc:`config_system`:

.. code-block:: python

    # Plugin can define its config schema
    class BeamformingPlugin(Plugin):
        def get_config_schema(self):
            """Return TOML schema for this plugin's configs."""
            return {
                "method": {"type": "string", "enum": ["fk", "planewave"]},
                "frequency_min": {"type": "float"},
                # ...
            }

Requires S3 Storage
-------------------

Plugins should work with :doc:`s3_storage`:

.. code-block:: python

    # Plugins use core storage abstraction
    from noiz.core.storage import get_filesystem

    class BeamformingPlugin(Plugin):
        def process(...):
            fs = get_filesystem()  # Gets S3 or local
            fs.write(path, data)

References
==========

* **Plugin Architectures**: Pytest, Sphinx, Flask extensions
* **Entry Points**: https://packaging.python.org/specifications/entry-points/
* **Namespace Packages**: PEP 420
* **Similar Systems**: Kedro, Prefect, MLflow

See Also
========

* :doc:`config_system` - Configuration with dependencies
* :doc:`refactoring_roadmap` - Modernization roadmap
* :doc:`architecture` - Current architecture analysis

Document History
================

:Version: 1.0
:Last Updated: 2025-10-15
:Status: Proposed - Awaiting review
:Dependencies: Requires config_system.rst to be implemented first

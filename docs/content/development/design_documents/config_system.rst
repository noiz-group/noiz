.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

================================================
Transferrable Configuration System with Linking
================================================

:Status: Proposed
:Author: Development Team
:Created: 2025-10-15
:Updated: 2025-10-15
:Priority: High
:Estimated Effort: 8-10 weeks

.. contents:: Table of Contents
   :local:
   :depth: 2

Overview
========

This proposal introduces a comprehensive configuration system that makes Noiz
processing parameters fully transferrable, reproducible, and traceable.
The new system allows users to export complete processing pipelines with all
parameter dependencies, enabling true scientific reproducibility.

Problem Statement
=================

Current State
-------------

The existing configuration system has critical limitations:

**Lack of Reproducibility**

* User A cannot share their complete processing configuration with User B
* No way to export a processing pipeline with all parameter relationships
* Configuration relationships are implicit, not explicit
* Cannot trace which configs were used together for a specific result

**Scientific Reproducibility Crisis**

* Results cannot be reproduced without manual config reconstruction
* No way to version complete processing pipelines
* Parameter dependencies are lost when sharing results
* Violates basic scientific reproducibility requirements

**Current Config Storage**

.. code-block:: python

    # Current approach: configs stored independently
    datachunk_params = DatachunkParams(...)  # ID: 1
    crosscorr_params = CrosscorrelationParams(...)  # ID: 5

    # Relationship is implicit - which datachunk config was used?
    # No way to know without checking the results

**File Structure Limitations**

Current TOML configs in ``config_examples/`` are:

* Independent files with no linking
* No way to express parameter inheritance
* No dependency tracking
* Cannot express "use datachunk config X with crosscorr config Y"

Real-World Impact
-----------------

**Publication Scenario**

1. Researcher processes data using specific parameter chain
2. Publishes paper with results
3. Reviewer asks: "What exact parameters were used?"
4. Researcher must manually reconstruct from database
5. High risk of incomplete or incorrect parameter documentation

**Collaboration Scenario**

1. User A develops optimal processing pipeline for specific noise analysis
2. User B wants to replicate on different network
3. No way to export/import complete pipeline
4. User B must manually recreate all configs
5. High risk of parameter mismatch

Proposed Solution
=================

ID-Based Configuration System
------------------------------

**Core Concept**: Use human-friendly IDs and explicit dependencies instead of
database auto-increment IDs or file structure.

Configuration ID Format
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: toml

    # datachunk_config.toml
    [config]
    id = "dc_2023_highfreq_v1"
    name = "High Frequency Datachunk Processing 2023"
    version = "1.0"
    created = "2023-06-15"
    author = "user@example.com"

    [parameters]
    sampling_rate = 100.0
    preprocessing = "demean"
    remove_response = true

**Benefits**:

* Human-readable identifiers
* Version control friendly
* No database dependency for IDs
* Can reference configs before they exist in database

Dependency and Linking System
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Express parent-child relationships explicitly:

.. code-block:: toml

    # processed_datachunk_method_a.toml
    [config]
    id = "pdc_spectral_whitening_v2"
    name = "Spectral Whitening Method"
    parent = "dc_2023_highfreq_v1"  # References parent config

    [parameters]
    spectral_whitening = true
    whitening_method = "running_mean"
    smooth_length = 20

    # processed_datachunk_method_b.toml
    [config]
    id = "pdc_one_bit_norm_v1"
    name = "One-Bit Normalization Method"
    parent = "dc_2023_highfreq_v1"  # Same parent, different processing

    [parameters]
    one_bit_normalization = true

Multiple children from same parent:

.. code-block:: toml

    # crosscorr_from_spectral.toml
    [config]
    id = "ccf_spectral_10s_v1"
    name = "10s Window Cross-Correlation"
    parent = "pdc_spectral_whitening_v2"

    [parameters]
    window_length = 10.0
    overlap = 0.5

    # crosscorr_from_onebit.toml
    [config]
    id = "ccf_onebit_10s_v1"
    name = "10s Window Cross-Correlation from One-Bit"
    parent = "pdc_one_bit_norm_v1"

    [parameters]
    window_length = 10.0
    overlap = 0.5

Complex Dependency Trees
~~~~~~~~~~~~~~~~~~~~~~~~~

Multiple parents (e.g., beamforming needs both processed datachunks and CCFs):

.. code-block:: toml

    # beamforming_config.toml
    [config]
    id = "bf_fk_analysis_v1"
    name = "FK Analysis Beamforming"
    depends_on = [
        "pdc_spectral_whitening_v2",
        "ccf_spectral_10s_v1"
    ]

    [parameters]
    method = "fk"
    frequency_min = 0.1
    frequency_max = 1.0

Pipeline Export Format
~~~~~~~~~~~~~~~~~~~~~~

Complete pipeline as single file:

.. code-block:: toml

    # pipeline_ambient_noise_2023.toml
    [pipeline]
    id = "pipeline_ambient_noise_tomography_2023"
    name = "Ambient Noise Tomography Pipeline 2023"
    version = "1.0"
    created = "2023-06-15"
    author = "research_group@university.edu"
    description = """
    Complete processing pipeline for ambient noise tomography.
    Used in Smith et al. (2023) publication.
    """

    [[configs]]
    stage = "datachunk"
    id = "dc_2023_highfreq_v1"

    [[configs]]
    stage = "processed_datachunk"
    id = "pdc_spectral_whitening_v2"
    parent = "dc_2023_highfreq_v1"

    [[configs]]
    stage = "processed_datachunk"
    id = "pdc_one_bit_norm_v1"
    parent = "dc_2023_highfreq_v1"

    [[configs]]
    stage = "crosscorrelation"
    id = "ccf_spectral_10s_v1"
    parent = "pdc_spectral_whitening_v2"

    [[configs]]
    stage = "crosscorrelation"
    id = "ccf_onebit_10s_v1"
    parent = "pdc_one_bit_norm_v1"

    [[configs]]
    stage = "stacking"
    id = "stack_linear_30days_v1"
    parent = "ccf_spectral_10s_v1"

**Export includes**:

* All config parameters inline or as references
* Complete dependency tree
* Metadata (author, dates, description)
* Version information

Database Schema Changes
-----------------------

Add ID and linking fields to config tables:

.. code-block:: python

    class DatachunkParams(db.Model):
        # Existing fields
        id = db.Column(db.BigInteger, primary_key=True)

        # NEW: Human-friendly ID
        config_id = db.Column(db.Unicode(255), unique=True, nullable=False)

        # NEW: Metadata
        config_name = db.Column(db.UnicodeText)
        config_version = db.Column(db.Unicode(50))
        config_author = db.Column(db.Unicode(255))
        created_date = db.Column(db.DateTime, default=datetime.utcnow)

        # NEW: Pipeline tracking
        pipeline_id = db.Column(db.Unicode(255))

        # Existing parameter fields...

    class ProcessedDatachunkParams(db.Model):
        id = db.Column(db.BigInteger, primary_key=True)

        # NEW fields
        config_id = db.Column(db.Unicode(255), unique=True, nullable=False)
        parent_config_id = db.Column(db.Unicode(255))  # References parent

        # NEW: Explicit parent relationship
        parent_config = db.Column(db.BigInteger,
                                  db.ForeignKey("datachunk_params.id"))

        # Metadata fields...

Parsing and Validation
----------------------

**Config Parser Module**: ``noiz/config_system/parser.py``

.. code-block:: python

    from typing import Dict, List, Optional
    from dataclasses import dataclass

    @dataclass
    class ConfigNode:
        """Represents a single config in the dependency tree."""
        config_id: str
        stage: str
        parent_id: Optional[str]
        depends_on: List[str]
        parameters: Dict[str, Any]
        metadata: Dict[str, Any]

    class ConfigParser:
        """Parse and validate config files with dependencies."""

        def parse_config(self, filepath: Path) -> ConfigNode:
            """Parse single config file."""
            data = toml.load(filepath)
            return self._validate_and_create_node(data)

        def parse_pipeline(self, filepath: Path) -> List[ConfigNode]:
            """Parse complete pipeline file."""
            data = toml.load(filepath)
            return self._parse_pipeline_structure(data)

        def validate_dependencies(self, nodes: List[ConfigNode]) -> None:
            """Validate all dependencies can be resolved."""
            # Check for circular dependencies
            # Check all referenced configs exist
            # Validate stage compatibility

    class DependencyResolver:
        """Resolve config dependency trees."""

        def build_execution_order(
            self,
            nodes: List[ConfigNode]
        ) -> List[ConfigNode]:
            """
            Topologically sort configs by dependencies.
            Returns execution order.
            """
            pass

        def find_conflicts(
            self,
            nodes: List[ConfigNode]
        ) -> List[str]:
            """
            Find configuration conflicts.
            E.g., incompatible parameters in parent-child.
            """
            pass

Visualization System
--------------------

**Dependency Graph Visualization**: ``noiz/config_system/visualizer.py``

.. code-block:: python

    import graphviz
    from typing import List

    class ConfigVisualizer:
        """Visualize config dependency trees."""

        def create_dependency_graph(
            self,
            nodes: List[ConfigNode],
            output_format: str = "png"
        ) -> Path:
            """
            Create visual dependency graph.
            Returns path to generated image.
            """
            dot = graphviz.Digraph(comment='Config Dependencies')

            # Add nodes
            for node in nodes:
                label = f"{node.config_id}\n{node.stage}"
                dot.node(node.config_id, label)

            # Add edges
            for node in nodes:
                if node.parent_id:
                    dot.edge(node.parent_id, node.config_id)
                for dep in node.depends_on:
                    dot.edge(dep, node.config_id, style='dashed')

            return dot.render(format=output_format)

        def create_pipeline_summary(
            self,
            pipeline: Pipeline
        ) -> str:
            """
            Generate text summary of pipeline.
            Includes all stages, configs, and dependencies.
            """
            pass

**Example output**:

::

    dc_2023_highfreq_v1
    └── pdc_spectral_whitening_v2
        └── ccf_spectral_10s_v1
            └── stack_linear_30days_v1
    └── pdc_one_bit_norm_v1
        └── ccf_onebit_10s_v1

Flask Web Editor
----------------

**Config Editor UI**: New Flask blueprint ``noiz/routes/config_editor.py``

**Features**:

* Visual pipeline builder (drag-and-drop)
* Config parameter editor with validation
* Dependency graph visualization
* Import/export pipeline files
* Version comparison
* Conflict detection

**Routes**:

.. code-block:: python

    @bp.route('/configs')
    def list_configs():
        """List all configs by stage."""
        pass

    @bp.route('/configs/<config_id>')
    def view_config(config_id):
        """View single config with dependencies."""
        pass

    @bp.route('/configs/<config_id>/edit', methods=['GET', 'POST'])
    def edit_config(config_id):
        """Edit config parameters."""
        pass

    @bp.route('/pipeline/new', methods=['GET', 'POST'])
    def create_pipeline():
        """Visual pipeline builder."""
        pass

    @bp.route('/pipeline/<pipeline_id>/export')
    def export_pipeline(pipeline_id):
        """Export pipeline as TOML."""
        pass

    @bp.route('/pipeline/import', methods=['POST'])
    def import_pipeline():
        """Import pipeline from TOML."""
        pass

**UI Components**:

* **Pipeline Canvas**: Visual node-based editor (using jsPlumb or similar)
* **Config Panel**: Parameter editing form
* **Dependency View**: Interactive graph visualization
* **Validation Panel**: Shows conflicts and missing dependencies

CLI Commands
------------

**New config management commands**:

.. code-block:: bash

    # Export pipeline
    noiz configs export-pipeline --pipeline-id ambient_noise_2023 \
                                  --output pipeline.toml

    # Import pipeline
    noiz configs import-pipeline --file pipeline.toml \
                                  --validate-only  # Dry run

    # Visualize dependencies
    noiz configs visualize --config-id ccf_spectral_10s_v1 \
                           --output graph.png

    # List all configs
    noiz configs list --stage crosscorrelation

    # Validate config file
    noiz configs validate --file my_config.toml

    # Show config tree
    noiz configs tree --config-id stack_linear_30days_v1

Implementation Plan
===================

Phase 1: Core Infrastructure (2 weeks)
---------------------------------------

**Week 1: Database Schema**

* Add ``config_id``, ``parent_config_id`` to all param tables
* Create migration
* Update model classes with new fields
* Add unique constraints

**Week 2: Parser and Validator**

* Implement ``ConfigParser`` class
* Implement ``DependencyResolver`` class
* Add TOML schema validation
* Write comprehensive tests

Phase 2: Import/Export (2 weeks)
---------------------------------

**Week 3: Export System**

* Implement pipeline export
* Support nested dependencies
* Include all metadata
* Handle circular references

**Week 4: Import System**

* Implement pipeline import
* Validate before inserting
* Handle ID conflicts
* Transaction-based import (all-or-nothing)

Phase 3: Visualization (1-2 weeks)
-----------------------------------

**Week 5: Dependency Graphs**

* Implement graphviz-based visualization
* Add ASCII tree output
* Support filtering by stage
* Handle large graphs

**Week 6: CLI Integration**

* Add ``noiz configs`` command group
* Implement all subcommands
* Add shell completion
* Update documentation

Phase 4: Flask Web Editor (3-4 weeks)
--------------------------------------

**Week 7-8: Basic Editor**

* Create Flask blueprint
* List/view configs
* Basic edit forms
* Dependency viewer

**Week 9-10: Visual Pipeline Builder**

* Implement drag-and-drop canvas
* Node-based editing
* Real-time validation
* Import/export UI

Phase 5: Migration and Documentation (1 week)
----------------------------------------------

**Week 11: Migration Tools**

* Create migration script for existing configs
* Assign config IDs to existing data
* Generate example pipelines
* Update all documentation

**Total Duration**: 8-10 weeks

Migration Strategy
==================

Backwards Compatibility
-----------------------

**Phase 1: Dual System**

Both old and new systems work simultaneously:

.. code-block:: python

    # Old way still works
    params = DatachunkParams.query.filter_by(id=1).first()

    # New way
    params = DatachunkParams.query.filter_by(
        config_id="dc_2023_highfreq_v1"
    ).first()

**Phase 2: Gradual Migration**

* Script to assign config IDs to existing configs
* Generate pipelines from existing result chains
* Update internal code to use config IDs

**Phase 3: Deprecation**

* Add warnings when using numeric IDs only
* Recommend config IDs in all new work
* Eventually require config IDs for new configs

Existing Config Migration
--------------------------

.. code-block:: python

    # Migration script: assign_config_ids.py
    from noiz.models import DatachunkParams
    from noiz.config_system import generate_config_id

    def migrate_existing_configs():
        """Assign config IDs to existing configurations."""
        for params in DatachunkParams.query.all():
            if not params.config_id:
                # Generate ID from parameters hash
                params.config_id = generate_config_id(
                    stage="datachunk",
                    params=params.to_dict()
                )
                params.config_name = f"Migrated config {params.id}"
                params.config_version = "migrated"

        db.session.commit()

Alternatives Considered
=======================

Alternative 1: File Structure Based
------------------------------------

**Approach**: Use directory hierarchy to express relationships

::

    configs/
    └── pipeline_2023/
        ├── datachunk.toml
        ├── processed/
        │   ├── spectral_whitening.toml
        │   └── one_bit.toml
        └── crosscorrelation/
            ├── from_spectral.toml
            └── from_onebit.toml

**Pros**:

* Intuitive file organization
* Easy to browse

**Cons**:

* Hard to express multiple parents
* Circular dependencies impossible
* Cannot reuse configs across pipelines
* Version control conflicts with nested structure

**Rejected**: Too limiting for complex dependency graphs

Alternative 2: Database-Only with Foreign Keys
-----------------------------------------------

**Approach**: Store all relationships in database only, no config files

**Pros**:

* Enforced referential integrity
* Standard database approach

**Cons**:

* Not portable (cannot share without database export)
* Hard to version control
* Cannot preview without database access
* Loses human-friendly IDs

**Rejected**: Fails portability requirement

Alternative 3: JSON Schema
---------------------------

**Approach**: Use JSON instead of TOML

**Pros**:

* More tooling support
* JSON Schema for validation

**Cons**:

* Less human-friendly
* No comments support
* TOML already used in project

**Rejected**: TOML better for config files

Risks and Mitigation
=====================

Risk 1: Config ID Conflicts
----------------------------

**Risk**: Two users create same config ID

**Impact**: Import fails or overwrites existing config

**Mitigation**:

* Validate uniqueness before import
* Prompt user to rename on conflict
* Support namespaced IDs (``username/config_id``)

Risk 2: Circular Dependencies
------------------------------

**Risk**: Config A depends on B, B depends on A

**Impact**: Cannot resolve execution order

**Mitigation**:

* Detect cycles during validation
* Reject circular imports
* Provide clear error messages

Risk 3: Performance with Large Trees
-------------------------------------

**Risk**: Visualizing pipeline with 100+ configs

**Impact**: Slow visualization, unreadable graphs

**Mitigation**:

* Support filtering by stage
* Pagination in web UI
* Collapsible subtrees
* SVG output with zoom

Risk 4: Complex Migration
--------------------------

**Risk**: Migrating existing configs is error-prone

**Impact**: Data loss or incorrect relationships

**Mitigation**:

* Dry-run migration mode
* Generate reports of changes
* Keep old IDs as backup
* Transaction-based migration

Benefits
========

Scientific Reproducibility
---------------------------

* Complete parameter documentation for publications
* Reviewers can verify exact processing pipeline
* Results fully reproducible

Collaboration
-------------

* Share processing pipelines between institutions
* Standardize processing across research groups
* Reduce setup time for new users

Quality Control
---------------

* Track parameter changes over time
* Compare different processing approaches
* Identify optimal parameter combinations

Education
---------

* Example pipelines for tutorials
* Best practices encoded in config files
* Easier onboarding for new users

Success Criteria
================

1. **Export/Import Works**: User can export pipeline, send to colleague,
   colleague imports and runs successfully
2. **Visualization Clear**: Dependency graphs help users understand pipelines
3. **Web Editor Usable**: Non-technical users can create pipelines in Flask UI
4. **Backwards Compatible**: Existing code continues to work
5. **Performance**: Import/export under 1 second for typical pipeline

Future Enhancements
===================

* **Config Registry**: Central repository of validated pipelines
* **Parameter Optimization**: Track which parameters work best
* **Automatic Documentation**: Generate method sections for papers
* **Config Diff**: Compare two pipelines visually
* **Templates**: Standard pipeline templates for common analyses
* **Cloud Storage**: Store pipelines in S3/MinIO
* **Version Control Integration**: Git-based config versioning

References
==========

* **TOML Specification**: https://toml.io/
* **Graphviz**: https://graphviz.org/
* **jsPlumb**: https://jsplumbtoolkit.com/
* **Similar Systems**: Snakemake, Nextflow, Airflow

See Also
========

* :doc:`plugin_architecture` - Pluggable processing modules
* :doc:`refactoring_roadmap` - Overall modernization plan
* :doc:`s3_storage` - Cloud storage for config files

Document History
================

:Version: 1.0
:Last Updated: 2025-10-15
:Status: Proposed - Awaiting review

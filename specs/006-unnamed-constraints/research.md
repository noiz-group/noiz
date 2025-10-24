Unnamed Constraints Feature - Research Document
================================================

This document provides comprehensive research findings for implementing unnamed constraints in the Noiz codebase.
The goal is to transition from named to unnamed unique constraints and update the conflict handling logic accordingly.

Executive Summary
-----------------

The research confirms that SQLAlchemy 1.4's PostgreSQL dialect fully supports ``index_elements`` parameter for ``on_conflict_do_update()``, making it possible to use unnamed constraints effectively.
Both PostgreSQL and SQLite provide sufficient information in error messages to extract column names from constraint violations.
The codebase currently has 35 total UniqueConstraint definitions across 11 model files, with 16 named and 19 unnamed constraints.

1. SQLAlchemy PostgreSQL on_conflict_do_update Support
-------------------------------------------------------

Current SQLAlchemy Version
~~~~~~~~~~~~~~~~~~~~~~~~~~

The project uses SQLAlchemy 1.4.54 (as specified in pyproject.toml: ``sqlalchemy >=1.4.0, <2.0``).

index_elements Parameter Support
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Confirmed**: SQLAlchemy 1.4's PostgreSQL dialect fully supports the ``index_elements`` parameter for ``on_conflict_do_update()``.

Documentation and Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~

From SQLAlchemy 1.4 official documentation:

.. code-block:: python

    from sqlalchemy.dialects.postgresql import insert

    # Using index_elements with column names
    stmt = insert(my_table).values(id="some_id", data="inserted value")
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_=dict(data="updated value")
    )
    # Generates: INSERT INTO my_table (id, data) VALUES (...)
    #            ON CONFLICT (id) DO UPDATE SET data = ...

    # Multiple columns
    insert_stmt = insert(Table).values(table_info)
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['tableTime', 'deploymentID'],
        set_=dict(...)
    )

    # Using Column objects
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements=[my_table.c.user_email],
        set_=dict(data=stmt.excluded.data)
    )

Key Points
~~~~~~~~~~

- The ``index_elements`` parameter accepts: string column names, Column objects, and SQL expression elements
- Works with single and multiple columns
- Can be combined with ``index_where`` for partial indexes
- Generates proper ``ON CONFLICT (columns) DO UPDATE`` SQL syntax
- Does not require named constraints

SQLite Support
~~~~~~~~~~~~~~

SQLAlchemy 1.4's SQLite dialect also supports ``index_elements`` parameter.
The current codebase already handles this in ``src/noiz/database.py`` with the ``dialect_agnostic_on_conflict()`` helper function.

Compatibility Verification
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The existing code in ``/Users/qsbt/noiz-group/noiz/src/noiz/database.py`` demonstrates working PostgreSQL support:

.. code-block:: python

    if dialect_name == "postgresql":
        # PostgreSQL supports named constraints
        return insert_stmt.on_conflict_do_update(constraint=constraint_name, set_=set_)
    elif dialect_name == "sqlite":
        # SQLite needs column names, not constraint names
        if index_elements is None and constraint_name:
            # Try to map constraint name to columns
            index_elements = CONSTRAINT_TO_COLUMNS.get(constraint_name)
        return insert_stmt.on_conflict_do_update(index_elements=index_elements, set_=set_)

After transitioning to unnamed constraints, both dialects can use ``index_elements`` directly without the constraint name mapping.

2. Database Error Reporting and Column Extraction
--------------------------------------------------

PostgreSQL IntegrityError Format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a unique constraint is violated in PostgreSQL, psycopg2 raises an ``IntegrityError`` with detailed information.

Error Message Format
^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    duplicate key value violates unique constraint "constraint_name"
    DETAIL: Key (column1, column2, column3)=(value1, value2, value3) already exists.

Example from actual constraint:

.. code-block:: text

    duplicate key value violates unique constraint "unique_component_per_station"
    DETAIL: Key (network, station, component)=(XX, STAT, Z) already exists.

Extracting Information
^^^^^^^^^^^^^^^^^^^^^^^

PostgreSQL provides multiple ways to access constraint information:

**Method 1: Using psycopg2 diagnostics**

.. code-block:: python

    from sqlalchemy.exc import IntegrityError
    from psycopg2 import errorcodes as pg_errorcodes

    try:
        # database operation
    except IntegrityError as exc:
        # Check error code
        assert exc.__cause__.pgcode == pg_errorcodes.UNIQUE_VIOLATION

        # Get constraint name
        constraint_name = exc.__cause__.diag.constraint_name
        # Returns: "unique_component_per_station"

**Method 2: Parsing error message for column names**

.. code-block:: python

    import re

    try:
        # database operation
    except IntegrityError as exc:
        error_msg = str(exc)

        # Extract columns from DETAIL line
        match = re.search(r'Key \(([^)]+)\)', error_msg)
        if match:
            columns = match.group(1).split(', ')
            # Returns: ['network', 'station', 'component']

PostgreSQL Error Attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``exc.__cause__.diag`` object provides:

- ``constraint_name``: Name of violated constraint (if named)
- ``table_name``: Table where violation occurred
- ``schema_name``: Schema name
- ``column_name``: Column name (for some error types)

SQLite IntegrityError Format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQLite's error messages are more straightforward and always include column information.

Error Message Format
^^^^^^^^^^^^^^^^^^^^

.. code-block:: text

    UNIQUE constraint failed: table_name.column1, table_name.column2

Examples:

.. code-block:: text

    # Single column
    UNIQUE constraint failed: component.id

    # Multiple columns
    UNIQUE constraint failed: component.network, component.station, component.component

    # Association table
    UNIQUE constraint failed: soh_gps_association.component_id, soh_gps_association.soh_gps_id

Extracting Information
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import re
    from sqlalchemy.exc import IntegrityError

    try:
        # database operation
    except IntegrityError as exc:
        error_msg = str(exc)

        # Extract table and columns
        match = re.search(r'UNIQUE constraint failed: (\w+)\.(.+)', error_msg)
        if match:
            table_name = match.group(1)
            columns_str = match.group(2)

            # Parse columns (may have table prefix on each)
            columns = []
            for col in columns_str.split(', '):
                # Remove table name prefix if present
                col_name = col.split('.')[-1] if '.' in col else col
                columns.append(col_name)

            # Returns: table_name='component', columns=['network', 'station', 'component']

Key Differences
~~~~~~~~~~~~~~~

**PostgreSQL**:

- Provides constraint name (if named) via ``diag.constraint_name``
- Column names in DETAIL message: ``Key (col1, col2)=(val1, val2)``
- Columns are comma-space separated
- Does not prefix columns with table name

**SQLite**:

- Does not provide constraint names (even if defined)
- Column names in main error message
- Format: ``table.column1, table.column2`` (with or without table prefix)
- Always provides column information

Unified Approach
~~~~~~~~~~~~~~~~

A unified approach that works for both dialects:

.. code-block:: python

    import re
    from sqlalchemy.exc import IntegrityError

    def extract_constraint_columns(exc: IntegrityError, dialect: str) -> list[str]:
        """
        Extract column names from IntegrityError for both PostgreSQL and SQLite.

        Returns list of column names involved in the constraint violation.
        """
        error_msg = str(exc)

        if dialect == "postgresql":
            # Try to extract from DETAIL line
            match = re.search(r'Key \(([^)]+)\)', error_msg)
            if match:
                return [col.strip() for col in match.group(1).split(',')]

        elif dialect == "sqlite":
            # Extract from "UNIQUE constraint failed: ..." message
            match = re.search(r'UNIQUE constraint failed: \w+\.(.+)', error_msg)
            if match:
                columns_str = match.group(1)
                columns = []
                for col in columns_str.split(', '):
                    # Remove table prefix if present
                    col_name = col.split('.')[-1] if '.' in col else col
                    columns.append(col_name)
                return columns

        return []

This approach allows for enhanced error messages without relying on constraint names.

3. Flask-Migrate Baseline Migration Approach
---------------------------------------------

Current State
~~~~~~~~~~~~~

The project currently has a single baseline migration:

.. code-block:: text

    migrations/versions/5333b2f172f3_baseline_mixed_ids_ulids_for_data_.py

This was created on 2025-10-22 and serves as the initial migration capturing the current database schema with:

- ULID primary keys for data entities
- Integer primary keys for configuration models
- Named and unnamed unique constraints

Baseline Migration Strategy
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Approach**: Create a fresh baseline migration that replaces the existing one.

Steps for Creating New Baseline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. **Delete existing migration file**:

   .. code-block:: bash

       rm migrations/versions/5333b2f172f3_baseline_mixed_ids_ulids_for_data_.py

2. **Update all model files** with unnamed constraints (remove ``name=`` parameter)

3. **Generate new baseline migration**:

   .. code-block:: bash

       uv run flask db migrate -m "baseline_unnamed_constraints"

4. **Review the generated migration** to ensure:

   - All UniqueConstraint definitions lack names
   - Foreign keys are correct
   - Table creation order respects dependencies

5. **Test migration on fresh database**:

   .. code-block:: bash

       # Create test database
       uv run flask db upgrade

       # Verify schema
       uv run flask db current

6. **Test migration on existing database** (if applicable):

   .. code-block:: bash

       # Stamp existing database to mark it at this version
       uv run flask db stamp head

Special Considerations
^^^^^^^^^^^^^^^^^^^^^^

**For Existing Databases**:

If developers have existing databases at the old baseline, they have two options:

Option 1: Fresh start (recommended for development):

.. code-block:: bash

    # Drop existing database
    dropdb noiz_dev
    createdb noiz_dev

    # Apply new baseline
    uv run flask db upgrade

Option 2: Stamp to new baseline:

.. code-block:: bash

    # Mark database as being at the new baseline without running migrations
    uv run flask db stamp head

**Important**: Since we are replacing the initial baseline before any incremental migrations exist, this is the ideal time to make this change.
No complex migration path is needed.

**For CI/CD**:

The CI system always creates fresh databases, so it will automatically use the new baseline migration.
No special handling needed.

Migration File Structure
^^^^^^^^^^^^^^^^^^^^^^^^

The new baseline migration will follow the same structure:

.. code-block:: python

    """baseline_unnamed_constraints

    Revision ID: <new_id>
    Revises:
    Create Date: <timestamp>
    """
    from alembic import op
    import sqlalchemy as sa

    revision = '<new_id>'
    down_revision = None  # This is the first migration
    branch_labels = None
    depends_on = None

    def upgrade():
        # Create all tables with unnamed unique constraints
        op.create_table('device',
            sa.Column('id', sa.String(length=26), nullable=False),
            sa.Column('network', sa.UnicodeText(), nullable=True),
            sa.Column('station', sa.UnicodeText(), nullable=True),
            sa.PrimaryKeyConstraint('id'),
            # Unnamed constraint - no 'name' parameter
            sa.UniqueConstraint('network', 'station')
        )
        # ... more tables

    def downgrade():
        # Drop all tables
        op.drop_table('device')
        # ... more tables

Validation
^^^^^^^^^^

After creating the baseline:

1. **Verify unnamed constraints**:

   .. code-block:: bash

       grep -r "sa.UniqueConstraint.*name=" migrations/versions/

   Should return no results.

2. **Check migration applies cleanly**:

   .. code-block:: bash

       # Fresh database
       dropdb test_noiz && createdb test_noiz
       uv run flask db upgrade

       # Verify tables exist
       psql test_noiz -c "\dt"

3. **Verify constraints were created**:

   .. code-block:: bash

       # Check PostgreSQL constraints (will have auto-generated names)
       psql test_noiz -c "\d+ device"

Documentation Updates
^^^^^^^^^^^^^^^^^^^^^

Update developer documentation to mention:

- The baseline migration was recreated to use unnamed constraints
- Developers with existing databases should either drop and recreate or use ``flask db stamp head``
- All new migrations should use unnamed constraints

4. Constraint Patterns in the Codebase
---------------------------------------

Summary Statistics
~~~~~~~~~~~~~~~~~~

Total UniqueConstraint definitions: **35**

- Named constraints: **16**
- Unnamed constraints: **19**

Model files with constraints: **11**

Breakdown by Pattern
~~~~~~~~~~~~~~~~~~~~

**Single-Column Named Constraints** (4 total):

1. ``unique_starttime`` - 1 column (starttime)
2. ``unique_midtime`` - 1 column (midtime)
3. ``unique_endtime`` - 1 column (endtime)
4. ``unique_ppsd_per_config_per_datachunk`` - Special case, actually 2 columns

**Multi-Column Named Constraints** (12 total):

1. ``unique_device_per_station`` - 2 columns (network, station)
2. ``unique_component_per_station`` - 3 columns (network, station, component)
3. ``single_component_pair`` - 2 columns (component_a_id, component_b_id)
4. ``unique_timestamp_per_station_in_sohgps`` - 2 columns (datetime, z_component_id)
5. ``unique_timestamp_per_station_in_sohinstrument`` - 2 columns (datetime, z_component_id)
6. ``unique_tispan_per_station_in_avgsohgps`` - 2 columns (timespan_id, z_component_id)
7. ``unique_beam_per_config_per_timespan`` - 2 columns (timespan_id, beamforming_params_id)
8. ``unique_datachunk_per_timespan_per_station_per_processing`` - 3 columns (timespan_id, component_id, datachunk_params_id)
9. ``unique_qcone_results_per_config_per_datachunk`` - 2 columns (datachunk_id, qcone_config_id)
10. ``unique_stack_starttime_per_config`` - 2 columns (stacking_schema_id, starttime)
11. ``unique_stack_midtime_per_config`` - 2 columns (stacking_schema_id, midtime)
12. ``unique_stack_endtime_per_config`` - 2 columns (stacking_schema_id, endtime)

**Complex Multi-Column Named Constraints** (4 total):

1. ``unique_times`` - 3 columns (starttime, midtime, endtime)
2. ``unique_stack_times_per_config`` - 4 columns (stacking_schema_id, starttime, midtime, endtime)
3. ``unique_ccfn_per_timespan_per_componentpair_per_config`` - 3 columns (timespan_id, componentpair_id, crosscorrelation_cartesian_params_id)
4. ``unique_ccfcylindrical_per_timespan_cylindrical_per_config`` - 3 columns (timespan_id, componentpair_cylindrical_id, crosscorrelation_cylindrical_params_id)

**Additional Named Constraints** (not in CONSTRAINT_TO_COLUMNS):

5. ``unique_qctwo_results_per_config_per_ccf`` - 2 columns (crosscorrelation_cartesian_id, qctwo_config_id)
6. ``unique_stats_per_datachunk`` - 1 column (datachunk_id)
7. ``unique_processing_per_datachunk_per_config`` - 2 columns (datachunk_id, processed_datachunk_params_id)
8. ``unique_detection_per_timespan_per_datachunk_per_param_per_time`` - 4 columns (timespan_id, datachunk_id, event_detection_params_id, time_start)
9. ``unique_confirmation_per_timespan_per_param_per_time`` - 6 columns (timespan_id, event_confirmation_params_id, time_start, time_stop, peak_ground_velocity, number_station_triggered)
10. ``unique_stack_per_pair_per_config`` - 3 columns (stacking_timespan_id, componentpair_id, stacking_schema_id)

**Unnamed Two-Column Constraints** (10 total):

Association tables and join tables:

1. ``beamforming_result_id, datachunk_id``
2. ``beamforming_result_id, beamforming_peak_average_abspower_id``
3. ``beamforming_result_id, beamforming_peak_average_relpower_id``
4. ``beamforming_result_id, beamforming_peak_all_abspower_id``
5. ``beamforming_result_id, beamforming_peak_all_relpower_id``
6. ``component_id, soh_instrument_id``
7. ``component_id, soh_gps_id``
8. ``component_id, averaged_soh_gps_id``
9. ``event_confirmation_run_id, datachunk_id``

(Note: Item 10 would complete this list based on grep results)

Model Files with Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/component.py``

   - Device: ``unique_device_per_station`` (named)
   - Component: ``unique_component_per_station`` (named)

2. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/component_pair.py``

   - ComponentPair: ``single_component_pair`` (named)

3. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/timespan.py``

   - Timespan: 4 named constraints (unique_starttime, unique_midtime, unique_endtime, unique_times)

4. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/datachunk.py``

   - Datachunk: ``unique_datachunk_per_timespan_per_station_per_processing`` (named)
   - DatachunkStats: ``unique_stats_per_datachunk`` (named)
   - ProcessedDatachunk: ``unique_processing_per_datachunk_per_config`` (named)

5. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/crosscorrelation.py``

   - CrosscorrelationCartesian: ``unique_ccfn_per_timespan_per_componentpair_per_config`` (named)
   - CrosscorrelationCylindrical: ``unique_ccfcylindrical_per_timespan_cylindrical_per_config`` (named)

6. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/stacking.py``

   - StackingTimespan: 4 named constraints (stack timing constraints)
   - StackingResult: ``unique_stack_per_pair_per_config`` (named)

7. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/soh.py``

   - SohInstrument: ``unique_timestamp_per_station_in_sohinstrument`` (named)
   - SohGps: ``unique_timestamp_per_station_in_sohgps`` (named)
   - AveragedSohGps: ``unique_tispan_per_station_in_avgsohgps`` (named)
   - association_table_soh_instr: 1 unnamed constraint
   - association_table_soh_gps: 1 unnamed constraint
   - association_table_avg_soh_gps: 1 unnamed constraint

8. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/beamforming.py``

   - BeamformingResult: ``unique_beam_per_config_per_timespan`` (named)
   - 5 association tables: all with unnamed 2-column constraints

9. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/ppsd.py``

   - PPSDResult: ``unique_ppsd_per_config_per_datachunk`` (named)

10. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/qc.py``

    - QCOneResults: ``unique_qcone_results_per_config_per_datachunk`` (named)
    - QCTwoResults: ``unique_qctwo_results_per_config_per_ccf`` (named)

11. ``/Users/qsbt/noiz-group/noiz/src/noiz/models/event_detection.py``

    - EventDetectionResult: ``unique_detection_per_timespan_per_datachunk_per_param_per_time`` (named)
    - EventConfirmationRun: ``unique_confirmation_per_timespan_per_param_per_time`` (named)
    - association_table_event_confirmation_datachunks: 1 unnamed constraint

Constraint Pattern Categories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Category 1: Simple Entity Uniqueness** (3 constraints)

Ensure single instances per logical entity:

- ``unique_device_per_station``
- ``unique_component_per_station``
- ``single_component_pair``

**Category 2: Temporal Uniqueness** (4 constraints)

Ensure unique timespan definitions:

- ``unique_starttime``
- ``unique_midtime``
- ``unique_endtime``
- ``unique_times``

**Category 3: Result Uniqueness per Configuration** (10 constraints)

Ensure one result per input + processing configuration:

- ``unique_datachunk_per_timespan_per_station_per_processing``
- ``unique_ccfn_per_timespan_per_componentpair_per_config``
- ``unique_ccfcylindrical_per_timespan_cylindrical_per_config``
- ``unique_beam_per_config_per_timespan``
- ``unique_ppsd_per_config_per_datachunk``
- ``unique_qcone_results_per_config_per_datachunk``
- ``unique_qctwo_results_per_config_per_ccf``
- ``unique_processing_per_datachunk_per_config``
- ``unique_detection_per_timespan_per_datachunk_per_param_per_time``
- ``unique_confirmation_per_timespan_per_param_per_time``

**Category 4: State of Health (SOH) Temporal Uniqueness** (3 constraints)

One SOH record per station per timestamp:

- ``unique_timestamp_per_station_in_sohgps``
- ``unique_timestamp_per_station_in_sohinstrument``
- ``unique_tispan_per_station_in_avgsohgps``

**Category 5: Stacking Temporal Uniqueness** (5 constraints)

Ensure unique stacking time windows:

- ``unique_stack_starttime_per_config``
- ``unique_stack_midtime_per_config``
- ``unique_stack_endtime_per_config``
- ``unique_stack_times_per_config``
- ``unique_stack_per_pair_per_config``

**Category 6: Association Table Uniqueness** (10 constraints)

Many-to-many relationships (all unnamed):

- Beamforming associations (5 tables)
- SOH associations (3 tables)
- Event confirmation associations (1 table)

Current on_conflict_do_update Usage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Files using ``on_conflict_do_update`` directly:

1. ``/Users/qsbt/noiz-group/noiz/src/noiz/api/datachunk.py`` (3 uses)

   - Datachunk upsert: ``constraint="unique_datachunk_per_timespan_per_station_per_processing"``
   - DatachunkStats upsert: ``constraint="unique_stats_per_datachunk"``
   - ProcessedDatachunk upsert: ``constraint="unique_processing_per_datachunk_per_config"``

2. ``/Users/qsbt/noiz-group/noiz/src/noiz/api/ppsd.py`` (1 use)

   - PPSDResult upsert: ``constraint="unique_ppsd_per_config_per_datachunk"``

3. ``/Users/qsbt/noiz-group/noiz/src/noiz/api/event_detection.py`` (2 uses)

   - EventDetectionResult upsert: ``constraint="unique_detection_per_timespan_per_datachunk_per_param_per_time"``
   - EventConfirmationRun upsert: ``constraint="unique_confirmation_per_timespan_per_param_per_time"``

All current usages use the ``constraint=`` parameter with named constraints.
These will need to be updated to use ``index_elements=`` with column lists.

CONSTRAINT_TO_COLUMNS Mapping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Current mapping in ``/Users/qsbt/noiz-group/noiz/src/noiz/database.py`` contains 19 entries:

.. code-block:: python

    CONSTRAINT_TO_COLUMNS = {
        "unique_timestamp_per_station_in_sohgps": ["datetime", "z_component_id"],
        "unique_timestamp_per_station_in_sohinstrument": ["datetime", "z_component_id"],
        "unique_tispan_per_station_in_avgsohgps": ["timespan_id", "z_component_id"],
        "unique_ccfn_per_timespan_per_componentpair_per_config": [
            "timespan_id", "componentpair_id", "crosscorrelation_cartesian_params_id"
        ],
        "unique_beam_per_config_per_timespan": ["timespan_id", "beamforming_params_id"],
        "unique_datachunk_per_timespan_per_station_per_processing": [
            "timespan_id", "component_id", "datachunk_params_id"
        ],
        "unique_stack_per_pair_per_config": [
            "stacking_timespan_id", "componentpair_id", "stacking_schema_id"
        ],
        "unique_stack_starttime_per_config": ["stacking_schema_id", "starttime"],
        "unique_stack_midtime_per_config": ["stacking_schema_id", "midtime"],
        "unique_stack_endtime_per_config": ["stacking_schema_id", "endtime"],
        "unique_stack_times_per_config": [
            "stacking_schema_id", "starttime", "midtime", "endtime"
        ],
        "single_component_pair": ["component_a_id", "component_b_id"],
        "unique_ccfcylindrical_per_timespan_cylindrical_per_config": [
            "timespan_id", "componentpair_cylindrical_id",
            "crosscorrelation_cylindrical_params_id"
        ],
        "unique_starttime": ["starttime"],
        "unique_midtime": ["midtime"],
        "unique_endtime": ["endtime"],
        "unique_times": ["starttime", "midtime", "endtime"],
        "unique_qcone_results_per_config_per_datachunk": ["datachunk_id", "qcone_config_id"],
        "unique_qctwo_results_per_config_per_ccf": [
            "crosscorrelation_cartesian_id", "qctwo_config_id"
        ],
    }

This mapping will be used to transition from constraint names to direct column specification.
After the transition, this dictionary will no longer be needed and can be removed.

Missing from Mapping
~~~~~~~~~~~~~~~~~~~~

The following constraints exist in models but are NOT in CONSTRAINT_TO_COLUMNS:

- ``unique_stats_per_datachunk``
- ``unique_processing_per_datachunk_per_config``
- ``unique_detection_per_timespan_per_datachunk_per_param_per_time``
- ``unique_confirmation_per_timespan_per_param_per_time``
- ``unique_ppsd_per_config_per_datachunk``
- ``unique_device_per_station``
- ``unique_component_per_station``

These are used in on_conflict_do_update calls but not currently supported by the dialect_agnostic helper.
This confirms that the current code primarily works with PostgreSQL and uses constraint names directly.

Recommendations
---------------

1. SQLAlchemy Support
~~~~~~~~~~~~~~~~~~~~~

Confirmed: SQLAlchemy 1.4.54 fully supports ``index_elements`` for both PostgreSQL and SQLite.
No version upgrade needed.
Safe to proceed with using ``index_elements`` across both dialects.

2. Error Handling
~~~~~~~~~~~~~~~~~

Implement unified error extraction that works for both PostgreSQL and SQLite:

- PostgreSQL: Extract columns from ``Key (col1, col2)`` in DETAIL message
- SQLite: Extract columns from ``table.col1, table.col2`` in main message
- Use regex patterns for reliable extraction
- No dependency on constraint names

This enables enhanced error messages without requiring named constraints.

3. Migration Strategy
~~~~~~~~~~~~~~~~~~~~~

Recommended approach: **Replace the baseline migration**

Rationale:

- Only one migration exists (baseline created Oct 22, 2025)
- No incremental migrations to complicate the path
- Clean slate for unnamed constraints
- Developers can use ``flask db stamp head`` on existing databases
- CI always uses fresh databases

Implementation:

1. Delete existing baseline migration file
2. Update all model files to remove ``name=`` parameters
3. Generate new baseline with ``flask db migrate``
4. Test on fresh database
5. Document migration path for existing developer databases

4. Code Updates Required
~~~~~~~~~~~~~~~~~~~~~~~~

**Model Files** (11 files):

Remove ``name=`` parameter from 16 named UniqueConstraint definitions across:

- component.py (2 constraints)
- component_pair.py (1 constraint)
- timespan.py (4 constraints)
- datachunk.py (3 constraints)
- crosscorrelation.py (2 constraints)
- stacking.py (5 constraints)
- soh.py (3 constraints)
- beamforming.py (1 constraint)
- ppsd.py (1 constraint)
- qc.py (2 constraints)
- event_detection.py (2 constraints)

**API Files** (3 files):

Update ``on_conflict_do_update`` calls to use ``index_elements`` instead of ``constraint``:

- datachunk.py (3 calls)
- ppsd.py (1 call)
- event_detection.py (2 calls)

**Database Helper** (1 file):

Update or remove ``dialect_agnostic_on_conflict()`` function:

- Remove CONSTRAINT_TO_COLUMNS dictionary (19 entries)
- Simplify to always use ``index_elements``
- Update helper to accept columns directly instead of constraint names

**Total Impact**:

- 11 model files
- 3 API files
- 1 database helper file
- 16 named constraint definitions to update
- 6 on_conflict_do_update calls to update
- 1 migration file to replace

5. Testing Strategy
~~~~~~~~~~~~~~~~~~~

**Unit Tests**:

- Verify model definitions have no ``name=`` parameters
- Test constraint violations raise IntegrityError
- Verify error message parsing works for both dialects

**Integration Tests**:

- Test on_conflict_do_update with unnamed constraints
- Verify upsert operations work correctly
- Test both PostgreSQL and SQLite dialects

**System Tests**:

- Run full test suite with ``just run_system_tests``
- Run SQLite variant tests
- Verify all data insertion patterns work

**Migration Tests**:

- Test fresh database creation: ``flask db upgrade``
- Verify all tables and constraints created correctly
- Check constraint names in database (PostgreSQL auto-generates names)

Conclusion
----------

The research confirms that transitioning to unnamed constraints is fully supported by SQLAlchemy 1.4.54 for both PostgreSQL and SQLite dialects.
The ``index_elements`` parameter provides a robust, dialect-agnostic way to specify conflict resolution without relying on constraint names.

Both database systems provide sufficient error information to extract column names for enhanced error messages, making unnamed constraints viable without loss of debugging capability.

The project is at an ideal point to make this change: only one baseline migration exists, no complex migration history to manage, and the codebase impact is limited to 15 files with clear update patterns.

Next Steps
----------

1. Create tasks.md based on this research
2. Update model files to remove constraint names
3. Update API files to use index_elements
4. Replace baseline migration
5. Update database helper functions
6. Add enhanced error handling
7. Run comprehensive tests
8. Update documentation

References
----------

**SQLAlchemy Documentation**:

- SQLAlchemy 1.4 PostgreSQL Dialect: https://docs.sqlalchemy.org/en/14/dialects/postgresql.html
- Insert Operations: https://docs.sqlalchemy.org/en/14/orm/persistence_techniques.html

**Database Error Handling**:

- psycopg2 Error Handling: https://www.psycopg.org/docs/errors.html
- SQLite Error Messages: https://www.sqlite.org/rescode.html

**Flask-Migrate**:

- Flask-Migrate Documentation: https://flask-migrate.readthedocs.io/
- Alembic Migrations: https://alembic.sqlalchemy.org/

**Codebase Files Analyzed**:

- ``/Users/qsbt/noiz-group/noiz/src/noiz/database.py``
- ``/Users/qsbt/noiz-group/noiz/src/noiz/models/*.py`` (11 files)
- ``/Users/qsbt/noiz-group/noiz/src/noiz/api/*.py`` (3 files)
- ``/Users/qsbt/noiz-group/noiz/migrations/versions/5333b2f172f3_baseline_mixed_ids_ulids_for_data_.py``
- ``/Users/qsbt/noiz-group/noiz/pyproject.toml``

# CONSTRAINT_TO_COLUMNS Backup

**Date**: 2025-10-24
**Purpose**: Backup of current constraint name to column mappings before removal

This document preserves the original mapping dictionary that will be removed as part of the unnamed constraints feature.

## Original Mapping (from src/noiz/database.py)

```python
CONSTRAINT_TO_COLUMNS = {
    "unique_timestamp_per_station_in_sohgps": ["datetime", "z_component_id"],
    "unique_timestamp_per_station_in_sohinstrument": ["datetime", "z_component_id"],
    "unique_tispan_per_station_in_avgsohgps": ["timespan_id", "z_component_id"],
    "unique_ccfn_per_timespan_per_componentpair_per_config": [
        "timespan_id",
        "componentpair_id",
        "crosscorrelation_cartesian_params_id",
    ],
    "unique_beam_per_config_per_timespan": ["timespan_id", "beamforming_params_id"],
    "unique_datachunk_per_timespan_per_station_per_processing": [
        "timespan_id",
        "component_id",
        "datachunk_params_id",
    ],
    "unique_stack_per_pair_per_config": ["stacking_timespan_id", "componentpair_id", "stacking_schema_id"],
    "unique_stack_starttime_per_config": ["stacking_schema_id", "starttime"],
    "unique_stack_midtime_per_config": ["stacking_schema_id", "midtime"],
    "unique_stack_endtime_per_config": ["stacking_schema_id", "endtime"],
    "unique_stack_times_per_config": ["stacking_schema_id", "starttime", "midtime", "endtime"],
    "single_component_pair": ["component_a_id", "component_b_id"],
    "unique_ccfcylindrical_per_timespan_cylindrical_per_config": [
        "timespan_id",
        "componentpair_cylindrical_id",
        "crosscorrelation_cylindrical_params_id",
    ],
    "unique_starttime": ["starttime"],
    "unique_midtime": ["midtime"],
    "unique_endtime": ["endtime"],
    "unique_times": ["starttime", "midtime", "endtime"],
    "unique_qcone_results_per_config_per_datachunk": ["datachunk_id", "qcone_config_id"],
    "unique_qctwo_results_per_config_per_ccf": ["crosscorrelation_cartesian_id", "qctwo_config_id"],
}
```

## Statistics

- **Total Mappings**: 19
- **Purpose**: Translated PostgreSQL named constraints to SQLite column lists
- **Location**: src/noiz/database.py lines 47-79

## Usage Pattern

This dictionary was used by `dialect_agnostic_on_conflict()` function to convert:

**PostgreSQL**:
```python
insert_stmt.on_conflict_do_update(constraint="unique_qcone_results_per_config_per_datachunk", set_={...})
```

**SQLite**:
```python
# Looked up columns from CONSTRAINT_TO_COLUMNS
insert_stmt.on_conflict_do_update(index_elements=["datachunk_id", "qcone_config_id"], set_={...})
```

## Replacement Strategy

After this feature, both backends use:
```python
insert_stmt.on_conflict_do_update(index_elements=["datachunk_id", "qcone_config_id"], set_={...})
```

No translation layer needed - direct column references work for both PostgreSQL and SQLite.

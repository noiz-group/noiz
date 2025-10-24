# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

# -*- coding: utf-8 -*-
"""Database module, including the SQLAlchemy database object and DB-related utilities."""

# Alias common SQLAlchemy names
from functools import partial

from typing import Type

from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine

db = SQLAlchemy()
migrate = Migrate()


@event.listens_for(Engine, "connect")
def _configure_sqlite_connection(dbapi_connection, connection_record):
    """Configure SQLite connections with required PRAGMA settings."""
    # Only apply to SQLite connections
    if hasattr(dbapi_connection, "execute"):
        try:
            cursor = dbapi_connection.cursor()
            # Enable foreign key constraints (disabled by default in SQLite)
            cursor.execute("PRAGMA foreign_keys=ON")
            # Enable Write-Ahead Logging for better concurrency
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.close()
        except Exception:
            # Ignore if not SQLite or if PRAGMAs not supported
            pass


Column = db.Column
relationship = db.relationship
NullColumn: Type[db.Column] = partial(db.Column, nullable=True)  # type: ignore
NotNullColumn: Type[db.Column] = partial(db.Column, nullable=False)  # type: ignore


# Mapping of PostgreSQL constraint names to SQLite index_elements (columns)
# Used for converting on_conflict_do_update() from constraint= to index_elements=
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


def get_dialect_insert():
    """
    Get the appropriate insert function for the current database dialect.

    Returns the dialect-specific insert function that supports upsert operations.
    For PostgreSQL, uses the PostgreSQL-specific insert with on_conflict_do_update.
    For SQLite, uses the SQLite-specific insert with on_conflict_do_update.

    :return: Insert function for the current dialect
    :rtype: Callable
    """
    from sqlalchemy import insert

    # Check if we're using PostgreSQL or SQLite
    dialect_name = db.engine.dialect.name

    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        return pg_insert
    elif dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        return sqlite_insert
    else:
        # Fallback to generic insert
        return insert


def dialect_agnostic_on_conflict(insert_stmt, constraint_name=None, index_elements=None, set_=None):
    """
    Apply on_conflict_do_update in a database-agnostic way.

    For PostgreSQL, uses constraint name.
    For SQLite, converts constraint name to index_elements (column names).

    :param insert_stmt: The insert statement to apply conflict resolution to
    :param constraint_name: PostgreSQL constraint name
    :param index_elements: Column names for the unique constraint (for SQLite)
    :param set_: Dictionary of columns to update on conflict
    :return: Insert statement with conflict resolution
    """
    dialect_name = db.engine.dialect.name

    if dialect_name == "postgresql":
        # PostgreSQL supports named constraints
        return insert_stmt.on_conflict_do_update(constraint=constraint_name, set_=set_)
    elif dialect_name == "sqlite":
        # SQLite needs column names, not constraint names
        if index_elements is None and constraint_name:
            # Try to map constraint name to columns
            index_elements = CONSTRAINT_TO_COLUMNS.get(constraint_name)
            if index_elements is None:
                raise ValueError(
                    f"Unknown constraint '{constraint_name}'. Add mapping to CONSTRAINT_TO_COLUMNS in database.py"
                )
        return insert_stmt.on_conflict_do_update(index_elements=index_elements, set_=set_)
    else:
        # Fallback - try constraint name
        return insert_stmt.on_conflict_do_update(constraint=constraint_name, set_=set_)

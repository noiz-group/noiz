# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

# -*- coding: utf-8 -*-
"""Database module, including the SQLAlchemy database object and DB-related utilities."""

# Alias common SQLAlchemy names
from functools import partial
import re

from typing import Type

from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

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


def parse_constraint_violation(error: IntegrityError) -> tuple:
    """
    Parse constraint violation error to extract table name, columns, and values.

    :param error: SQLAlchemy IntegrityError
    :type error: IntegrityError
    :return: tuple of (table_name, column_list, value_dict)
    :rtype: tuple[str, list[str], dict]
    """
    error_msg = str(error.orig)
    dialect_name = db.engine.dialect.name

    if dialect_name == "postgresql":
        # PostgreSQL format: 'duplicate key value violates unique constraint "name"'
        # DETAIL: Key (col1, col2)=(val1, val2) already exists.

        # Extract table from constraint name if available
        table_match = re.search(r'violates unique constraint "(\w+)"', error_msg)
        table = table_match.group(1) if table_match else "unknown"

        # Extract columns from DETAIL
        detail_match = re.search(r"Key \(([^)]+)\)=\(([^)]+)\)", error_msg)
        if detail_match:
            columns = [col.strip() for col in detail_match.group(1).split(",")]
            values_str = detail_match.group(2).split(",")
            values = {col: val.strip() for col, val in zip(columns, values_str)}
            return table, columns, values

    elif dialect_name == "sqlite":
        # SQLite format: 'UNIQUE constraint failed: table.col1, table.col2'
        match = re.search(r"UNIQUE constraint failed: (\w+)\.([\w, .]+)", error_msg)
        if match:
            table = match.group(1)
            columns_str = match.group(2)
            # Handle both 'col1, table.col2' and 'col1, col2' formats
            columns = [col.split(".")[-1].strip() for col in columns_str.split(",")]
            return table, columns, {}

    # Fallback
    return "unknown", [], {}


def enhance_constraint_error(error: IntegrityError) -> Exception:
    """
    Convert SQLAlchemy IntegrityError to enhanced ConstraintViolationError.

    :param error: SQLAlchemy IntegrityError
    :type error: IntegrityError
    :return: ConstraintViolationError with parsed details
    :rtype: ConstraintViolationError
    """
    from noiz.exceptions import ConstraintViolationError

    table, columns, values = parse_constraint_violation(error)
    return ConstraintViolationError(error, table, columns, values)

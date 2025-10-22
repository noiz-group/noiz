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

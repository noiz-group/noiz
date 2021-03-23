# -*- coding: utf-8 -*-
"""Database module, including the SQLAlchemy database object and DB-related utilities."""

# Alias common SQLAlchemy names
from pathlib import Path

from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy.types as types


db = SQLAlchemy()
migrate = Migrate()


Column = db.Column
relationship = db.relationship


class PathInDB(types.TypeDecorator):
    '''
    Casts a :py:class:`pathlib.Path` object to string when adding to DB, brings it back to
     :py:class:`pathlib.Path` on the way back
    '''

    impl = types.UnicodeText

    def process_bind_param(self, value, dialect):
        return str(value)

    def process_result_value(self, value, dialect):
        return Path(value)

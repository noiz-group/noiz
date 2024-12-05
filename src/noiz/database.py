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

db = SQLAlchemy()
migrate = Migrate()


Column = db.Column
relationship = db.relationship
NullColumn: Type[db.Column] = partial(db.Column, nullable=True)  # type: ignore
NotNullColumn: Type[db.Column] = partial(db.Column, nullable=False)  # type: ignore

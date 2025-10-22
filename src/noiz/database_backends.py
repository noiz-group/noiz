# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

"""Database backend enumeration for SQLite and PostgreSQL support."""

from enum import Enum


class DatabaseBackend(str, Enum):
    """Supported database backends for Noiz."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"

    def __str__(self) -> str:
        return self.value

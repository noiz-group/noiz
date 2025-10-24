# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from typing import Optional


class NoizBaseException(Exception):
    pass


class MissingDataFileException(NoizBaseException):
    pass


class CorruptedMiniseedFileException(NoizBaseException):
    pass


class NoDataException(NoizBaseException):
    pass


class MissingProcessingStepError(NoizBaseException):
    pass


class CorruptedDataException(NoizBaseException):
    pass


class SohParsingException(NoizBaseException):
    pass


class UnparsableDateTimeException(NoizBaseException):
    pass


class NoSOHPresentException(NoizBaseException):
    pass


class EmptyResultException(NoizBaseException):
    pass


class InconsistentDataException(NoizBaseException):
    pass


class ObspyError(NoizBaseException):
    pass


class ResponseRemovalError(NoizBaseException):
    pass


class NotEnoughDataError(NoizBaseException):
    pass


class SubobjectNotLoadedError(NoizBaseException):
    pass


class ValidationError(NoizBaseException):
    pass


class ConstraintViolationError(NoizBaseException):
    """Raised when a database constraint is violated with enhanced error information."""

    def __init__(
        self,
        original_error: Exception,
        table: str,
        columns: list,
        values: Optional[dict] = None,
    ):
        self.original_error = original_error
        self.table = table
        self.columns = columns
        self.values = values if values is not None else {}

        column_list = ", ".join(columns)
        if values:
            value_list = ", ".join(f"{col}={values.get(col, 'N/A')}" for col in columns)
            message = f"Unique constraint violation on {table}({column_list}): Duplicate values {value_list}"
        else:
            message = f"Unique constraint violation on {table}({column_list})"

        super().__init__(message)

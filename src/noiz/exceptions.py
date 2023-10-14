# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

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

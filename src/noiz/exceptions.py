class NoizBaseException(Exception):
    pass


class MissingDataFileException(NoizBaseException):
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

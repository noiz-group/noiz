class NoizBaseException(Exception):
    pass


class MissingDataFileException(NoizBaseException):
    pass


class NoDataException(NoizBaseException):
    pass


class SohParsingException(NoizBaseException):
    pass


class UnparsableDateTimeException(NoizBaseException):
    pass


class NoSOHPresentException(NoizBaseException):
    pass


class EmptyResultException(NoizBaseException):
    pass

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from enum import Enum

import os


def _get_processed_data_dir():
    """
    Get PROCESSED_DATA_DIR from environment variable.

    This function reads the environment variable dynamically rather than
    at import time, which allows pytest fixtures to set it after module import.

    :return: Value of PROCESSED_DATA_DIR environment variable, or empty string
    :rtype: str
    """
    return os.environ.get("PROCESSED_DATA_DIR", "")


# For backwards compatibility, provide PROCESSED_DATA_DIR as a callable
# that returns the current value from environment
class _ProcessedDataDirProxy:
    """
    Proxy object that reads PROCESSED_DATA_DIR from environment on access.

    This allows the value to be determined dynamically at runtime rather than
    at import time, which is necessary for pytest fixtures to properly set
    the environment variable before it's used.
    """

    def __str__(self):
        return _get_processed_data_dir()

    def __repr__(self):
        return f"ProcessedDataDirProxy({_get_processed_data_dir()!r})"

    def __fspath__(self):
        """Support os.PathLike protocol for use with Path()."""
        return _get_processed_data_dir()

    def __bool__(self):
        return bool(_get_processed_data_dir())

    def __eq__(self, other):
        return str(self) == other

    def __hash__(self):
        return hash(str(self))


PROCESSED_DATA_DIR = _ProcessedDataDirProxy()


class ExtendedEnum(Enum):
    @classmethod
    def list(cls):
        return [c.value for c in cls]  # type: ignore

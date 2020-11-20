from enum import Enum

import os

PROCESSED_DATA_DIR = os.environ.get("PROCESSED_DATA_DIR", '')


class ExtendedEnum(Enum):

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))  # type: ignore

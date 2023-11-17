# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from enum import Enum

import os

PROCESSED_DATA_DIR = os.environ.get("NOIZ_PROCESSED_DATA_DIR", '')


class ExtendedEnum(Enum):

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))  # type: ignore

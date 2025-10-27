# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from enum import Enum


class ExtendedEnum(Enum):
    @classmethod
    def list(cls):
        return [c.value for c in cls]  # type: ignore

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from pathlib import Path

from sqlalchemy import types as types


class PathInDB(types.TypeDecorator):
    '''
    Casts a :py:class:`pathlib.Path` object to string when adding to DB, brings it back to
     :py:class:`pathlib.Path` on the way back
    '''

    impl = types.UnicodeText

    def process_bind_param(self, value, dialect):
        return str(value)

    def process_result_value(self, value, dialect):
        return Path(value)

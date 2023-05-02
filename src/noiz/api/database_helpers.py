# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from sqlalchemy import func, Column
from sqlalchemy.orm import Query

from noiz.database import db


def _get_maximum_value_of_column_incremented(table_column: Column, increment: int = 1) -> int:
    """
    Query the db for the max value of a column and returns it incremented by one.

    :param table_column: the Column of a databse table holding the value to be queried
    :type Column
    :param increment: the value used for the incrementation. default value is 1.
    :type int
    :return: Count the max value of the given column + increment
    :rtype: int
    """
    max_val = db.session.query(func.max(table_column)).first()
    if max_val[0] is None:
        count = 1
    else:
        count = max_val[0] + increment
    return count

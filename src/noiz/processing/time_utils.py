# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
import pandas as pd
from typing import Tuple, Union

from noiz.validation_helpers import validate_timedelta_as_pytimedelta


def get_year_doy(date: datetime.datetime) -> Tuple[int, int]:
    """
    Divides a datetime into year and day of year.
    :param date: date to be split
    :type date: datetime.datetime
    :return: Year and day of year of given datetime
    :rtype: Tuple[int, int]
    """
    year = date.year
    day_of_year = date.timetuple().tm_yday
    return year, day_of_year


def calculate_window_step_or_overlap(
        stacking_length: Union[pd.Timedelta, datetime.timedelta],
        stacking_step_or_overlap: Union[pd.Timedelta, datetime.timedelta],
) -> datetime.timedelta:
    return validate_timedelta_as_pytimedelta(stacking_length) - \
           validate_timedelta_as_pytimedelta(stacking_step_or_overlap)

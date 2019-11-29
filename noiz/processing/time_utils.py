import datetime
from typing import Tuple, Union

import numpy as np
import pandas as pd


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


def validate_timestamp(
    time_obj: Union[pd.Timestamp, datetime.datetime, np.datetime64]
) -> pd.Timestamp:
    """
    Takes a time object and converts it to a pd.Timestamp if originally it was either datetime.datetime,
    np.datetime64 or pd.Timestamp
    :param time_obj: Time object to be validated
    :type time_obj: Union[pd.Timestamp, datetime.datetime, np.datetime64]
    :return: Validated Timestamp
    :rtype: pd.Timestamp
    """
    if isinstance(time_obj, pd.Timestamp):
        return time_obj
    elif isinstance(time_obj, (datetime.datetime, np.datetime64)):
        return pd.Timestamp(time_obj)

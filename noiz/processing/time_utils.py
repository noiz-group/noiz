import datetime
from typing import Tuple, Union

import numpy as np
import pandas as pd


def get_year_doy(date: datetime.date) -> Tuple[int, int]:
    year = date.year
    day_of_year = date.timetuple().tm_yday
    return year, day_of_year


def validate_timestamp(
    time_obj: Union[pd.Timestamp, datetime.datetime, np.datetime64]
) -> pd.Timestamp:
    if isinstance(time_obj, pd.Timestamp):
        return time_obj
    elif isinstance(time_obj, (datetime.datetime, np.datetime64)):
        return pd.Timestamp(time_obj)

import datetime
from typing import Tuple


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

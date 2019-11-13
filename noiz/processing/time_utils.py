import datetime
from typing import Tuple


def get_year_doy(date: datetime.date) -> Tuple[int, int]:
    year = date.year
    day_of_year = date.timetuple().tm_yday
    return year, day_of_year

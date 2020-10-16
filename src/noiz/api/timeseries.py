import datetime
from typing import List

from noiz.exceptions import NoDataException
from noiz.models import Component, Tsindex


def fetch_raw_timeseries(
    component: Component, execution_date: datetime.datetime
) -> Tsindex:
    year = execution_date.year
    day_of_year = execution_date.timetuple().tm_yday
    time_series: List[Tsindex] = Tsindex.query.filter(
        Tsindex.network == component.network,
        Tsindex.station == component.station,
        Tsindex.component == component.component,
        Tsindex.starttime_year == year,
        Tsindex.starttime_doy == day_of_year,
    ).all()
    if len(time_series) > 1:
        raise ValueError(
            f"There are more then one files for that day in timeseries!"
            f" {component._make_station_string()} {year}.{day_of_year}"
        )
    elif len(time_series) == 0:
        raise NoDataException(f"No data for {component} on day {year}.{day_of_year}")
    else:
        return time_series[0]

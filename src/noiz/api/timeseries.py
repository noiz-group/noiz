import datetime
from flask import current_app as app
from typing import List
from pathlib import Path

from noiz.exceptions import NoDataException
from noiz.models.component import Component
from noiz.models.timeseries import Tsindex
from noiz.processing.tsindex import run_mseedindex_on_passed_dir


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


def add_seismic_data(
        basedir: Path,
        current_dir: Path,
        filename_pattern: str = "*",
):
    mseedindex_executable = app.config['MSEEDINDEX_EXECUTABLE']
    postgres_host = app.config['POSTGRES_HOST']
    postgres_user = app.config['POSTGRES_USER']
    postgres_password = app.config['POSTGRES_PASSWORD']
    postgres_db = app.config['POSTGRES_DB']

    run_mseedindex_on_passed_dir(
        basedir=basedir,
        current_dir=current_dir,
        filename_pattern=filename_pattern,
        mseedindex_executable=mseedindex_executable,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_db=postgres_db,
    )
    return

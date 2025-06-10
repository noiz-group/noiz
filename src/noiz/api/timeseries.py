# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
from flask import current_app as app
from pathlib import Path
from sqlalchemy.orm import Query
from typing import List, Collection, Union

from noiz.exceptions import NoDataException
from noiz.models.component import Component
from noiz.models.timeseries import Tsindex
from noiz.processing.timeseries import run_mseedindex_on_passed_dir
from noiz.models.timespan import Timespan


def fetch_raw_timeseries(component: Component, execution_date: datetime.datetime) -> Tsindex:
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
    basedir: Union[Path, Collection[Path]],
    current_dir: Path,
    filename_pattern: str = "*",
    parallel: bool = True,
) -> None:
    """
    Executes call to mseedindex app to add the seismic data from provided directory to the db.
    For connection with database uses information from the noiz application config.

    Requires to be run within app_context

    :param basedir: Directory to start search within
    :type basedir: Union[Path, Collection[Path]]
    :param current_dir: Current dir for execution
    :type current_dir: Path
    :param filename_pattern: Pattern to call rglob with on the basedir
    :type filename_pattern: str
    :return: None
    :rtype: NoneType
    """
    mseedindex_executable = app.config["MSEEDINDEX_EXECUTABLE"]
    postgres_host = app.config["POSTGRES_HOST"]
    postgres_user = app.config["POSTGRES_USER"]
    postgres_password = app.config["POSTGRES_PASSWORD"]
    postgres_db = app.config["POSTGRES_DB"]

    run_mseedindex_on_passed_dir(
        basedir=basedir,
        current_dir=current_dir,
        filename_pattern=filename_pattern,
        mseedindex_executable=mseedindex_executable,
        postgres_host=postgres_host,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_db=postgres_db,
        parallel=parallel,
    )
    return


def fetch_timeseries_for_component_timespan(component: Component, timespan: Timespan) -> List[Tsindex]:
    return _query_tsindex_by_time(
        component=component, starttime=timespan.starttime, endtime=timespan.remove_last_microsecond()
    ).all()


def count_timeseries_for_component_timespan(component: Component, timespan: Timespan) -> int:
    return _query_tsindex_by_time(
        component=component, starttime=timespan.starttime, endtime=timespan.remove_last_microsecond()
    ).count()


def _query_tsindex_by_time(component: Component, starttime: datetime.datetime, endtime: datetime.datetime) -> Query:
    query = Tsindex.query.filter(
        Tsindex.network == component.network,
        Tsindex.station == component.station,
        Tsindex.component == component.component,
        Tsindex.starttime <= starttime,
        Tsindex.endtime >= endtime,
    )
    return query

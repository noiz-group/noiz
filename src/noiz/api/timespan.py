# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
from obspy import UTCDateTime
import numpy as np
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query
from sqlalchemy.sql.elements import BinaryExpression
from typing import Iterable, List, Union, Optional, Generator, Any, Collection

from noiz.database import db
from noiz.models.timespan import Timespan
from noiz.processing.timespan import generate_timespans
from noiz.validation_helpers import validate_timestamp_as_pydatetime, validate_to_tuple


def create_and_insert_timespans_to_db(
    startdate: datetime.datetime,
    enddate: datetime.datetime,
    window_length: Union[float, int],
    window_overlap: Optional[Union[float, int]] = None,
    generate_over_midnight: bool = False,
    bulk_insert: bool = True,
    add_to_db: bool = False,
) -> Optional[Generator[Timespan, Any, Any]]:
    """
    Creates instances of :class:`noiz.models.timespan.Timespan` according to passed specifications.
    For generation of the series of those instances uses :func:`~noiz.processing.timespan.generate_timespans`
    It is being able to generate windows of specified length between two dates, with or without overlapping.
    It is also able to generate or not windows spanning over midnight since sometimes that can be problematic to have
    a window across two days.
    After generating of the windows, it can add them to DB or just return them for verification, depending on add_to_db.

    Important Note: both startdate and enddate will be normalized to midnight!

    :param startdate: Starttime for requested timespans. Warning! It will be normalized to midnight.
    :type startdate: datetime.datetime
    :param enddate: Endtime for requested timespans. Warning! It will be normalized to midnight.
    :type enddate: datetime.datetime
    :param window_length: Window length in seconds
    :type window_length: Union[int, float]
    :param window_overlap: Window overlap in seconds
    :type window_overlap: Optional[Union[int, float]]
    :param generate_over_midnight: If windows spanning over midnight should be included
    :type generate_over_midnight: bool
    :param bulk_insert: If bulk insert function should be used. Can be used to add missing timespans.
    :type bulk_insert: bool
    :param add_to_db: If timespans should be inserted into db or just returned
    :type add_to_db: bool
    :return: Optionally returns generated timespans for verification
    :rtype: Optional[Generator[Timespan, Any, Any]]
    """
    timespans = generate_timespans(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_over_midnight=generate_over_midnight,
    )
    if add_to_db:
        insert_timespans_into_db(timespans=timespans, bulk_insert=bulk_insert)
        return None
    else:
        return timespans


def insert_timespans_into_db(timespans: Iterable[Timespan], bulk_insert: bool) -> None:
    """
    Inserts provided timespans into database.
    It can either use SQLAlchemy's bulk_save_objects or Postgres specific insert with update_on_conflict.

    Warning: It has to be executed withing application context.

    :param timespans: Iterable of Timespans to be inserted
    :type timespans: Iterable[Timespan]
    :param bulk_insert: If use bulk_save_objects method
    :type bulk_insert: bool
    :return: None
    :rtype: None
    """
    # TODO add some logging messages

    if bulk_insert:
        db.session.bulk_save_objects(timespans)
        db.session.commit()
        return

    for ts in timespans:
        insert_command = (
            insert(Timespan)
            .values(starttime=ts.starttime, midtime=ts.midtime, endtime=ts.endtime)
            .on_conflict_do_update(
                constraint="unique_starttime",
                set_={"starttime": ts.starttime, "midtime": ts.midtime, "endtime": ts.endtime},
            )
            .on_conflict_do_update(
                constraint="unique_midtime",
                set_={"starttime": ts.starttime, "midtime": ts.midtime, "endtime": ts.endtime},
            )
            .on_conflict_do_update(
                constraint="unique_endtime",
                set_={"starttime": ts.starttime, "midtime": ts.midtime, "endtime": ts.endtime},
            )
            .on_conflict_do_update(
                constraint="unique_times",
                set_={"starttime": ts.starttime, "midtime": ts.midtime, "endtime": ts.endtime},
            )
        )
        db.session.execute(insert_command)
    db.session.commit()
    return


def fetch_timespans_for_doy(year: int, doy: int) -> List[Timespan]:
    """
    Fetches all timespans for a given day of year.
    It's based on timespan's midtime.

    Warning: It has to be executed withing application context.

    :param year: Year to be fetched
    :type year: int
    :param doy: Day of year to be fetched
    :type doy: int
    :return: List of all timespans on given day
    :rtype: List[Timespan]
    """
    timespans = Timespan.query.filter(Timespan.midtime_year == year, Timespan.midtime_doy == doy).all()
    return timespans


def fetch_timespans_between_dates(
    starttime: Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str],
    endtime: Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str],
) -> List[Timespan]:
    """
    Fetches all timespans between two times.
    It looks for Timespans that start after or at starttime
    and end before or at endtime.

    Warning: It has to be executed withing application context.

    :param starttime: Time after which to look for timespans
    :type starttime: Union[datetime.date, datetime.datetime, UTCDateTime]
    :param endtime: Time before which to look for timespans
    :type endtime: Union[datetime.date, datetime.datetime, UTCDateTime],
    :return: List of all timespans on given day
    :rtype: List[Timespan]
    """

    py_starttime = validate_timestamp_as_pydatetime(starttime)
    py_endtime = validate_timestamp_as_pydatetime(endtime)

    timespans = Timespan.query.filter(
        Timespan.starttime >= py_starttime,
        Timespan.endtime <= py_endtime,
    ).all()
    return timespans


def fetch_timespans(
    starttime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    endtime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    accepted_timespan_ids: Optional[Collection[int]] = None,
    rejected_timespan_ids: Optional[Collection[int]] = None,
    accepted_midtime_iso_dayofweek: Optional[Collection[int]] = None,
    rejected_midtime_iso_dayofweek: Optional[Collection[int]] = None,
    accepted_midtime_hours: Optional[Collection[int]] = None,
    rejected_midtime_hours: Optional[Collection[int]] = None,
    order_by_id: bool = True,
    query_just_id: bool = False,
) -> List[Timespan]:
    return _query_timespans(
        starttime=starttime,
        endtime=endtime,
        accepted_timespan_ids=accepted_timespan_ids,
        rejected_timespan_ids=rejected_timespan_ids,
        accepted_midtime_iso_dayofweek=accepted_midtime_iso_dayofweek,
        rejected_midtime_iso_dayofweek=rejected_midtime_iso_dayofweek,
        accepted_midtime_hours=accepted_midtime_hours,
        rejected_midtime_hours=rejected_midtime_hours,
        order_by_id=order_by_id,
        query_just_id=query_just_id,
    ).all()


def count_timespans(
    starttime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    endtime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    accepted_timespan_ids: Optional[Collection[int]] = None,
    rejected_timespan_ids: Optional[Collection[int]] = None,
    accepted_midtime_iso_dayofweek: Optional[Collection[int]] = None,
    rejected_midtime_iso_dayofweek: Optional[Collection[int]] = None,
    accepted_midtime_hours: Optional[Collection[int]] = None,
    rejected_midtime_hours: Optional[Collection[int]] = None,
    order_by_id: bool = True,
) -> int:
    return _query_timespans(
        starttime=starttime,
        endtime=endtime,
        accepted_timespan_ids=accepted_timespan_ids,
        rejected_timespan_ids=rejected_timespan_ids,
        accepted_midtime_iso_dayofweek=accepted_midtime_iso_dayofweek,
        rejected_midtime_iso_dayofweek=rejected_midtime_iso_dayofweek,
        accepted_midtime_hours=accepted_midtime_hours,
        rejected_midtime_hours=rejected_midtime_hours,
        order_by_id=order_by_id,
        query_just_id=True,
    ).count()


def _query_timespans(
    starttime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    endtime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    accepted_timespan_ids: Optional[Collection[int]] = None,
    rejected_timespan_ids: Optional[Collection[int]] = None,
    accepted_midtime_iso_dayofweek: Optional[Collection[int]] = None,
    rejected_midtime_iso_dayofweek: Optional[Collection[int]] = None,
    accepted_midtime_hours: Optional[Collection[int]] = None,
    rejected_midtime_hours: Optional[Collection[int]] = None,
    order_by_id: bool = True,
    query_just_id: bool = False,
) -> Query:
    filters = _determine_filters_and_opts_for_timespan(
        starttime=starttime,
        endtime=endtime,
        accepted_timespan_ids=accepted_timespan_ids,
        rejected_timespan_ids=rejected_timespan_ids,
        accepted_midtime_iso_dayofweek=accepted_midtime_iso_dayofweek,
        rejected_midtime_iso_dayofweek=rejected_midtime_iso_dayofweek,
        accepted_midtime_hours=accepted_midtime_hours,
        rejected_midtime_hours=rejected_midtime_hours,
    )
    if query_just_id:
        basequery = db.session.query(Timespan.id)
    else:
        basequery = db.session.query(Timespan)

    if order_by_id:
        return basequery.filter(*filters).order_by(Timespan.id)
    else:
        return basequery.filter(*filters)


def _determine_filters_and_opts_for_timespan(
    starttime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    endtime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    accepted_timespan_ids: Optional[Collection[int]] = None,
    rejected_timespan_ids: Optional[Collection[int]] = None,
    accepted_midtime_iso_dayofweek: Optional[Collection[int]] = None,
    rejected_midtime_iso_dayofweek: Optional[Collection[int]] = None,
    accepted_midtime_hours: Optional[Collection[int]] = None,
    rejected_midtime_hours: Optional[Collection[int]] = None,
) -> Union[List[BinaryExpression], List[bool]]:
    filters = []
    if starttime is not None:
        py_starttime = validate_timestamp_as_pydatetime(starttime)
        filters.append(Timespan.starttime >= py_starttime)
    if endtime is not None:
        py_endtime = validate_timestamp_as_pydatetime(endtime)
        filters.append(Timespan.starttime <= py_endtime)
    if accepted_timespan_ids is not None:
        filters.append(Timespan.id.in_(validate_to_tuple(accepted_timespan_ids, int)))  # type: ignore
    if rejected_timespan_ids is not None:
        filters.append(~Timespan.id.in_(validate_to_tuple(rejected_timespan_ids, int)))  # type: ignore
    if accepted_midtime_iso_dayofweek is not None:
        filters.append(
            Timespan.midtime_isoweekday.in_(  # type: ignore
                validate_to_tuple(accepted_midtime_iso_dayofweek, int)
            )
        )  # type: ignore
    if rejected_midtime_iso_dayofweek is not None:
        filters.append(
            ~Timespan.midtime_isoweekday.in_(  # type: ignore
                validate_to_tuple(rejected_midtime_iso_dayofweek, int)
            )
        )  # type: ignore
    if accepted_midtime_hours is not None:
        filters.append(Timespan.midtime_hour.in_(validate_to_tuple(accepted_midtime_hours, int)))  # type: ignore
    if rejected_midtime_hours is not None:
        filters.append(~Timespan.midtime_hour.in_(validate_to_tuple(rejected_midtime_hours, int)))  # type: ignore
    if len(filters) == 0:
        filters.append(True)
    return filters

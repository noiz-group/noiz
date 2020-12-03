import datetime
from obspy import UTCDateTime
from sqlalchemy.dialects.postgresql import insert
from typing import Iterable, List, Union, Optional, Generator, Any

from noiz.database import db
from noiz.models.timespan import Timespan
from noiz.processing.timespan import generate_timespans


def create_and_insert_timespans_to_db(
    startdate: datetime.datetime,
    enddate: datetime.datetime,
    window_length: Union[float, int],
    window_overlap: Optional[Union[float, int]] = None,
    generate_over_midnight: bool = False,
    add_to_db: bool = False
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
        insert_timespans_into_db(timespans=timespans, bulk_insert=True)
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

    con = db.session.connection()
    for ts in timespans:
        insert_command = (
            insert(Timespan)
            .values(starttime=ts.starttime, midtime=ts.midtime, endtime=ts.endtime)
            .on_conflict_do_update(
                constraint="unique_starttime",
                set_=dict(
                    starttime=ts.starttime, midtime=ts.midtime, endtime=ts.endtime
                ),
            )
            .on_conflict_do_update(
                constraint="unique_midtime",
                set_=dict(
                    starttime=ts.starttime, midtime=ts.midtime, endtime=ts.endtime
                ),
            )
            .on_conflict_do_update(
                constraint="unique_endtime",
                set_=dict(
                    starttime=ts.starttime, midtime=ts.midtime, endtime=ts.endtime
                ),
            )
            .on_conflict_do_update(
                constraint="unique_times",
                set_=dict(
                    starttime=ts.starttime, midtime=ts.midtime, endtime=ts.endtime
                ),
            )
        )
        con.execute(insert_command)
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
    timespans = Timespan.query.filter(
        Timespan.midtime_year == year, Timespan.midtime_doy == doy
    ).all()
    return timespans


def fetch_timespans_between_dates(
        starttime: Union[datetime.date, datetime.datetime, UTCDateTime],
        endtime: Union[datetime.date, datetime.datetime, UTCDateTime],
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

    if isinstance(starttime, UTCDateTime):
        starttime = starttime.datetime
    elif not isinstance(starttime, (datetime.date, datetime.datetime)):
        raise ValueError(f"And starttime was expecting either "
                         f"datetime.date, datetime.datetime or UTCDateTime objects."
                         f"Got instance of {type(starttime)}")

    if isinstance(endtime, UTCDateTime):
        endtime = endtime.datetime
    elif not isinstance(endtime, (datetime.date, datetime.datetime)):
        raise ValueError(f"And endtime was expecting either "
                         f"datetime.date, datetime.datetime or UTCDateTime objects."
                         f"Got instance of {type(endtime)}")

    timespans = Timespan.query.filter(
        Timespan.starttime >= starttime,
        Timespan.endtime <= endtime,
        ).all()
    return timespans

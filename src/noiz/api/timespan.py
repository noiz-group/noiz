from sqlalchemy.dialects.postgresql import insert
from typing import Iterable, List, Union

from datetime import date, datetime
from obspy import UTCDateTime

from noiz.database import db
from noiz.models.timespan import Timespan


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
        starttime: Union[date, datetime, UTCDateTime],
        endtime: Union[date, datetime, UTCDateTime],
) -> List[Timespan]:
    """
    Fetches all timespans between two times.
    It looks for Timespans that start after or at starttime
    and end before or at endtime.

    Warning: It has to be executed withing application context.

    :param starttime: Time after which to look for timespans
    :type starttime: Union[date, datetime, UTCDateTime]
    :param endtime: Time before which to look for timespans
    :type endtime: Union[date, datetime, UTCDateTime],
    :return: List of all timespans on given day
    :rtype: List[Timespan]
    """

    if isinstance(starttime, UTCDateTime):
        starttime = starttime.datetime
    elif not isinstance(starttime, (date, datetime)):
        raise ValueError(f"And starttime was expecting either "
                         f"date, datetime or UTCDateTime objects."
                         f"Got instance of {type(starttime)}")

    if isinstance(endtime, UTCDateTime):
        endtime = endtime.datetime
    elif not isinstance(endtime, (date, datetime)):
        raise ValueError(f"And endtime was expecting either "
                         f"date, datetime or UTCDateTime objects."
                         f"Got instance of {type(endtime)}")

    timespans = Timespan.query.filter(
        Timespan.starttime >= starttime,
        Timespan.endtime <= endtime,
        ).all()
    return timespans

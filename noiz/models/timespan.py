import datetime
from typing import Union, Optional, Tuple, Iterable

import numpy as np
import pandas as pd

from noiz.database import db
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import extract, func
from sqlalchemy.ext.hybrid import hybrid_property


from flask.logging import logging

logger = logging.getLogger(__name__)


class Timespan(db.Model):
    __tablename__ = "timespan"
    __table_args__ = (
        db.UniqueConstraint("starttime", name="unique_starttime"),
        db.UniqueConstraint("midtime", name="unique_midtime"),
        db.UniqueConstraint("endtime", name="unique_endtime"),
        db.UniqueConstraint("starttime", "midtime", "endtime", name="unique_times"),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    midtime = db.Column("midtime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)

    @hybrid_property
    def starttime_year(self):
        return self.starttime.year

    @starttime_year.expression
    def starttime_year(cls):
        return func.date_part("year", cls.starttime)

    @hybrid_property
    def starttime_doy(self):
        return self.starttime.timetuple().tm_yday

    @starttime_doy.expression
    def starttime_doy(cls):
        return func.date_part("doy", cls.starttime)

    @hybrid_property
    def midtime_year(self):
        return self.midtime.year

    @midtime_year.expression
    def midtime_year(cls):
        return func.date_part("year", cls.midtime)

    @hybrid_property
    def midtime_doy(self):
        return self.midtime.timetuple().tm_yday

    @midtime_doy.expression
    def midtime_doy(cls):
        return func.date_part("doy", cls.midtime)

    @hybrid_property
    def endtime_year(self):
        return self.endtime.year

    @endtime_year.expression
    def endtime_year(cls):
        return func.date_part("year", cls.endtime)

    @hybrid_property
    def endtime_doy(self):
        return self.endtime.timetuple().tm_yday

    @endtime_doy.expression
    def endtime_doy(cls):
        return func.date_part("doy", cls.endtime)

    def remove_last_nanosecond(self):
        return self.endtime - pd.Timedelta(1)

    def same_day(self):
        return self.starttime.floor("D") == (self.endtime - pd.Timedelta(1)).floor("D")


def generate_starttimes_endtimes(
    startdate: Union[datetime.datetime, np.datetime64],
    enddate: Union[datetime.datetime, np.datetime64],
    window_length: Union[float, int, pd.Timedelta, np.timedelta64],
    window_overlap: Optional[Union[float, int, pd.Timedelta, np.timedelta64]] = None,
    generate_midtimes: bool = False,
) -> Union[
    Tuple[pd.DatetimeIndex, pd.DatetimeIndex],
    Tuple[pd.DatetimeIndex, pd.DatetimeIndex, pd.DatetimeIndex],
]:
    """

    :param startdate: Starting day of the dates range. Will be rounded to midnight.
    :type startdate: Union[datetime.datetime, np.datetime64]
    :param enddate:  Starting day of the dates range. Will be rounded to midnight.
    :type enddate: Union[datetime.datetime, np.datetime64],
    :param window_length: Length of the window. Should be number of seconds or timedelta.
    :type window_length: Union[float, int, pd.Timedelta, np.timedelta64]
    :param window_overlap: Length of overlap. Should be number of seconds or timedelta. Defaults to None.
    :type window_overlap: Optional[Union[float, int, pd.Timedelta, np.timedelta64]
    :param generate_midtimes: Generating midtimes flag. If True, there will be generated third timeindex
    representing midtime of each window.
    :type generate_midtimes: bool
    :return: Returns two iterables, first with starttimes second with endtimes.
    :rtype: Tuple[pd.DatetimeIndex, pd.DatetimeIndex]
    """

    if isinstance(window_length, (float, int)):
        window_length = pd.Timedelta(window_length, "s")
    if isinstance(window_overlap, (float, int)):
        window_overlap = pd.Timedelta(window_overlap, "s")

    if window_overlap is None:
        starttime_freq = window_length
    elif isinstance(window_overlap, (pd.Timedelta, np.timedelta64)):
        starttime_freq = window_length - window_overlap
        if window_overlap >= window_length:
            raise ValueError(
                f"The overlap time `{window_overlap}` cannot be equal or longer than window length `{window_length}`"
            )
    else:
        raise ValueError(
            "The overlap is expected to be eitherint or float of seconds \
                         or pd.Timedelta or np.timedelta64"
        )

    starttimes = pd.date_range(
        start=startdate, end=enddate, freq=starttime_freq, normalize=True
    )
    endtimes = starttimes + window_length
    if not generate_midtimes:
        return starttimes.to_list(), endtimes.to_list()
    else:
        midtimes = starttimes + window_length / 2
        return starttimes.to_list(), midtimes.to_list(), endtimes.to_list()


def generate_timespans(
    startdate: Union[datetime.datetime, np.datetime64],
    enddate: Union[datetime.datetime, np.datetime64],
    window_length: Union[float, int, pd.Timedelta, np.timedelta64],
    window_overlap: Optional[Union[float, int, pd.Timedelta, np.timedelta64]] = None,
    generate_over_midnight: bool = False,
) -> Iterable[Timespan]:
    timespans = generate_starttimes_endtimes(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_midtimes=True,
    )

    if generate_over_midnight:
        for starttime, midtime, endtime in zip(*timespans):
            yield Timespan(starttime=starttime, midtime=midtime, endtime=endtime)
    else:
        for starttime, midtime, endtime in zip(*timespans):
            timespan = Timespan(starttime=starttime, midtime=midtime, endtime=endtime)
            if timespan.same_day():
                yield timespan


def insert_timespans_into_db(app, timespans, bulk_insert):
    if bulk_insert:
        with app.app_context() as ctx:
            db.session.bulk_save_objects(timespans)
            db.session.commit()
    else:
        with app.app_context() as ctx:
            con = db.session.connection()
            for ts in timespans:
                insert_command = (
                    insert(Timespan)
                    .values(
                        starttime=ts.starttime, midtime=ts.midtime, endtime=ts.endtime
                    )
                    .on_conflict_do_update(
                        constraint="unique_starttime",
                        set_=dict(
                            starttime=ts.starttime,
                            midtime=ts.midtime,
                            endtime=ts.endtime,
                        ),
                    )
                    .on_conflict_do_update(
                        constraint="unique_midtime",
                        set_=dict(
                            starttime=ts.starttime,
                            midtime=ts.midtime,
                            endtime=ts.endtime,
                        ),
                    )
                    .on_conflict_do_update(
                        constraint="unique_endtime",
                        set_=dict(
                            starttime=ts.starttime,
                            midtime=ts.midtime,
                            endtime=ts.endtime,
                        ),
                    )
                    .on_conflict_do_update(
                        constraint="unique_times",
                        set_=dict(
                            starttime=ts.starttime,
                            midtime=ts.midtime,
                            endtime=ts.endtime,
                        ),
                    )
                )
                con.execute(insert_command)

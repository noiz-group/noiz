import obspy
import pandas as pd

from noiz.database import db
from sqlalchemy import func
from sqlalchemy.ext.hybrid import hybrid_property

import logging


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
    datachunks = db.relationship("DataChunk")

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
        return obspy.UTCDateTime(self.endtime - pd.Timedelta(1000))

    def same_day(self):
        return self.starttime.floor("D") == (self.endtime - pd.Timedelta(1)).floor("D")

    def starttime_obspy(self):
        return obspy.UTCDateTime(self.starttime)

    def midtime_obspy(self):
        return obspy.UTCDateTime(self.midtime)

    def endtime_obspy(self):
        return obspy.UTCDateTime(self.endtime)

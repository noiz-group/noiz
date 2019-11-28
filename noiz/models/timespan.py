import obspy
import pandas as pd

from noiz.database import db
from sqlalchemy import func
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Use this to make hybrid_property's have the same typing as a normal property until stubs are improved.
    typed_hybrid_property = property
else:
    from sqlalchemy.ext.hybrid import hybrid_property as typed_hybrid_property

from noiz.processing.time_utils import validate_timestamp


class TimespanModel(db.Model):
    __abstract__ = True
    id: int = db.Column("id", db.BigInteger, primary_key=True)
    starttime: pd.Timestamp = db.Column(
        "starttime", db.TIMESTAMP(timezone=True), nullable=False
    )
    midtime: pd.Timestamp = db.Column(
        "midtime", db.TIMESTAMP(timezone=True), nullable=False
    )
    endtime: pd.Timestamp = db.Column(
        "endtime", db.TIMESTAMP(timezone=True), nullable=False
    )

    def __init__(self, **kwargs):
        super(TimespanModel, self).__init__(**kwargs)
        self.starttime: pd.Timestamp = validate_timestamp(kwargs.get("starttime"))
        self.midtime: pd.Timestamp = validate_timestamp(kwargs.get("midtime"))
        self.endtime: pd.Timestamp = validate_timestamp(kwargs.get("endtime"))

    @typed_hybrid_property
    def starttime_year(self) -> int:
        return self.starttime.year

    @starttime_year.expression
    def starttime_year(cls) -> int:
        return func.date_part("year", cls.starttime)

    @typed_hybrid_property
    def starttime_doy(self) -> int:
        return self.starttime.dayofyear

    @starttime_doy.expression
    def starttime_doy(cls) -> int:
        return func.date_part("doy", cls.starttime)

    @typed_hybrid_property
    def midtime_year(self) -> int:
        return self.midtime.year

    @midtime_year.expression
    def midtime_year(cls) -> int:
        return func.date_part("year", cls.midtime)

    @typed_hybrid_property
    def midtime_doy(self) -> int:
        return self.midtime.dayofyear

    @midtime_doy.expression
    def midtime_doy(cls) -> int:
        return func.date_part("doy", cls.midtime)

    @typed_hybrid_property
    def endtime_year(self) -> int:
        return self.endtime.year

    @endtime_year.expression
    def endtime_year(cls) -> int:
        return func.date_part("year", cls.endtime)

    @typed_hybrid_property
    def endtime_doy(self) -> int:
        return self.endtime.dayofyear

    @endtime_doy.expression
    def endtime_doy(cls) -> int:
        return func.date_part("doy", cls.endtime)

    def remove_last_microsecond(self) -> obspy.UTCDateTime:
        return obspy.UTCDateTime(self.endtime - pd.Timedelta(microseconds=1))

    def same_day(self) -> bool:
        """
        Checks if starttime and endtime are effectively the same day.
        It mean that it checks if an endtime with 10^-9 s removed is still the same day that starttime.
        It's usefull if you want to check if generated timespan crosses the midnight or not.
        :return: Check if timespan crosses midnight
        :rtype: bool
        """
        return self.starttime.floor("D") == (self.endtime - pd.Timedelta(1)).floor("D")

    def starttime_obspy(self) -> obspy.UTCDateTime:
        return obspy.UTCDateTime(self.starttime)

    def midtime_obspy(self) -> obspy.UTCDateTime:
        return obspy.UTCDateTime(self.midtime)

    def endtime_obspy(self) -> obspy.UTCDateTime:
        return obspy.UTCDateTime(self.endtime)


class Timespan(TimespanModel):
    __tablename__ = "timespan"
    __table_args__ = (
        db.UniqueConstraint("starttime", name="unique_starttime"),
        db.UniqueConstraint("midtime", name="unique_midtime"),
        db.UniqueConstraint("endtime", name="unique_endtime"),
        db.UniqueConstraint("starttime", "midtime", "endtime", name="unique_times"),
    )

    datachunks = db.relationship("DataChunk")

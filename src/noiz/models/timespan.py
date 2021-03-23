import datetime
import obspy
import pandas as pd
from sqlalchemy import func
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    # Use this to make hybrid_property's have the same typing as a normal property until stubs are improved.
    typed_hybrid_property = property
else:
    from sqlalchemy.ext.hybrid import hybrid_property as typed_hybrid_property

from noiz.database import db
from noiz.validation_helpers import _validate_timestamp_as_pydatetime


class TimespanModel(db.Model):
    __abstract__ = True
    id: int = db.Column("id", db.BigInteger, primary_key=True)
    starttime: datetime.datetime = db.Column(
        "starttime", db.TIMESTAMP(timezone=True), nullable=False
    )
    midtime: datetime.datetime = db.Column(
        "midtime", db.TIMESTAMP(timezone=True), nullable=False
    )
    endtime: datetime.datetime = db.Column(
        "endtime", db.TIMESTAMP(timezone=True), nullable=False
    )

    def __init__(self, **kwargs):
        super(TimespanModel, self).__init__(**kwargs)
        self.starttime: datetime.datetime = _validate_timestamp_as_pydatetime(kwargs.get("starttime"))
        self.midtime: datetime.datetime = _validate_timestamp_as_pydatetime(kwargs.get("midtime"))
        self.endtime: datetime.datetime = _validate_timestamp_as_pydatetime(kwargs.get("endtime"))

    def __repr__(self):
        return f"Timespan id: {self.id} from {self.starttime} -- {self.endtime}"

    @typed_hybrid_property
    def starttime_year(self) -> int:
        return self.starttime.year  # type: ignore

    @property
    def starttime_pd(self) -> pd.Timestamp:
        return pd.Timestamp(self.starttime)

    @starttime_year.expression  # type: ignore
    def starttime_year(cls) -> int:  # type: ignore
        return func.date_part("year", cls.starttime)  # type: ignore

    @typed_hybrid_property
    def starttime_doy(self) -> int:
        return self.starttime.timetuple().tm_yday

    @starttime_doy.expression
    def starttime_doy(cls) -> int:
        return func.date_part("doy", cls.starttime)  # type: ignore

    @property
    def midtime_pd(self) -> pd.Timestamp:
        return pd.Timestamp(self.midtime)

    @typed_hybrid_property
    def midtime_year(self) -> int:
        return self.midtime.year

    @midtime_year.expression
    def midtime_year(cls) -> int:
        return func.date_part("year", cls.midtime)  # type: ignore

    @typed_hybrid_property
    def midtime_doy(self) -> int:
        return self.midtime.timetuple().tm_yday

    @midtime_doy.expression
    def midtime_doy(cls) -> int:
        return func.date_part("doy", cls.midtime)  # type: ignore

    @property
    def endtime_pd(self) -> pd.Timestamp:
        return pd.Timestamp(self.endtime)

    @typed_hybrid_property
    def endtime_year(self) -> int:
        return self.endtime.year

    @endtime_year.expression
    def endtime_year(cls) -> int:
        return func.date_part("year", cls.endtime)  # type: ignore

    @typed_hybrid_property
    def endtime_doy(self) -> int:
        return self.endtime.timetuple().tm_yday

    @endtime_doy.expression
    def endtime_doy(cls) -> int:
        return func.date_part("doy", cls.endtime)  # type: ignore

    def remove_last_microsecond(self) -> obspy.UTCDateTime:
        return obspy.UTCDateTime(self.endtime_pd - pd.Timedelta(microseconds=1))

    def endtime_at_last_sample(self, sampling_rate: Union[int, float]) -> pd.Timestamp:
        return self.endtime_pd - pd.Timedelta(seconds=1/sampling_rate)

    def same_day(self) -> bool:
        """
        Checks if starttime and endtime are effectively the same day.
        It mean that it checks if an endtime with 10^-9 s removed is still the same day that starttime.
        It's useful if you want to check if generated timespan crosses the midnight or not.

        :return: Check if timespan crosses midnight
        :rtype: bool
        """
        return self.starttime_pd.floor("D") == (self.endtime_pd - pd.Timedelta(1)).floor("D")

    # TODO convert to property
    def starttime_obspy(self) -> obspy.UTCDateTime:
        return obspy.UTCDateTime(self.starttime)

    def midtime_obspy(self) -> obspy.UTCDateTime:
        return obspy.UTCDateTime(self.midtime)

    def endtime_obspy(self) -> obspy.UTCDateTime:
        return obspy.UTCDateTime(self.endtime)

    @property
    def length(self) -> pd.Timedelta:
        return self.endtime_pd - self.starttime_pd


class Timespan(TimespanModel):
    __tablename__ = "timespan"
    __table_args__ = (
        db.UniqueConstraint("starttime", name="unique_starttime"),
        db.UniqueConstraint("midtime", name="unique_midtime"),
        db.UniqueConstraint("endtime", name="unique_endtime"),
        db.UniqueConstraint("starttime", "midtime", "endtime", name="unique_times"),
    )

    datachunks = db.relationship("Datachunk")

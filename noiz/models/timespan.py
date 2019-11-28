import obspy
import pandas as pd

from noiz.database import db
from sqlalchemy import func
from sqlalchemy.ext.hybrid import hybrid_property

from noiz.processing.time_utils import validate_timestamp


class TimespanModel(db.Model):
    __abstract__ = True
    id = db.Column("id", db.BigInteger, primary_key=True)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    midtime = db.Column("midtime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)

    def __init__(self, **kwargs):
        super(TimespanModel, self).__init__(**kwargs)
        self.starttime = validate_timestamp(kwargs.get("starttime"))
        self.midtime = validate_timestamp(kwargs.get("midtime"))
        self.endtime = validate_timestamp(kwargs.get("endtime"))

    @hybrid_property  # type: ignore
    def starttime_year(self) -> int:
        return self.starttime.year

    @starttime_year.expression  # type: ignore
    def starttime_year(cls) -> int:
        return func.date_part("year", cls.starttime)

    @hybrid_property  # type: ignore
    def starttime_doy(self) -> int:
        return self.starttime.timetuple().tm_yday

    @starttime_doy.expression  # type: ignore
    def starttime_doy(cls) -> int:
        return func.date_part("doy", cls.starttime)

    @hybrid_property  # type: ignore
    def midtime_year(self) -> int:
        return self.midtime.year

    @midtime_year.expression  # type: ignore
    def midtime_year(cls) -> int:
        return func.date_part("year", cls.midtime)

    @hybrid_property  # type: ignore
    def midtime_doy(self) -> int:
        return self.midtime.timetuple().tm_yday

    @midtime_doy.expression  # type: ignore
    def midtime_doy(cls) -> int:
        return func.date_part("doy", cls.midtime)

    @hybrid_property  # type: ignore
    def endtime_year(self) -> int:
        return self.endtime.year

    @endtime_year.expression  # type: ignore
    def endtime_year(cls) -> int:
        return func.date_part("year", cls.endtime)

    @hybrid_property  # type: ignore
    def endtime_doy(self) -> int:
        return self.endtime.timetuple().tm_yday

    @endtime_doy.expression  # type: ignore
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

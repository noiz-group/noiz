from sqlalchemy.dialects.postgresql import HSTORE, ARRAY, NUMRANGE
from sqlalchemy import extract, func
from sqlalchemy.ext.hybrid import hybrid_property

import obspy

from noiz.database import db

from flask.logging import logging

logger = logging.getLogger(__name__)


class Tsindex(db.Model):
    __tablename__ = "tsindex"
    id = db.Column("id", db.BigInteger, primary_key=True)
    network = db.Column("network", db.UnicodeText, nullable=False)
    station = db.Column("station", db.UnicodeText, nullable=False)
    location = db.Column("location", db.UnicodeText, nullable=False)
    channel = db.Column("channel", db.UnicodeText, nullable=False)
    quality = db.Column("quality", db.UnicodeText)
    version = db.Column("version", db.Integer)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    samplerate = db.Column("samplerate", db.NUMERIC, nullable=False)
    filename = db.Column("filename", db.UnicodeText, nullable=False)
    byteoffset = db.Column("byteoffset", db.BigInteger, nullable=False)
    bytes = db.Column("bytes", db.BigInteger)
    hash = db.Column("hash", db.UnicodeText)
    timeindex = db.Column("timeindex", HSTORE)
    timespans = db.Column("timespans", ARRAY(NUMRANGE))
    timerates = db.Column("timerates", ARRAY(db.NUMERIC))
    format = db.Column("format", db.UnicodeText)
    filemodtime = db.Column("filemodtime", db.TIMESTAMP(timezone=True), nullable=False)
    updated = db.Column("updated", db.TIMESTAMP(timezone=True), nullable=False)
    scanned = db.Column("scanned", db.TIMESTAMP(timezone=True), nullable=False)

    def read_file(self):
        return obspy.read(self.filename, format=self.format)

    @hybrid_property
    def component(self):
        return self.channel[-1]

    @component.expression
    def component(cls):
        return func.right(cls.channel, 1)

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
    def starttime_month(self):
        return self.starttime.month

    @starttime_month.expression
    def starttime_month(cls):
        return func.date_part("month", cls.starttime)

    @hybrid_property
    def starttime_day(self):
        return self.starttime.day

    @starttime_day.expression
    def starttime_day(cls):
        return func.date_part("day", cls.starttime)

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

    @hybrid_property
    def endtime_month(self):
        return self.endtime.month

    @endtime_month.expression
    def endtime_month(cls):
        return func.date_part("month", cls.endtime)

    @hybrid_property
    def endtime_day(self):
        return self.endtime.day

    @endtime_day.expression
    def endtime_day(cls):
        return func.date_part("day", cls.endtime)

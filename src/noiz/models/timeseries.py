# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.
from pathlib import Path

from numpy import deprecate_with_doc
from sqlalchemy import func
from sqlalchemy.ext.hybrid import hybrid_property
import obspy

from noiz.exceptions import MissingDataFileException
from noiz.database import db
from noiz.processing.miniseed_helpers import _read_single_miniseed


class Tsindex(db.Model):
    """
    Seismic data file index.

    Stores metadata about miniSEED files for efficient data location and loading.
    Populated from mseedindex JSON output (database-agnostic approach).
    """

    __tablename__ = "raw_data_index"
    id = db.Column("id", db.BigInteger, primary_key=True)

    # Required fields (used by Noiz queries)
    network = db.Column("network", db.UnicodeText, nullable=False)
    station = db.Column("station", db.UnicodeText, nullable=False)
    location = db.Column("location", db.UnicodeText, nullable=False)
    channel = db.Column("channel", db.UnicodeText, nullable=False)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    samplerate = db.Column("samplerate", db.NUMERIC, nullable=False)
    filename = db.Column("filename", db.UnicodeText, nullable=False)

    # Optional metadata fields
    quality = db.Column("quality", db.UnicodeText)
    version = db.Column("version", db.Integer)
    byteoffset = db.Column("byteoffset", db.BigInteger)
    bytes = db.Column("bytes", db.BigInteger)
    hash = db.Column("hash", db.UnicodeText)
    format = db.Column("format", db.UnicodeText)
    filemodtime = db.Column("filemodtime", db.TIMESTAMP(timezone=True))
    updated = db.Column("updated", db.TIMESTAMP(timezone=True))
    scanned = db.Column("scanned", db.TIMESTAMP(timezone=True))

    # REMOVED: PostgreSQL-specific types that blocked SQLite
    # - timeindex (HSTORE) - never used in queries
    # - timespans (ARRAY(NUMRANGE)) - never used in queries
    # - timerates (ARRAY(NUMERIC)) - never used in queries

    @deprecate_with_doc(msg="This function is deprecated. use load_data instead.")
    def read_file(self) -> obspy.Stream:
        """
        Deprecated. Use load_data
        """
        return self.load_data()

    def load_data(self) -> obspy.Stream:
        """
        Loads a seismic file associated with an entry.
        :return: Returns seismic stream read from file
        :rtype: obspy.Stream
        """
        try:
            st = _read_single_miniseed(filename=Path(self.filename), format=self.format)
        except MissingDataFileException as e:
            raise MissingDataFileException(f"Data file for chunk {self} is missing") from e
        return st

    @hybrid_property  # type: ignore
    def component(self):
        return self.channel[-1]

    @component.expression  # type: ignore
    def component(cls):
        return func.right(cls.channel, 1)

    @hybrid_property  # type: ignore
    def starttime_year(self):
        return self.starttime.year

    @starttime_year.expression  # type: ignore
    def starttime_year(cls):
        return func.date_part("year", cls.starttime)

    @hybrid_property  # type: ignore
    def starttime_doy(self):
        return self.starttime.timetuple().tm_yday

    @starttime_doy.expression  # type: ignore
    def starttime_doy(cls):
        return func.date_part("doy", cls.starttime)

    @hybrid_property  # type: ignore
    def starttime_month(self):
        return self.starttime.month

    @starttime_month.expression  # type: ignore
    def starttime_month(cls):
        return func.date_part("month", cls.starttime)

    @hybrid_property  # type: ignore
    def starttime_day(self):
        return self.starttime.day

    @starttime_day.expression  # type: ignore
    def starttime_day(cls):
        return func.date_part("day", cls.starttime)

    @hybrid_property  # type: ignore
    def endtime_year(self):
        return self.endtime.year

    @endtime_year.expression  # type: ignore
    def endtime_year(cls):
        return func.date_part("year", cls.endtime)

    @hybrid_property  # type: ignore
    def endtime_doy(self):
        return self.endtime.timetuple().tm_yday

    @endtime_doy.expression  # type: ignore
    def endtime_doy(cls):
        return func.date_part("doy", cls.endtime)

    @hybrid_property  # type: ignore
    def endtime_month(self):
        return self.endtime.month

    @endtime_month.expression  # type: ignore
    def endtime_month(cls):
        return func.date_part("month", cls.endtime)

    @hybrid_property  # type: ignore
    def endtime_day(self):
        return self.endtime.day

    @endtime_day.expression  # type: ignore
    def endtime_day(cls):
        return func.date_part("day", cls.endtime)

from sqlalchemy.dialects.postgresql import HSTORE, ARRAY, NUMRANGE

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

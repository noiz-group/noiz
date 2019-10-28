from noiz.database import db

from flask.logging import logging

logger = logging.getLogger(__name__)


class Timespan(db.Model):
    __tablename__ = "timespan"

    id = db.Column("id", db.BigInteger, primary_key=True)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    midtime = db.Column("midtime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    year = db.Column("year", db.Integer, nullable=False)
    julday = db.Column("julday", db.Integer, nullable=False)

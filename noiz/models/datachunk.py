from noiz.database import db

from flask.logging import logging

logger = logging.getLogger(__name__)


class DataChunk(db.Model):
    __tablename__ = "datachunk"

    id = db.Column("id", db.BigInteger, primary_key=True)
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "component_id",
            name="unique_datachunk_per_timespan_per_station",
        ),
    )
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"))
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"))
    sampling_rate = db.Column("sampling_rate", db.Float)
    npts = db.Column("npts", db.Integer)
    filepath = db.Column("filepath", db.UnicodeText)

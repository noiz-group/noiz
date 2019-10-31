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
            "processing_params_id",
            name="unique_datachunk_per_timespan_per_station_per_processing",
        ),
    )
    component_id = db.Column(
        "component_id", db.Integer, db.ForeignKey("component.id"), nullable=False
    )
    processing_params_id = db.Column(
        "processing_params_id",
        db.Integer,
        db.ForeignKey("processing_params.id"),
        nullable=False,
    )
    timespan_id = db.Column(
        "timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False
    )
    sampling_rate = db.Column("sampling_rate", db.Float, nullable=False)
    npts = db.Column("npts", db.Integer, nullable=False)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)

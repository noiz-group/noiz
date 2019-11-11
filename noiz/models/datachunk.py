from noiz.exceptions import MissingDataFileException
from noiz.database import db

from flask.logging import logging
from pathlib import Path
import obspy


logger = logging.getLogger(__name__)


class DataChunk(db.Model):
    __tablename__ = "datachunk"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "component_id",
            "processing_params_id",
            name="unique_datachunk_per_timespan_per_station_per_processing",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
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
    padded_npts = db.Column("padded_npts", db.Integer, nullable=True)

    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    component = db.relationship("Component", foreign_keys=[component_id])
    processing_params = db.relationship(
        "ProcessingParams", foreign_keys=[processing_params_id]
    )
    processed_datachunks = db.relationship("ProcessedDatachunk")

    def load_data(self):
        if Path(self.filepath).exists:
            return obspy.read(self.filepath, "MSEED")
        else:
            raise MissingDataFileException(f"Data file for chunk {self} is missing")


class ProcessedDatachunk(db.Model):
    __tablename__ = "processeddatachunk"
    __table_args__ = (
        db.UniqueConstraint(
            "datachunk_id",
            "processing_params_id",
            name="unique_processing_per_datachunk",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    processing_params_id = db.Column(
        "processing_params_id",
        db.Integer,
        db.ForeignKey("processing_params.id"),
        nullable=False,
    )
    datachunk_id = db.Column(
        "datachunk_id", db.Integer, db.ForeignKey("datachunk.id"), nullable=False
    )
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)

    datachunk = db.relationship("DataChunk", foreign_keys=[datachunk_id])

    def load_data(self):
        if Path(self.filepath).exists:
            return obspy.read(self.filepath, "MSEED")
        else:
            raise MissingDataFileException(f"Data file for chunk {self} is missing")
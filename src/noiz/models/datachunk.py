from noiz.exceptions import MissingDataFileException
from noiz.database import db

from pathlib import Path
import obspy


class DatachunkFile(db.Model):
    __tablename__ = "datachunk_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)


class Datachunk(db.Model):
    __tablename__ = "datachunk"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "component_id",
            "datachunk_params_id",
            name="unique_datachunk_per_timespan_per_station_per_processing",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    component_id = db.Column(
        "component_id", db.Integer, db.ForeignKey("component.id"), nullable=False
    )
    datachunk_params_id = db.Column(
        "datachunk_params_id",
        db.Integer,
        db.ForeignKey("datachunk_params.id"),
        nullable=False,
    )
    timespan_id = db.Column(
        "timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False
    )
    sampling_rate = db.Column("sampling_rate", db.Float, nullable=False)
    npts = db.Column("npts", db.Integer, nullable=False)
    padded_npts = db.Column("padded_npts", db.Integer, nullable=True)
    datachunk_file_id = db.Column(
        "datachunk_file_id",
        db.BigInteger,
        db.ForeignKey("datachunk_file.id"),
        nullable=True,
    )
    device_id = db.Column("device_id", db.Integer, db.ForeignKey("device.id"), nullable=True)

    device = db.relationship("Device", foreign_keys=[device_id], uselist=False, lazy="joined")
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id], back_populates="datachunks")
    component = db.relationship("Component", foreign_keys=[component_id])
    params = db.relationship("DatachunkParams", uselist=False, foreign_keys=[datachunk_params_id])
    stats = db.relationship("DatachunkStats", uselist=False, back_populates="datachunk")
    qcones = db.relationship("QCOneResults", uselist=True, back_populates="datachunk")
    processed_datachunks = db.relationship("ProcessedDatachunk")

    datachunk_file = db.relationship(
        "DatachunkFile",
        foreign_keys=[datachunk_file_id],
        uselist=False,
        lazy="joined",
    )

    def load_data(self):
        filepath = Path(self.datachunk_file.filepath)
        if filepath.exists:
            # FIXME when obspy will be released, str(Path) wont be necesary
            return obspy.read(str(filepath), "MSEED")
        else:
            raise MissingDataFileException(f"Data file for chunk {self} is missing")

    def load_data_with_file(self, datachunk_file: DatachunkFile):
        if datachunk_file.id != self.datachunk_file_id:
            raise ValueError("You provided wrong datachunk file! Expected id: {self.datachunk_file_id}")
        filepath = Path(datachunk_file.filepath)
        if filepath.exists:
            # FIXME when obspy will be released, str(Path) wont be necesary
            return obspy.read(str(filepath), "MSEED")
        else:
            raise MissingDataFileException(f"File associated with {DatachunkFile} does not exist.")


class DatachunkStats(db.Model):
    __tablename__ = "datachunk_stats"
    __table_args__ = (
        db.UniqueConstraint(
            "datachunk_id",
            name="unique_stats_per_datachunk",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)

    datachunk_id = db.Column("datachunk_id", db.BigInteger, db.ForeignKey("datachunk.id"), nullable=False)

    energy = db.Column("energy", db.Float, nullable=True)
    min = db.Column("min", db.Float, nullable=True)
    max = db.Column("max", db.Float, nullable=True)
    mean = db.Column("mean", db.Float, nullable=True)
    variance = db.Column("variance", db.Float, nullable=True)
    skewness = db.Column("skewness", db.Float, nullable=True)
    kurtosis = db.Column("kurtosis", db.Float, nullable=True)

    datachunk = db.relationship("Datachunk", foreign_keys=[datachunk_id], back_populates="stats")

    def __repr__(self):
        return f"Stats of Datachunk no.{self.datachunk_id}"


class ProcessedDatachunk(db.Model):
    __tablename__ = "processeddatachunk"
    __table_args__ = (
        db.UniqueConstraint(
            "datachunk_id",
            "processed_datachunk_params_id",
            name="unique_processing_per_datachunk_per_config",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    processed_datachunk_params_id = db.Column(
        "processed_datachunk_params_id",
        db.Integer,
        db.ForeignKey("datachunk_params.id"),
        nullable=False,
    )
    datachunk_id = db.Column(
        "datachunk_id",
        db.Integer,
        db.ForeignKey("datachunk.id"),
        nullable=False
    )
    processed_datachunk_file_id = db.Column(
        "processed_datachunk_file_id",
        db.BigInteger,
        db.ForeignKey("processed_datachunk_file.id"),
        nullable=True,
    )

    datachunk = db.relationship("Datachunk", foreign_keys=[datachunk_id], back_populates="processed_datachunks")
    datachunk_processing_config = db.relationship(
        "DatachunkParams", foreign_keys=[processed_datachunk_params_id],
    )
    processed_datachunk_file = db.relationship(
        "ProcessedDatachunkFile",
        foreign_keys=[processed_datachunk_file_id],
        uselist=False,
        lazy="joined",
    )

    def load_data(self):
        filepath = Path(self.processed_datachunk_file.filepath)
        if filepath.exists:
            # FIXME when obspy will be released, str(Path) wont be necesary
            return obspy.read(str(filepath), "MSEED")
        else:
            raise MissingDataFileException(f"Data file for chunk {self} is missing")


class ProcessedDatachunkFile(db.Model):
    __tablename__ = "processed_datachunk_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)

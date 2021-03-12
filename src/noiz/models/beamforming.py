from pathlib import Path
from sqlalchemy.ext.associationproxy import association_proxy

from noiz.database import db
from noiz.exceptions import MissingDataFileException


class BeamformingFile(db.Model):
    __tablename__ = "beamforming_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)


association_table_beamforming_results_datachunks = db.Table(
    "beamforming_association_datachunks",
    db.metadata,
    db.Column(
        "datachunk_id", db.BigInteger, db.ForeignKey("datachunk.id")
    ),
    db.Column("beamforming_result_id", db.BigInteger, db.ForeignKey("beamforming_result.id")),
    db.UniqueConstraint("beamforming_result_id", "datachunk_id"),
)


class BeamformingResult(db.Model):
    __tablename__ = "beamforming_result"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id", "beamforming_params_id", name="unique_beam_per_config_per_timespan"
        ),
    )
    id = db.Column("id", db.Integer, primary_key=True)
    beamforming_params_id = db.Column(
        "beamforming_params_id",
        db.Integer,
        db.ForeignKey("beamforming_params.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)

    mean_relative_relpow = db.Column("mean_relative_relpow", db.Float, nullable=False)
    std_relative_relpow = db.Column("std_relative_relpow", db.Float, nullable=False)
    mean_absolute_relpow = db.Column("mean_absolute_relpow", db.Float, nullable=False)
    std_absolute_relpow = db.Column("std_absolute_relpow", db.Float, nullable=False)
    mean_backazimuth = db.Column("mean_backazimuth", db.Float, nullable=False)
    std_backazimuth = db.Column("std_backazimuth", db.Float, nullable=False)
    mean_slowness = db.Column("mean_slowness", db.Float, nullable=False)
    std_slowness = db.Column("std_slowness", db.Float, nullable=False)

    beamforming_file_id = db.Column(
        "beamforming_file_id",
        db.BigInteger,
        db.ForeignKey("beamforming_file.id"),
        nullable=True,
    )

    timespan = db.relationship(
        "Timespan",
        foreign_keys=[timespan_id],
        uselist=False,
        lazy="joined",
    )

    beamforming_params = db.relationship(
        "BeamformingParams",
        foreign_keys=[beamforming_params_id],
        uselist=False,
        lazy="joined",
    )

    beamforming_file = db.relationship(
        "BeamformingFile",
        foreign_keys=[beamforming_file_id],
        uselist=False,
        lazy="joined",
    )

    datachunks = db.relationship("Datachunk", secondary=lambda: association_table_beamforming_results_datachunks)
    datachunk_ids = association_proxy('datachunks', 'id')

    def load_data(self):
        filepath = Path(self.beamforming_file.filepath)
        if filepath.exists:
            raise NotImplementedError("Not yet implemented, use np.load()")
        else:
            raise MissingDataFileException(f"Inventory file for component {self} is missing")

from pathlib import Path
from sqlalchemy.ext.associationproxy import association_proxy

from noiz.database import db
from noiz.exceptions import MissingDataFileException
from noiz.models import Timespan, BeamformingParams, FileModelMixin
from noiz.models.mixins import BeamformingPeakExtractMixin


class BeamformingFile(FileModelMixin):
    __tablename__ = "beamforming_file"

    _file_model_type: str = "beamforming"
    _filename_extension: str = "npz"

    def find_empty_filepath(self, ts: Timespan, params: BeamformingParams) -> Path:
        """filldocs"""
        self._filepath = self._find_empty_filepath(params=params, ts=ts, cmp=None)
        return self.filepath


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

    used_component_count = db.Column("used_component_count", db.Integer, nullable=False)

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

    file = db.relationship(
        "BeamformingFile",
        foreign_keys=[beamforming_file_id],
        uselist=False,
        lazy="joined",
    )

    average_abspower_peaks = db.relationship("BeamformingPeakAverageAbspower", lazy="joined")
    average_relpower_peaks = db.relationship("BeamformingPeakAverageRelpower", lazy="joined")
    all_abspower_peaks = db.relationship("BeamformingPeakAllAbspower", lazy="joined")
    all_relpower_peaks = db.relationship("BeamformingPeakAllRelpower", lazy="joined")

    datachunks = db.relationship("Datachunk", secondary=lambda: association_table_beamforming_results_datachunks)
    datachunk_ids = association_proxy('datachunks', 'id')

    def load_data(self):
        filepath = Path(self.file.filepath)
        if filepath.exists:
            raise NotImplementedError("Not yet implemented, use np.load()")
        else:
            raise MissingDataFileException(f"Inventory file for component {self} is missing")


class BeamformingPeakAverageAbspower(BeamformingPeakExtractMixin):
    __tablename__ = "beamforming_peak_average_abspower"
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.Integer,
        db.ForeignKey("beamforming_result.id"),
        nullable=False,
    )


class BeamformingPeakAverageRelpower(BeamformingPeakExtractMixin):
    __tablename__ = "beamforming_peak_average_relpower"
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.Integer,
        db.ForeignKey("beamforming_result.id"),
        nullable=False,
    )


class BeamformingPeakAllAbspower(BeamformingPeakExtractMixin):
    __tablename__ = "beamforming_peak_all_abspower"
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.Integer,
        db.ForeignKey("beamforming_result.id"),
        nullable=False,
    )


class BeamformingPeakAllRelpower(BeamformingPeakExtractMixin):
    __tablename__ = "beamforming_peak_all_relpower"
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.Integer,
        db.ForeignKey("beamforming_result.id"),
        nullable=False,
    )

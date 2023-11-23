# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import Mapped

from noiz.database import db
from noiz.exceptions import MissingDataFileException
from noiz.globals import ExtendedEnum
from noiz.models import Timespan, Datachunk, BeamformingParams, FileModelMixin
from noiz.models.mixins import BeamformingPeakExtractMixin

import numpy as np


class BeamformingResultType(ExtendedEnum):
    AVGABSPOWER = "avg_abspower"
    AVGRELPOWER = "avg_relpower"
    ALLABSPOWER = "all_abspower"
    ALLRELPOWER = "all_relpower"


class BeamformingFile(FileModelMixin):
    __tablename__ = "beamforming_file"
    id: Mapped[UUID] = db.Column("id", db.UUID, primary_key=True, default=uuid4)  # type: ignore

    _file_model_type: str = "beamforming"
    _filename_extension: str = "npz"

    def find_empty_filepath(self, ts: Timespan, params: BeamformingParams) -> Path:
        """filldocs"""
        self._filepath = self._find_empty_filepath(params=params, ts=ts, cmp=None)
        return self.filepath


class BeamformingResult(db.Model):
    __tablename__ = "beamforming_result"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id", "beamforming_params_id", name="unique_beam_per_config_per_timespan"
        ),
    )
    id: Mapped[UUID] = db.Column("id", db.UUID, primary_key=True, default=uuid4)
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
        db.UUID,
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

    beamforming_result_datachunks_associations = db.relationship(
        "BeamformingResultDatchunksAssociation",
        back_populates="beamforming_result",
        cascade="all, delete-orphan",
    )
    datachunks = association_proxy(
        "beamforming_result_datachunks_associations",
        "datachunk",
    )

    datachunk_ids = association_proxy(
        "beamforming_result_datachunks_associations",
        "datachunk_id",
    )

    beamforming_result_avg_abspower_associations = db.relationship(
        "BeamformingResulAvgAbspowerAssociation",
        back_populates="beamforming_result",
        cascade="all, delete-orphan",
    )
    average_abspower_peaks = association_proxy(
        "beamforming_result_avg_abspower_associations",
        "avg_abspower",
    )

    beamforming_result_avg_relpower_associations = db.relationship(
        "BeamformingResulAvgRelpowerAssociation",
        back_populates="beamforming_result",
        cascade="all, delete-orphan",
    )
    average_relpower_peaks = association_proxy(
        "beamforming_result_avg_relpower_associations",
        "avg_relpower",
    )

    beamforming_result_all_abspower_associations = db.relationship(
        "BeamformingResulAllAbspowerAssociation",
        back_populates="beamforming_result",
        cascade="all, delete-orphan",
    )
    all_abspower_peaks = association_proxy(
        "beamforming_result_all_abspower_associations",
        "all_abspower",
    )

    beamforming_result_all_relpower_associations = db.relationship(
        "BeamformingResulAllRelpowerAssociation",
        back_populates="beamforming_result",
        cascade="all, delete-orphan",
    )
    all_relpower_peaks = association_proxy(
        "beamforming_result_all_relpower_associations",
        "all_relpower",
    )

    def load_data(self):
        filepath = Path(self.file.filepath)
        if filepath.exists():
            return np.load(str(filepath))
        else:
            raise MissingDataFileException(f"Inventory file for component {self} is missing")


class BeamformingResultDatchunksAssociation(db.Model):
    __tablename__ = "beamforming_association_datachunks"
    datachunk_id = db.Column(
        "datachunk_id",
        db.BigInteger,
        db.ForeignKey("datachunk.id"),
        primary_key=True,
    )
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.UUID,
        db.ForeignKey("beamforming_result.id"),
        primary_key=True,
    )

    beamforming_result = db.relationship(
        BeamformingResult,
        back_populates="beamforming_result_datachunks_associations"
    )
    datachunk = db.relationship("Datachunk")

    def __init__(
            self,
            datachunk: Optional[Datachunk] = None,
            datachunk_id: Optional[int] = None,
            beamfroming_result: Optional[BeamformingResult] = None,
            beamfroming_result_id: Optional[UUID] = None,
    ):
        self.datachunk = datachunk
        self.datachunk_id = datachunk_id
        self.beamforming_result = beamfroming_result
        self.beamforming_result_id = beamfroming_result_id


class BeamformingResulAvgAbspowerAssociation(db.Model):
    __tablename__ = "beamforming_result_association_avg_abspower"
    beamforming_peak_average_abspower_id = db.Column(
        "beamforming_peak_average_abspower_id",
        db.UUID,
        db.ForeignKey("beamforming_peak_average_abspower.id"),
        primary_key=True,
    )
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.UUID,
        db.ForeignKey("beamforming_result.id"),
        primary_key=True,
    )

    beamforming_result = db.relationship(
        BeamformingResult,
        back_populates="beamforming_result_avg_abspower_associations",
    )
    avg_abspower = db.relationship(
        "BeamformingPeakAverageAbspower",
        back_populates="beamforming_result_avg_abspower_associations",
    )

    def __init__(
            self,
            avg_abspower: Optional["BeamformingPeakAverageAbspower"] = None,
            avg_abspower_id: Optional[UUID] = None,
            beamfroming_result: Optional[BeamformingResult] = None,
            beamfroming_result_id: Optional[UUID] = None,
    ):
        self.avg_abspower = avg_abspower
        self.avg_abspower_id = avg_abspower_id
        self.beamforming_result = beamfroming_result
        self.beamforming_result_id = beamfroming_result_id


class BeamformingResulAvgRelpowerAssociation(db.Model):
    __tablename__ = "beamforming_result_association_avg_relpower"
    beamforming_peak_average_abspower_id = db.Column(
        "beamforming_peak_average_relpower_id",
        db.UUID,
        db.ForeignKey("beamforming_peak_average_relpower.id"),
        primary_key=True,
    )
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.UUID,
        db.ForeignKey("beamforming_result.id"),
        primary_key=True,
    )

    beamforming_result = db.relationship(
        BeamformingResult,
        back_populates="beamforming_result_avg_relpower_associations",
    )
    avg_relpower = db.relationship(
        "BeamformingPeakAverageRelpower",
        back_populates="beamforming_result_avg_relpower_associations",
    )

    def __init__(
            self,
            avg_relpower: Optional["BeamformingPeakAverageRelpower"] = None,
            avg_relpower_id: Optional[UUID] = None,
            beamfroming_result: Optional[BeamformingResult] = None,
            beamfroming_result_id: Optional[UUID] = None,
    ):
        self.avg_relpower = avg_relpower
        self.avg_relpower_id = avg_relpower_id
        self.beamforming_result = beamfroming_result
        self.beamforming_result_id = beamfroming_result_id


class BeamformingResulAllAbspowerAssociation(db.Model):
    __tablename__ = "beamforming_result_association_all_abspower"
    beamforming_peak_all_abspower_id = db.Column(
        "beamforming_peak_all_abspower_id",
        db.UUID,
        db.ForeignKey("beamforming_peak_all_abspower.id"),
        primary_key=True,
    )
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.UUID,
        db.ForeignKey("beamforming_result.id"),
        primary_key=True,
    )

    beamforming_result = db.relationship(
        BeamformingResult,
        back_populates="beamforming_result_all_abspower_associations",
    )
    all_abspower = db.relationship(
        "BeamformingPeakAllAbspower",
        back_populates="beamforming_result_all_abspower_associations",
    )

    def __init__(
            self,
            all_abspower: Optional["BeamformingPeakAllAbspower"] = None,
            all_abspower_id: Optional[UUID] = None,
            beamfroming_result: Optional[BeamformingResult] = None,
            beamfroming_result_id: Optional[UUID] = None,
    ):
        self.all_abspower = all_abspower
        self.all_abspower_id = all_abspower_id
        self.beamforming_result = beamfroming_result
        self.beamforming_result_id = beamfroming_result_id


class BeamformingResulAllRelpowerAssociation(db.Model):
    __tablename__ = "beamforming_result_association_all_relpower"
    beamforming_peak_average_abspower_id = db.Column(
        "beamforming_peak_all_relpower_id",
        db.UUID,
        db.ForeignKey("beamforming_peak_all_relpower.id"),
        primary_key=True,
    )
    beamforming_result_id = db.Column(
        "beamforming_result_id",
        db.UUID,
        db.ForeignKey("beamforming_result.id"),
        primary_key=True,
    )

    beamforming_result = db.relationship(
        BeamformingResult,
        back_populates="beamforming_result_all_relpower_associations",
    )
    all_relpower = db.relationship(
        "BeamformingPeakAllRelpower",
        back_populates="beamforming_result_all_relpower_associations",
    )

    def __init__(
            self,
            all_relpower: Optional["BeamformingPeakAllRelpower"] = None,
            all_relpower_id: Optional[UUID] = None,
            beamfroming_result: Optional[BeamformingResult] = None,
            beamfroming_result_id: Optional[UUID] = None,
    ):
        self.all_relpower = all_relpower
        self.all_relpower_id = all_relpower_id
        self.beamforming_result = beamfroming_result
        self.beamforming_result_id = beamfroming_result_id


class BeamformingPeakAverageAbspower(BeamformingPeakExtractMixin):
    __tablename__ = "beamforming_peak_average_abspower"

    beamforming_result_avg_abspower_associations = db.relationship(
        "BeamformingResulAvgAbspowerAssociation",
        back_populates="avg_abspower",
    )
    beamforming_result = association_proxy(
        "beamforming_result_avg_abspower_associations",
        "beamforming_result",
    )


class BeamformingPeakAverageRelpower(BeamformingPeakExtractMixin):
    __tablename__ = "beamforming_peak_average_relpower"

    beamforming_result_avg_relpower_associations = db.relationship(
        "BeamformingResulAvgRelpowerAssociation",
        back_populates="avg_relpower",
    )
    beamforming_result = association_proxy(
        "beamforming_result_avg_relpower_associations",
        "beamforming_result",
    )


class BeamformingPeakAllAbspower(BeamformingPeakExtractMixin):
    __tablename__ = "beamforming_peak_all_abspower"

    beamforming_result_all_abspower_associations = db.relationship(
        "BeamformingResulAllAbspowerAssociation",
        back_populates="all_abspower",
    )
    beamforming_result = association_proxy(
        "beamforming_result_all_abspower_associations",
        "beamforming_result",
    )


class BeamformingPeakAllRelpower(BeamformingPeakExtractMixin):
    __tablename__ = "beamforming_peak_all_relpower"

    beamforming_result_all_relpower_associations = db.relationship(
        "BeamformingResulAllRelpowerAssociation",
        back_populates="all_relpower",
    )
    beamforming_result = association_proxy(
        "beamforming_result_all_relpower_associations",
        "beamforming_result",
    )

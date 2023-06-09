# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from noiz.exceptions import MissingDataFileException
from pathlib import Path

from typing import Optional

from sqlalchemy.dialects.postgresql import ARRAY
from noiz.database import db

from noiz.models.stacking import ccf_ccfstack_association_table


class CrosscorrelationOld(db.Model):
    __tablename__ = "crosscorrelation"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_id",
            "crosscorrelation_params_id",
            name="unique_ccf_per_timespan_per_componentpair_per_config",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    componentpair_id = db.Column(
        "componentpair_id",
        db.Integer,
        db.ForeignKey("componentpair_cartesian.id"),
        nullable=False,
    )
    timespan_id = db.Column(
        "timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False
    )
    crosscorrelation_params_id = db.Column(
        "crosscorrelation_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_params.id"),
        nullable=False,
    )
    ccf = db.Column("ccf", ARRAY(db.Float))

    componentpair_cartesian = db.relationship("ComponentPairCartesian", foreign_keys=[componentpair_id])
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    crosscorrelation_params = db.relationship(
        "CrosscorrelationParams", foreign_keys=[crosscorrelation_params_id]
    )
    # stacks = db.relationship(
    #     "CCFStack", secondary=ccf_ccfstack_association_table, back_populates="ccfs"
    # )


class CrosscorrelationFile(db.Model):
    __tablename__ = "crosscorrelation_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)


class Crosscorrelation(db.Model):
    __tablename__ = "crosscorrelationnew"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_id",
            "crosscorrelation_params_id",
            name="unique_ccfn_per_timespan_per_componentpair_per_config",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    componentpair_id = db.Column(
        "componentpair_id",
        db.Integer,
        db.ForeignKey("componentpair_cartesian.id"),
        nullable=False,
    )
    timespan_id = db.Column(
        "timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False
    )
    crosscorrelation_params_id = db.Column(
        "crosscorrelation_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_params.id"),
        nullable=False,
    )

    crosscorrelation_file_id = db.Column(
        "crosscorrelation_file_id",
        db.BigInteger,
        db.ForeignKey("crosscorrelation_file.id"),
        nullable=True,
    )

    componentpair_cartesian = db.relationship("ComponentPairCartesian", foreign_keys=[componentpair_id])
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    crosscorrelation_params = db.relationship(
        "CrosscorrelationParams", foreign_keys=[crosscorrelation_params_id]
    )

    file = db.relationship(
        "CrosscorrelationFile",
        foreign_keys=[crosscorrelation_file_id],
        uselist=False,
        lazy="joined",
    )

    stacks = db.relationship(
        "CCFStack", secondary=ccf_ccfstack_association_table, back_populates="ccfs"
    )

    def load_data(self, crosscorrelation_file: Optional[CrosscorrelationFile] = None):
        import numpy as np
        if crosscorrelation_file is None:
            filepath = Path(self.file.filepath)
        else:
            filepath = Path(crosscorrelation_file.filepath)
            if crosscorrelation_file.id != self.crosscorrelation_file_id:
                raise ValueError("You provided wrong datachunk file! Expected id: {self.datachunk_file_id}")

        if filepath.exists():
            return np.load(file=filepath)
        else:
            # FIXME remove this workaround when database will be upgraded
            with_suffix = filepath.with_name(f"{filepath.name}.npy")
            if with_suffix.exists():
                return np.load(file=with_suffix)
            else:
                raise MissingDataFileException(f"Data file for Crosscorrelation {self} is missing")

    @property
    def ccf(self):
        return self.load_data()

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from pathlib import Path
from typing import Optional

from noiz.database import db
from noiz.exceptions import MissingDataFileException
from noiz.models.stacking import ccf_ccfstack_association_table


class CrosscorrelationCartesianFile(db.Model):
    __tablename__ = "crosscorrelation_cartesian_file"
    id = db.Column("id", db.BigInteger, primary_key=True)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)


class CrosscorrelationCartesian(db.Model):
    __tablename__ = "crosscorrelation_cartesian"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_id",
            "crosscorrelation_cartesian_params_id",
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
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    crosscorrelation_cartesian_params_id = db.Column(
        "crosscorrelation_cartesian_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian_params.id"),
        nullable=False,
    )

    crosscorrelation_cartesian_file_id = db.Column(
        "crosscorrelation_cartesian_file_id",
        db.BigInteger,
        db.ForeignKey("crosscorrelation_cartesian_file.id"),
        nullable=True,
    )

    componentpair_cartesian = db.relationship("ComponentPairCartesian", foreign_keys=[componentpair_id], lazy="joined")

    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    crosscorrelation_cartesian_params = db.relationship(
        "CrosscorrelationCartesianParams", foreign_keys=[crosscorrelation_cartesian_params_id]
    )

    file = db.relationship(
        "CrosscorrelationCartesianFile",
        foreign_keys=[crosscorrelation_cartesian_file_id],
        uselist=False,
        lazy="joined",
    )

    stacks = db.relationship("CCFStack", secondary=ccf_ccfstack_association_table, back_populates="ccfs")

    def load_data(self, crosscorrelation_cartesian_file: Optional[CrosscorrelationCartesianFile] = None):
        import numpy as np

        if crosscorrelation_cartesian_file is None:
            filepath = Path(self.file.filepath)
        else:
            filepath = Path(crosscorrelation_cartesian_file.filepath)
            if crosscorrelation_cartesian_file.id != self.crosscorrelation_cartesian_file_id:
                raise ValueError("You provided wrong datachunk file! Expected id: {self.datachunk_file_id}")

        if filepath.exists():
            return np.load(file=filepath)
        else:
            # FIXME remove this workaround when database will be upgraded
            with_suffix = filepath.with_name(f"{filepath.name}.npy")
            if with_suffix.exists():
                return np.load(file=with_suffix)
            else:
                raise MissingDataFileException(f"Data file for CrosscorrelationCartesian {self} is missing")

    @property
    def ccf(self):
        return self.load_data()


class CrosscorrelationCylindricalFile(db.Model):
    __tablename__ = "crosscorrelation_cylindrical_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)


class CrosscorrelationCylindrical(db.Model):
    __tablename__ = "crosscorrelation_cylindrical"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "componentpair_cylindrical_id",
            "crosscorrelation_cylindrical_params_id",
            name="unique_ccfcylindrical_per_timespan_cylindrical_per_config",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    componentpair_cylindrical_id = db.Column(
        "componentpair_cylindrical_id",
        db.Integer,
        db.ForeignKey("componentpair_cylindrical.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.BigInteger, db.ForeignKey("timespan.id"), nullable=False)
    crosscorrelation_cartesian_1_id = db.Column(
        "crosscorrelation_cartesian_1_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_1_code_pair = db.Column(
        "crosscorrelation_cartesian_1_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_2_id = db.Column(
        "crosscorrelation_cartesian_2_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_2_code_pair = db.Column(
        "crosscorrelation_cartesian_2_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_3_id = db.Column(
        "crosscorrelation_cartesian_3_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_3_code_pair = db.Column(
        "crosscorrelation_cartesian_3_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cartesian_4_id = db.Column(
        "crosscorrelation_cartesian_4_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cartesian.id"),
        nullable=True,
    )
    crosscorrelation_cartesian_4_code_pair = db.Column(
        "crosscorrelation_cartesian_4_code_pair", db.UnicodeText, nullable=True
    )
    crosscorrelation_cylindrical_params_id = db.Column(
        "crosscorrelation_cylindrical_params_id",
        db.Integer,
        db.ForeignKey("crosscorrelation_cylindrical_params.id"),
        nullable=False,
    )
    crosscorrelation_cylindrical_file_id = db.Column(
        "crosscorrelation_cylindrical_file_id",
        db.BigInteger,
        db.ForeignKey("crosscorrelation_cylindrical_file.id"),
        nullable=True,
    )

    componentpair_cylindrical = db.relationship(
        "ComponentPairCylindrical", foreign_keys=[componentpair_cylindrical_id]
    )
    timespan = db.relationship("Timespan", foreign_keys=[timespan_id])
    crosscorrelation_cylindrical_params = db.relationship(
        "CrosscorrelationCylindricalParams", foreign_keys=[crosscorrelation_cylindrical_params_id]
    )
    file = db.relationship(
        "CrosscorrelationCylindricalFile",
        foreign_keys=[crosscorrelation_cylindrical_file_id],
        uselist=False,
        lazy="joined",
    )
    crosscorrelation_cartesian_1 = db.relationship(
        "CrosscorrelationCartesian",
        foreign_keys=[crosscorrelation_cartesian_1_id],
        lazy="joined",
    )
    crosscorrelation_cartesian_2 = db.relationship(
        "CrosscorrelationCartesian",
        foreign_keys=[crosscorrelation_cartesian_2_id],
        lazy="joined",
    )
    crosscorrelation_cartesian_3 = db.relationship(
        "CrosscorrelationCartesian",
        foreign_keys=[crosscorrelation_cartesian_3_id],
        lazy="joined",
    )
    crosscorrelation_cartesian_4 = db.relationship(
        "CrosscorrelationCartesian",
        foreign_keys=[crosscorrelation_cartesian_4_id],
        lazy="joined",
    )

    def load_data_cylindrical(
        self, crosscorrelation_cylindrical_file: Optional[CrosscorrelationCylindricalFile] = None
    ):
        import numpy as np

        if crosscorrelation_cylindrical_file is None:
            filepath = Path(self.file.filepath)
        else:
            filepath = Path(crosscorrelation_cylindrical_file.filepath)
            if crosscorrelation_cylindrical_file.id != self.crosscorrelation_cylindrical_file_id:
                raise ValueError("You provided wrong datachunk file! Expected id: {self.datachunk_file_id}")

        if filepath.exists():
            return np.load(file=filepath)
        else:
            # FIXME remove this workaround when database will be upgraded
            with_suffix = filepath.with_name(f"{filepath.name}.npy")
            if with_suffix.exists():
                return np.load(file=with_suffix)
            else:
                raise MissingDataFileException(f"Data file for CrosscorrelationCylindrical {self} is missing")

    @property
    def ccf(self):
        return self.load_data_cylindrical()

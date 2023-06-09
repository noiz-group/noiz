# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from typing import Dict, List

import numpy as np
from numpy.lib.npyio import NpzFile
from pathlib import Path

from noiz.database import db
from noiz.exceptions import MissingDataFileException
from noiz.models import Timespan, Component, PPSDParams
from noiz.models.mixins import FileModelMixin


class PPSDFile(FileModelMixin):
    __tablename__ = "ppsd_file"

    _file_model_type: str = "psd"
    _filename_extension: str = "npz"

    def find_empty_filepath(self, cmp: Component, ts: Timespan, params: PPSDParams) -> Path:
        """filldocs"""
        self._filepath = self._find_empty_filepath(params=params, ts=ts, cmp=cmp)
        return self.filepath


class PPSDResult(db.Model):
    __tablename__ = "ppsd_result"
    __table_args__ = (
        db.UniqueConstraint(
            "datachunk_id", "ppsd_params_id", name="unique_ppsd_per_config_per_datachunk"
        ),
    )
    id = db.Column("id", db.Integer, primary_key=True)
    ppsd_params_id = db.Column("ppsd_params_id", db.Integer, db.ForeignKey("ppsd_params.id"), nullable=False)
    timespan_id = db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"), nullable=False)

    ppsd_file_id = db.Column(
        "ppsd_file_id",
        db.BigInteger,
        db.ForeignKey("ppsd_file.id"),
        nullable=True,
    )

    timespan = db.relationship(
        "Timespan",
        foreign_keys=[timespan_id],
        uselist=False,
        lazy="joined",
    )
    datachunk = db.relationship(
        "Datachunk",
        foreign_keys=[datachunk_id],
        uselist=False,
        lazy="joined",
    )

    ppsd_params = db.relationship(
        "PPSDParams",
        foreign_keys=[ppsd_params_id],
        uselist=False,
        lazy="joined",
    )

    file = db.relationship(
        "PPSDFile",
        foreign_keys=[ppsd_file_id],
        uselist=False,
        lazy="joined",
    )

    def load_data(self) -> NpzFile:
        """
        Load npz file associated with this result.
        Outputs standard output of :py:meth:`numpy.load` called for '*.npz' file.

        :return: Loaded file associated with this result
        :rtype: NpzFile
        """
        filepath = Path(self.file.filepath)
        if filepath.exists():
            return np.load(filepath)
        else:
            raise MissingDataFileException(f"Result file for PPSDResult {self} is missing")


GroupedPSDs = Dict[Component, List[PPSDResult]]
GroupedAvgPSDs = Dict[Component, np.ndarray]  # npt.ArrayLike

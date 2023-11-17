# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import os
from typing import Optional, Union
from pathlib import Path

from sqlalchemy.orm import Mapped

from noiz.database import db
from noiz.globals import PROCESSED_DATA_DIR
from noiz.models import Component, Timespan, ComponentPairCartesian
from noiz.models.custom_db_types import PathInDB
from noiz.processing.path_helpers import directory_exists_or_create, increment_filename_counter

from noiz.models.processing_params import ParamsLike


class FileModelMixin(db.Model):
    __abstract__ = True
    id: Mapped[int] = db.Column("id", db.BigInteger, primary_key=True)
    _filepath: Mapped[Path] = db.Column("filepath", PathInDB, nullable=False)

    _file_model_type: str
    _filename_extension: Optional[str]

    def __init__(self, **kwargs):
        super(FileModelMixin, self).__init__(**kwargs)

    @property
    def filepath(self):
        return self._filepath

    @property
    def file_model_type(self):
        return self._file_model_type

    def _assemble_filename(
            self,
            cmp: Optional[Union[Component, ComponentPairCartesian]],
            ts: Timespan,
            count: int = 0,
    ) -> str:
        year = str(ts.starttime.year)
        doy = ts.starttime.strftime("%j")
        time = ts.starttime.strftime("%H%M")

        if isinstance(cmp, Component):
            filename_elements = [
                cmp.network,
                cmp.station,
                cmp.component,
                year,
                doy,
                time,
            ]
        elif isinstance(cmp, ComponentPairCartesian):
            raise NotImplementedError("For componentpair_cartesian is not yet implemented")
        elif cmp is None:
            filename_elements = [
                self.file_model_type,
                year,
                doy,
                time,
            ]
        else:
            raise TypeError(f"Expected either Component, ComponentPairCartesian or None. Got {type(cmp)}")

        extensions = [str(count), ]

        if self._filename_extension is not None:
            extensions.append(self._filename_extension)

        name = ".".join(filename_elements)

        return os.extsep.join([name, *tuple(extensions)])

    def _assemble_dirpath(
            self,
            params: ParamsLike,
            ts: Timespan,
            cmp: Optional[Union[Component, ComponentPairCartesian]],
    ) -> Path:
        year = str(ts.starttime.year)
        doy = ts.starttime.strftime("%j")
        if isinstance(cmp, Component):
            return (
                Path(PROCESSED_DATA_DIR)
                .joinpath(self.file_model_type)
                .joinpath(str(params.id))
                .joinpath(year)
                .joinpath(cmp.network)
                .joinpath(cmp.station)
                .joinpath(cmp.component)
                .joinpath(doy)
            )
        elif isinstance(cmp, ComponentPairCartesian):
            return (
                Path(PROCESSED_DATA_DIR)
                .joinpath(self.file_model_type)
                .joinpath(str(params.id))
                .joinpath(year)
                .joinpath(doy)
                .joinpath(cmp.component_code_pair)
                .joinpath(f"{cmp.component_a.network}.{cmp.component_a.station}-"
                          f"{cmp.component_b.network}.{cmp.component_b.station}")
            )
        elif cmp is None:
            return (
                Path(PROCESSED_DATA_DIR)
                .joinpath(self.file_model_type)
                .joinpath(str(params.id))
                .joinpath(year)
                .joinpath(doy)
            )
        else:
            raise TypeError(f"Expected either Component, ComponentPairCartesian or None. Got {type(cmp)}")

    def _prepare_dirpath(
            self,
            params: ParamsLike,
            ts: Timespan,
            cmp: Optional[Union[Component, ComponentPairCartesian]],
    ) -> Path:
        """filldocs"""
        dirpath = self._assemble_dirpath(params=params, ts=ts, cmp=cmp)
        directory_exists_or_create(dirpath=dirpath)
        return dirpath

    def _find_empty_filepath(
            self,
            ts: Timespan,
            params: ParamsLike,
            cmp: Optional[Union[Component, ComponentPairCartesian]]
    ) -> Path:
        """filldocs"""
        dirpath = self._prepare_dirpath(params=params, ts=ts, cmp=None)

        proposed_filepath = dirpath.joinpath(self._assemble_filename(ts=ts, count=0, cmp=cmp))
        self._filepath = increment_filename_counter(filepath=proposed_filepath, extension=True)

        return self.filepath


class BeamformingPeakExtractMixin(db.Model):
    __abstract__ = True
    id: Mapped[int] = db.Column("id", db.BigInteger, primary_key=True)

    slowness = db.Column("slowness", db.Float, nullable=False)
    slowness_x = db.Column("slowness_x", db.Float, nullable=False)
    slowness_y = db.Column("slowness_y", db.Float, nullable=False)
    amplitude = db.Column("amplitude", db.Float, nullable=False)
    azimuth = db.Column("azimuth", db.Float, nullable=False)
    backazimuth = db.Column("backazimuth", db.Float, nullable=False)

    def __init__(self, **kwargs):
        super(BeamformingPeakExtractMixin, self).__init__(**kwargs)

import os

from typing import Union, Optional

from pathlib import Path

from noiz.database import db
from noiz.exceptions import MissingDataFileException

from noiz.globals import PROCESSED_DATA_DIR
from noiz.models import Timespan, Component, ComponentPair
from noiz.models.processing_params import PPSDParams
from noiz.processing.path_helpers import parent_directory_exists_or_create, increment_filename_counter

ParamsLike = Union[
    PPSDParams,
]


class FileModel(db.Model):
    __abstract__ = True
    id: int = db.Column("id", db.BigInteger, primary_key=True)
    _filepath: str = db.Column("filepath", db.UnicodeText, nullable=False)

    _file_model_type: str
    _filename_extension: Optional[str]

    def __init__(self, **kwargs):
        super(FileModel, self).__init__(**kwargs)

    @property
    def file_model_type(self):
        return self._file_model_type

    def _assemble_filename(
            self,
            cmp: Component,
            ts: Timespan,
            count: int = 0
    ) -> str:
        year = str(ts.starttime.year)
        doy_time = ts.starttime.strftime("%j.%H%M")

        filename_elements = [
            cmp.network,
            cmp.station,
            cmp.component,
            year,
            doy_time,
        ]

        extensions = [str(count), ]

        if self._filename_extension is not None:
            extensions.append(self._filename_extension)

        name = ".".join(filename_elements)

        return os.extsep.join([name, *tuple(extensions)])

    def _assemble_dirpath(
            self,
            params: ParamsLike,
            ts: Timespan,
            cmp: Union[Component, ComponentPair]
    ) -> Path:
        if isinstance(cmp, Component):
            return (
                Path(PROCESSED_DATA_DIR)
                .joinpath(self.file_model_type)
                .joinpath(str(params.id))
                .joinpath(str(ts.starttime_year))
                .joinpath(cmp.network)
                .joinpath(cmp.station)
                .joinpath(cmp.component)
                .joinpath(str(ts.starttime_doy))
            )
        elif isinstance(cmp, ComponentPair):
            return (
                Path(PROCESSED_DATA_DIR)
                .joinpath(self.file_model_type)
                .joinpath(str(params.id))
                .joinpath(str(ts.starttime_year))
                .joinpath(str(ts.starttime_doy))
                .joinpath(str(ts.starttime.month))
                .joinpath(cmp.component_code_pair)
                .joinpath(f"{cmp.component_a.network}.{cmp.component_a.station}-"
                          f"{cmp.component_b.network}.{cmp.component_b.station}")
            )
        else:
            raise TypeError(f"Expected either Component or ComponentPair. Got {type(cmp)}")

    @property
    def filepath(self):
        return Path(self._filepath)


class PPSDFile(FileModel):
    __tablename__ = "ppsd_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    _filepath = db.Column("filepath", db.UnicodeText, nullable=False)

    _file_model_type: str = "psd"
    _filename_extension: str = "npz"

    def find_empty_filepath(self, cmp: Component, ts: Timespan, ppsd_params: PPSDParams) -> Path:
        dirpath = self._assemble_dirpath(
            params=ppsd_params,
            ts=ts,
            cmp=cmp,
        )

        parent_directory_exists_or_create(filepath=dirpath)

        proposed_filepath = dirpath.joinpath(self._assemble_filename(cmp=cmp, ts=ts, count=0))
        self._filepath = increment_filename_counter(filepath=proposed_filepath, extension=True)

        return self.filepath


class PPSDResult(db.Model):
    __tablename__ = "ppsd_result"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id", "ppsd_params_id", name="unique_ppsd_per_config_per_timespan"
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

    ppsd_file = db.relationship(
        "PPSDFile",
        foreign_keys=[ppsd_file_id],
        uselist=False,
        lazy="joined",
    )

    def load_data(self):
        filepath = Path(self.ppsd_file.filepath)
        if filepath.exists:
            raise NotImplementedError("Not yet implemented, use np.load()")
        else:
            raise MissingDataFileException(f"Result file for PPSDResult {self} is missing")

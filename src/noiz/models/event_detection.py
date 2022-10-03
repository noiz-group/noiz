from pathlib import Path
import obspy

from noiz.exceptions import MissingDataFileException
from noiz.database import db
from noiz.models.mixins import FileModelMixin
from noiz.models.custom_db_types import PathInDB
from noiz.models import Timespan, Component, EventDetectionParams, EventConfirmationParams


class EventDetectionFile(FileModelMixin):
    __tablename__ = "event_detection_file"

    # \todo add a folder with detection type
    _file_model_type: str = "event_detected"
    _filename_extension: str = "mseed"

    _pngpath: Path = db.Column("pngpath", PathInDB, nullable=True)
    _trgpath: Path = db.Column("trgpath", PathInDB, nullable=True)
    _npzpath: Path = db.Column("npzpath", PathInDB, nullable=True)

    @property
    def pngpath(self):
        return self._pngpath

    @property
    def trgpath(self):
        return self._trgpath

    @property
    def npzpath(self):
        return self._npzpath

    def find_empty_filepath(self, cmp: Component, ts: Timespan, params: EventDetectionParams, time_start: str) -> Path:
        """filldocs"""

        temp = self._find_empty_filepath(params=params, ts=ts, cmp=cmp)
        self._filepath = temp.parent / Path(temp.stem + "." + time_start + "." + self._filename_extension)
        self._pngpath = temp.parent / Path(temp.stem + "." + time_start + ".png")
        self._trgpath = temp.parent / Path(temp.stem + "." + time_start + ".trigger.png")
        self._npzpath = temp.parent / Path(temp.stem + "." + time_start + ".npz")

        return self.filepath


class EventConfirmationFile(FileModelMixin):
    __tablename__ = "event_confirmation_file"

    _file_model_type: str = "event_confirmed"
    _filename_extension: str = ""

    def find_empty_folder_path(self, cmp: Component, ts: Timespan, params: EventConfirmationParams, time_start: str) -> Path:
        """filldocs"""

        temp = self._find_empty_filepath(params=params, ts=ts, cmp=cmp)
        self._filepath = temp.parent / Path(time_start + "/")
        self._filepath.mkdir(parents=True, exist_ok=True)

        return self.filepath


association_table_event_confirmation_result_event_detection_result = db.Table(
    "event_confirmation_result_association_event_detection_result",
    db.metadata,
    db.Column(
        "event_confirmation_result_id", db.BigInteger, db.ForeignKey("event_confirmation_result.id")
    ),
    db.Column("event_detection_result_id", db.BigInteger, db.ForeignKey("event_detection_result.id")),
)


association_table_event_confirmation_run_datachunk = db.Table(
    "event_confirmation_run_association_datachunk",
    db.metadata,
    db.Column(
        "event_confirmation_run_id", db.BigInteger, db.ForeignKey("event_confirmation_run.id")
    ),
    db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id")),
    db.UniqueConstraint("event_confirmation_run_id", "datachunk_id"),
)


class EventDetectionResult(db.Model):
    __tablename__ = "event_detection_result"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "datachunk_id",
            "event_detection_params_id",
            "time_start",
            name="unique_detection_per_timespan_per_datachunk_per_param_per_time",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    event_detection_run_id = db.Column("event_detection_run_id", db.Integer, nullable=False)
    event_detection_params_id = db.Column(
        "event_detection_params_id",
        db.Integer,
        db.ForeignKey("event_detection_params.id"),
        nullable=False,
    )
    datachunk_id = db.Column("datachunk_id", db.Integer, db.ForeignKey("datachunk.id"), nullable=False)
    timespan_id = db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)

    detection_type = db.Column("detection_type", db.UnicodeText, nullable=False)

    time_start = db.Column("time_start", db.TIMESTAMP(timezone=True), nullable=False)
    time_stop = db.Column("time_stop", db.TIMESTAMP(timezone=True), nullable=False)
    peak_ground_velocity = db.Column("peak_ground_velocity", db.Float, nullable=False)
    minimum_frequency = db.Column("minimum_frequency", db.Float, nullable=False)
    maximum_frequency = db.Column("maximum_frequency", db.Float, nullable=False)

    event_detection_file_id = db.Column(
        "event_detection_file_id",
        db.BigInteger,
        db.ForeignKey("event_detection_file.id"),
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

    event_detection_params = db.relationship(
        "EventDetectionParams",
        foreign_keys=[event_detection_params_id],
        uselist=False,
        lazy="joined",
    )

    file = db.relationship(
        "EventDetectionFile",
        foreign_keys=[event_detection_file_id],
        uselist=False,
        lazy="joined",
    )

    def load_data(self) -> obspy.Trace:
        """
        Loads data from associated :py:attr:`noiz.models.event_detection.EventDetectionResult.file.filepath`.

        :return: Loaded trace associated with this result
        :rtype: obspy.Trace
        """
        filepath = Path(self.file.filepath)

        if filepath.exists:
            # FIXME when obspy will be released, str(Path) wont be necessary
            return obspy.read(str(filepath), "MSEED")
        else:
            raise MissingDataFileException(f"Data file for EventDetectionResult {self} is missing")


class EventConfirmationResult(db.Model):
    __tablename__ = "event_confirmation_result"
    __table_args__ = (
        db.UniqueConstraint(
            "timespan_id",
            "event_confirmation_params_id",
            "time_start",
            "time_stop",
            "peak_ground_velocity",
            "number_station_triggered",
            name="unique_confirmation_per_timespan_per_param_per_time",
        ),
    )

    id = db.Column("id", db.BigInteger, primary_key=True)
    event_confirmation_params_id = db.Column(
        "event_confirmation_params_id",
        db.Integer,
        db.ForeignKey("event_confirmation_params.id"),
        nullable=False,
    )
    event_confirmation_run_id = db.Column(
        "event_confirmation_run_id",
        db.Integer,
        db.ForeignKey("event_confirmation_run.id"),
        nullable=False,
    )
    timespan_id = db.Column("timespan_id", db.Integer, db.ForeignKey("timespan.id"), nullable=False)
    time_start = db.Column("time_start", db.TIMESTAMP(timezone=True), nullable=False)
    time_stop = db.Column("time_stop", db.TIMESTAMP(timezone=True), nullable=False)
    peak_ground_velocity = db.Column("peak_ground_velocity", db.Float, nullable=False)
    number_station_triggered = db.Column("number_station_triggered", db.Integer, nullable=False)
    event_confirmation_file_id = db.Column(
        "event_confirmation_file_id",
        db.BigInteger,
        db.ForeignKey("event_confirmation_file.id"),
        nullable=True,
    )

    timespan = db.relationship(
        "Timespan",
        foreign_keys=[timespan_id],
        uselist=False,
        lazy="joined",
    )

    event_confirmation_run = db.relationship(
        "EventConfirmationRun",
        foreign_keys=[event_confirmation_run_id],
        uselist=False,
        lazy="joined",
    )

    event_confirmation_params = db.relationship(
        "EventConfirmationParams",
        foreign_keys=[event_confirmation_params_id],
        uselist=False,
        lazy="joined",
    )

    file = db.relationship(
        "EventConfirmationFile",
        foreign_keys=[event_confirmation_file_id],
        uselist=False,
        lazy="joined",
    )

    event_detection_results = db.relationship(
        "EventDetectionResult",
        lazy="joined",
        secondary=association_table_event_confirmation_result_event_detection_result)

    def load_data(self) -> obspy.Trace:
        """
        Loads data from associated :py:attr:`noiz.models.event_detection.EventConfirmationResult.file.filepath`.

        :return: Loaded trace associated with this result
        :rtype: obspy.Trace
        """
        filepath = Path(self.file.filepath)

        if filepath.exists:
            # FIXME when obspy will be released, str(Path) wont be necessary
            return obspy.read(str(filepath), "MSEED")
        else:
            raise MissingDataFileException(f"Data file for EventConfirmationResult {self} is missing")


class EventConfirmationRun(db.Model):
    __tablename__ = "event_confirmation_run"

    id = db.Column("id", db.BigInteger, primary_key=True)
    specific_stations_params = db.Column("specific_stations_params", db.UnicodeText, nullable=True)

    event_confirmation_params_id = db.Column(
        "event_confirmation_params_id",
        db.Integer,
        db.ForeignKey("event_confirmation_params.id"),
        nullable=False,
    )
    event_detection_type = db.Column(
        "event_detection_type",
        db.UnicodeText,
        nullable=False,
    )

    event_confirmation_params = db.relationship(
        "EventConfirmationParams",
        foreign_keys=[event_confirmation_params_id],
        uselist=False,
        lazy="joined",
    )

    datachunks = db.relationship(
        "Datachunk",
        lazy="joined",
        secondary=association_table_event_confirmation_run_datachunk)

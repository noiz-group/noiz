# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from loguru import logger
from obspy.core.util import AttribDict
from obspy import read_inventory
from pathlib import Path
from typing import Optional, Tuple, TYPE_CHECKING
from numpy import deprecate_with_doc
from sqlalchemy import func

import utm
import datetime

from noiz.database import db
from noiz.exceptions import MissingDataFileException
from noiz.validation_helpers import validate_timestamp_as_pydatetime

if TYPE_CHECKING:
    # Use this to make hybrid_property's have the same typing as a normal property until stubs are improved.
    typed_hybrid_property = property
else:
    from sqlalchemy.ext.hybrid import hybrid_property as typed_hybrid_property


class Device(db.Model):
    __tablename__ = "device"
    __table_args__ = (
        db.UniqueConstraint(
            "network", "station", name="unique_device_per_station"
        ),
    )

    id = db.Column("id", db.Integer, primary_key=True)
    network = db.Column("network", db.UnicodeText)
    station = db.Column("station", db.UnicodeText)
    components = db.relationship("Component", uselist=True, back_populates="device")
    avg_soh_gps = db.relationship("AveragedSohGps", uselist=True, back_populates="device")


class Component(db.Model):
    __tablename__ = "component"
    __table_args__ = (
        db.UniqueConstraint(
            "network", "station", "component", name="unique_component_per_station"
        ),
    )
    id = db.Column("id", db.Integer, primary_key=True)
    network = db.Column("network", db.UnicodeText)
    station = db.Column("station", db.UnicodeText)
    component = db.Column("component", db.UnicodeText)
    inventory_filepath = db.Column("inventory_filepath", db.UnicodeText)
    lat = db.Column("lon", db.Float)
    lon = db.Column("lat", db.Float)
    x = db.Column("x", db.Float)
    y = db.Column("y", db.Float)
    zone = db.Column("zone", db.Integer)
    northern = db.Column("northern", db.Boolean)
    elevation = db.Column("elevation", db.Float)
    device_id = db.Column(
        "device_id",
        db.Integer,
        db.ForeignKey("device.id"),
        nullable=True,
    )
    component_file_id = db.Column(
        "component_file_id",
        db.BigInteger,
        db.ForeignKey("component_file.id"),
        nullable=True,
    )

    device = db.relationship(
        "Device",
        foreign_keys=[device_id],
        uselist=False,
    )
    component_file = db.relationship(
        "ComponentFile",
        foreign_keys=[component_file_id],
        uselist=False,
        lazy="joined",
    )

    start_date: datetime.datetime = db.Column(
        "start_date", db.TIMESTAMP(timezone=True), nullable=False
    )

    end_date: datetime.datetime = db.Column(
        "end_date", db.TIMESTAMP(timezone=True), nullable=False
    )

    def __init__(self, **kwargs):
        super(Component, self).__init__(**kwargs)
        lat = kwargs.get("lat")
        lon = kwargs.get("lon")
        x = kwargs.get("x")
        y = kwargs.get("y")
        zone = kwargs.get("zone")
        northern = kwargs.get("northern")
        try:
            self.start_date: datetime.datetime = validate_timestamp_as_pydatetime(kwargs["start_date"])
            self.end_date: datetime.datetime = validate_timestamp_as_pydatetime(kwargs["end_date"])
        except KeyError as e:
            raise KeyError(f"Both `start_date` and `end_date` needs to be provided to initialize Component. "
                           f"{e} was not provided. Please add it.")

        if not all((lat, lon)):
            if not all((x, y, zone)):
                raise ValueError("You need to provide location either in UTM or latlon")
            else:
                zone, northern = self.__validate_zone_hemisphere(
                    zone=zone, northern=northern
                )
                self._set_latlon_from_xy(x, y, zone, northern)
        else:
            self._set_xy_from_latlon(lat, lon)

    def _make_station_string(self):
        return f"{self.network}.{self.station}.{self.component}"

    def __str__(self):
        return f"{self.id}.{self._make_station_string()}"

    def __repr__(self):
        return f"Component {self.id}.{self._make_station_string()}"

    def _set_xy_from_latlon(self, lat, lon):
        x, y, zone, zone_letter = utm.from_latlon(lat, lon)
        self.x = x
        self.y = y
        self.zone = zone
        # Checks if zone letter is in the norther hemisphere
        self.northern = self.__checkif_zone_letter_in_northern(zone_letter=zone_letter)
        return

    def _set_latlon_from_xy(self, x, y, zone, northern):
        lat, lon = utm.to_latlon(
            easting=x, northing=y, zone_number=zone, northern=northern
        )
        self.lat = lat
        self.lon = lon

    @deprecate_with_doc(msg="This function is deprecated. use load_data instead.")
    def read_inventory(self):
        """
        Deprecated. Use load_data
        """
        return self.load_data()

    def load_data(self):
        filepath = Path(self.component_file.filepath)
        if filepath.exists:
            # FIXME when obspy will be released, str(Path) wont be necesary
            return read_inventory(str(filepath), format="stationxml")
        else:
            raise MissingDataFileException(f"Inventory file for component {self} is missing")

    def get_location_as_attribdict(self):
        return AttribDict(
            {"latitude": self.lat, "longitude": self.lon, "elevation": self.elevation}
        )

    @staticmethod
    def __checkif_zone_letter_in_northern(zone_letter: str) -> bool:
        return zone_letter in ("X", "W", "V", "U", "T", "S", "R", "Q", "P", "N")

    @staticmethod
    def __validate_zone_hemisphere(
        zone: Optional[int], northern: Optional[bool]
    ) -> Tuple[int, bool]:
        if zone is None:
            logger.warning("Zone is not set, using default 32.")
            zone = 32
        if northern is None:
            logger.warning("Northern is not set, using default True.")
            northern = True
        return zone, northern

    @typed_hybrid_property
    def start_date_year(self) -> int:
        return self.start_date.year

    @start_date_year.expression  # type: ignore
    def start_date_year(cls) -> int:  # type: ignore
        return func.date_part("year", cls.start_date)  # type: ignore

    @typed_hybrid_property
    def start_date_doy(self) -> int:
        return self.start_date.timetuple().tm_yday

    @start_date_doy.expression
    def start_date_doy(cls) -> int:
        return func.date_part("doy", cls.start_date)  # type: ignore

    @typed_hybrid_property
    def start_date_isoweekday(self) -> int:
        return self.end_date.isoweekday()

    @start_date_isoweekday.expression
    def start_date_isoweekday(cls) -> int:
        return func.date_part("isodow", cls.start_date)  # type: ignore

    @typed_hybrid_property
    def start_date_hour(self) -> int:
        return self.start_date.hour

    @start_date_hour.expression
    def start_date_hour(cls) -> int:
        return func.date_part("hour", cls.start_date)  # type: ignore

    @typed_hybrid_property
    def end_date_year(self) -> int:
        return self.end_date.year

    @end_date_year.expression
    def end_date_year(cls) -> int:
        return func.date_part("year", cls.end_date)  # type: ignore

    @typed_hybrid_property
    def end_date_doy(self) -> int:
        return self.end_date.timetuple().tm_yday

    @end_date_doy.expression
    def end_date_doy(cls) -> int:
        return func.date_part("doy", cls.end_date)  # type: ignore

    @typed_hybrid_property
    def end_date_isoweekday(self) -> int:
        return self.end_date.isoweekday()

    @end_date_isoweekday.expression
    def end_date_isoweekday(cls) -> int:
        return func.date_part("isodow", cls.end_date)  # type: ignore

    @typed_hybrid_property
    def end_date_hour(self) -> int:
        return self.end_date.hour

    @end_date_hour.expression
    def end_date_hour(cls) -> int:
        return func.date_part("hour", cls.end_date)  # type: ignore


class ComponentFile(db.Model):
    __tablename__ = "component_file"

    id = db.Column("id", db.BigInteger, primary_key=True)
    filepath = db.Column("filepath", db.UnicodeText, nullable=False)

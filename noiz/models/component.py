from typing import Optional, Tuple

import utm
import obspy

from noiz.database import db

from flask.logging import logging

logger = logging.getLogger(__name__)


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

    def __init__(self, **kwargs):
        super(Component, self).__init__(**kwargs)
        lat = kwargs.get("lat")
        lon = kwargs.get("lon")
        x = kwargs.get("x")
        y = kwargs.get("y")
        zone = kwargs.get("zone")
        northern = kwargs.get("northern")

        if not all((lat, lon)):
            if not all((x, y)):
                raise ValueError("You need to provide location either in UTM or latlon")
            else:
                zone, northern = self.__validate_zone_hemisphere(northern, zone)
                self._set_latlon_from_xy(x, y, zone, northern)
        else:
            self._set_xy_from_latlon(lat, lon)

    def _make_station_string(self):
        return f"{self.network}.{self.station}.{self.component}"

    def __str__(self):
        return f"Station {self._make_station_string()}"

    def _set_xy_from_latlon(self, lat, lon):
        x, y, zone, zone_letter = utm.from_latlon(lat, lon)
        self.x = x
        self.y = y
        self.zone = zone
        # Checks if zone letter is in the norther hemisphere
        self.northern = self.__checkif_zone_letter_in_northern(zone_letter=zone_letter)
        return

    def _set_latlon_from_xy(self, x, y, zone, northern):
        lat, lon = utm.to_latlon(x, y, zone, northern)
        self.lat = lat
        self.lon = lon

    def read_inventory(self):
        return obspy.read_inventory(self.inventory_filepath, format="stationxml")

    @staticmethod
    def __checkif_zone_letter_in_northern(zone_letter: str) -> bool:
        return zone_letter in ("X", "W", "V", "U", "T", "S", "R", "Q", "P", "N")

    @staticmethod
    def __validate_zone_hemisphere(
        northern: Optional[int], zone: Optional[bool]
    ) -> Tuple[int, bool]:
        if zone is None:
            logger.warning("Zone is not set, using default 32.")
            zone = 32
        if northern is None:
            logger.warning("Northern is not set, using default True.")
            northern = True
        return zone, northern

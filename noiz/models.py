from noiz.database import db
from sqlalchemy.dialects.postgresql import HSTORE, NUMRANGE, ARRAY

from typing import Optional, Tuple

import utm

from flask.logging import logging

logger = logging.getLogger(__name__)


class ProcessingConfig(db.Model):
    __tablename__ = "processingconfig"
    id = db.Column("id", db.Integer, primary_key=True)
    use_winter_time = db.Column("use_winter_time", db.Boolean)
    f_sampling_out = db.Column("f_sampling_out", db.Integer)
    downsample_filt_order = db.Column("downsample_filt_order", db.Integer)
    # default value  chosen so that the window length corresponds to 2^n samples for sampling frequencies Fs=25*2^{m} with m any positive integer
    window_length_spectrum_sec = db.Column("window_length_spectrum_sec", db.Float)
    window_spectrum_overlap = db.Column("window_spectrum_overlap", db.Float)
    # Method for rejecting the noisiest time windows for spectral estimation. The criterion is defined on the energy of the windows within the frequency band specified by f_min_reject_crit and f_max_reject_crit. The window population on which the criterion is applied is defined by window_spectrum_num_for_stats.
    # possible values:
    # - 'mean-std' : reject windows with energy above mean+(this)*std over the population
    # - 'quantile': reject windows with energy above quantile(this) over the population
    # - None
    window_spectrum_reject_method = db.Column(
        "window_spectrum_reject_method", db.String(50)
    )
    # std reject criterion
    # used if window_spectrum_reject_method=mean-std
    window_spectrum_crit_std = db.Column("window_spectrum_crit_std", db.Float)
    # quantile reject criterion
    # used if window_spectrum_reject_method=quantile
    window_spectrum_crit_quantile = db.Column("window_spectrum_crit_quantile", db.Float)
    # number of adjacent spectral windows to take for computing statistics and defining reject criteria
    window_spectrum_num_for_stat = db.Column("window_spectrum_num_for_stat", db.Integer)
    # value in seconds set so that
    # sequence=25*window_spectrum
    sequence_length_sec = db.Column("sequence_length_sec", db.Float)
    sequence_overlap = db.Column("sequence_overlap", db.Float)
    # Method for rejecting the noisiest sequencies. The criterion is defined on the energy of the sequencies within the frequency band specified by f_min_reject_crit and f_max_reject_crit. The sequence population on which the criterion is applied is defined by sequence_num_for_stats.
    # possible values:
    # - 'mean-std' : reject sequences with energy above mean+(this)*std over the population
    # - 'quantile': reject reject sequences with energy above quantile(this) over the population
    sequence_reject_method = db.Column("sequence_reject_method", db.String(50))
    # analog to window_spectrum_crit_std for sequences
    sequence_crit_std = db.Column("sequence_crit_std", db.Float)
    # analog to window_spectrum_crit_quantile for sequences
    sequence_crit_quantile = db.Column("sequence_crit_quantile", db.Float)
    # number of adjacent sequencies to take for computing statistics and defining reject criteria
    sequence_num_for_stat = db.Column("sequence_num_for_stat", db.Integer)
    # which proportion of the spectral estimation windows within a sequence are allowed to be rejected ? (if more that this, the whole sequence is rejected)
    sequence_reject_tolerance_on_window_spectrum = db.Column(
        "sequence_reject_tolerance_on_window_spectrum", db.Float
    )
    f_min_reject_crit = db.Column("f_min_reject_crit", db.Float)
    f_max_reject_crit = db.Column("f_max_reject_crit", db.Float)
    # filter to be used for all filtering operations when time domain output needed
    # 'fourier_cosine_taper' or 'butterworth'
    filter_type = db.Column("filter_type", db.String(50))
    # taper to be used in time domain before fft
    # 'cosine'
    # 'hanning'
    #
    taper_time_type = db.Column("taper_time_type", db.String(50))
    # width of the  transition region in cosine taper
    # relative to the maximum period of processing
    taper_time_width_periods = db.Column("taper_time_width_periods", db.Float)
    # minimum number of time samples in the transition region of cosine taper
    taper_time_width_min_samples = db.Column("taper_time_width_min_samples", db.Integer)
    # maximum length of the transition region in the cosine taper as ratio over the whole length of the processed time series
    taper_time_width_max_proportion = db.Column(
        "taper_time_width_max_proportion", db.Float
    )
    # taper to be used in freq domain before ifft
    # 'cosine'
    # 'hanning'
    taper_freq_type = db.Column("taper_freq_type", db.String(50))
    # width of the transition region in cosine taper
    # relative to the width of the queried frequency band
    taper_freq_width_proportion = db.Column("taper_freq_width_proportion", db.Float)
    # analog to taper_time_width_min_samples
    taper_freq_width_min_samples = db.Column("taper_freq_width_min_samples", db.Integer)
    # maximum width of the  transition region in cosine taper
    # relative to the minimum frequency of processing
    taper_freq_width_max_freqs = db.Column("taper_freq_width_max_freqs", db.Float)


class Tsindex(db.Model):
    __tablename__ = "tsindex"
    id = db.Column("id", db.BigInteger, primary_key=True)
    network = db.Column("network", db.UnicodeText, nullable=False)
    station = db.Column("station", db.UnicodeText, nullable=False)
    location = db.Column("location", db.UnicodeText, nullable=False)
    channel = db.Column("channel", db.UnicodeText, nullable=False)
    quality = db.Column("quality", db.UnicodeText)
    version = db.Column("version", db.Integer)
    starttime = db.Column("starttime", db.TIMESTAMP(timezone=True), nullable=False)
    endtime = db.Column("endtime", db.TIMESTAMP(timezone=True), nullable=False)
    samplerate = db.Column("samplerate", db.NUMERIC, nullable=False)
    filename = db.Column("filename", db.UnicodeText, nullable=False)
    byteoffset = db.Column("byteoffset", db.BigInteger, nullable=False)
    bytes = db.Column("bytes", db.BigInteger)
    hash = db.Column("hash", db.UnicodeText)
    timeindex = db.Column("timeindex", HSTORE)
    timespans = db.Column("timespans", ARRAY(NUMRANGE))
    timerates = db.Column("timerates", ARRAY(db.NUMERIC))
    format = db.Column("format", db.UnicodeText)
    filemodtime = db.Column("filemodtime", db.TIMESTAMP(timezone=True), nullable=False)
    updated = db.Column("updated", db.TIMESTAMP(timezone=True), nullable=False)
    scanned = db.Column("scanned", db.TIMESTAMP(timezone=True), nullable=False)


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


class Soh(db.Model):
    __tablename__ = "soh"
    __table_args__ = (
        db.UniqueConstraint(
            "datetime", "component_id", name="unique_timestamp_per_station"
        ),
    )

    id = db.Column("id", db.Integer, primary_key=True)
    component_id = db.Column("component_id", db.Integer, db.ForeignKey("component.id"))
    datetime = db.Column("datetime", db.TIMESTAMP(timezone=True), nullable=False)
    voltage = db.Column("voltage", db.Float, nullable=True)
    current = db.Column("current", db.Float, nullable=True)
    temperature = db.Column("temperature", db.Float, nullable=True)

    def to_dict(self):
        return {
            "datetime": self.datetime,
            "voltage": self.voltage,
            "current": self.current,
            "temperature": self.temperature,
        }

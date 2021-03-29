from functools import cached_property

from noiz.models.mixins import NotNullColumn
from noiz.validation_helpers import validate_exactly_one_argument_provided

from typing import Optional, Union, Tuple
from pydantic.dataclasses import dataclass
import numpy as np
import numpy.typing as npt

from noiz.database import db
from noiz.globals import ExtendedEnum


class ZeroPaddingMethod(ExtendedEnum):
    # filldocs
    INTERPOLATED = "interpolated"
    PADDED = "padded"
    TAPERED_PADDED = "tapered_padded"


class DatachunkParams(db.Model):
    __tablename__ = "datachunk_params"

    id = db.Column("id", db.Integer, primary_key=True)

    _sampling_rate = db.Column("sampling_rate", db.Float, default=24, nullable=False)

    _prefiltering_low = db.Column("prefiltering_low", db.Float, default=0.01, nullable=False)
    _prefiltering_high = db.Column("prefiltering_high", db.Float, default=12.0, nullable=False)
    _prefiltering_order = db.Column("prefiltering_order", db.Integer, default=4, nullable=False)

    _preprocessing_taper_type = db.Column("preprocessing_taper_type", db.UnicodeText, default="cosine", nullable=False)
    _preprocessing_taper_side = db.Column("preprocessing_taper_side", db.UnicodeText, default="both", nullable=False)
    _preprocessing_taper_max_length = db.Column("preprocessing_taper_max_length", db.Float, nullable=False)
    _preprocessing_taper_max_percentage = db.Column("preprocessing_taper_max_percentage", db.Float, nullable=False)

    _remove_response = db.Column("remove_response", db.Boolean, default=True, nullable=False)

    _datachunk_sample_tolerance = db.Column("datachunk_sample_threshold", db.Float, default=0.98, nullable=False)
    _max_gap_for_merging = db.Column("max_gap_for_merging", db.Integer, default=10, nullable=False)

    _padding_method = db.Column("padding_method", db.UnicodeText, default="padding_with_tapering", nullable=False)
    _padding_taper_type = db.Column("padding_taper_type", db.UnicodeText, nullable=True)
    _padding_taper_max_length = db.Column("padding_taper_max_length", db.Float, nullable=True)
    _padding_taper_max_percentage = db.Column("padding_taper_max_percentage", db.Float, nullable=True)

    # TODO explore if bac_populates here makes sense
    processed_datachunk_params = db.relationship(
        "ProcessedDatachunkParams", uselist=True, back_populates="datachunk_params"
    )

    def __init__(self, **kwargs):
        self._sampling_rate = kwargs.get("sampling_rate", 24)

        self._prefiltering_low = kwargs.get("prefiltering_low", 0.01)
        self._prefiltering_high = kwargs.get("prefiltering_high", 12.0)
        self._prefiltering_order = kwargs.get("prefiltering_order", 4)

        self._preprocessing_taper_type = kwargs.get("preprocessing_taper_type", "cosine")
        self._preprocessing_taper_side = kwargs.get("preprocessing_taper_side", "both")
        self._preprocessing_taper_max_length = kwargs.get("preprocessing_taper_max_length", 5)
        self._preprocessing_taper_max_percentage = kwargs.get("preprocessing_taper_max_percentage", 0.1)

        self._remove_response = kwargs.get("remove_response", True)

        self._spectral_whitening = kwargs.get("spectral_whitening", True)  # deprecatethis
        self._one_bit = kwargs.get("one_bit", True)  # deprecatethis

        if "datachunk_sample_threshold" in kwargs.keys() and "datachunk_sample_tolerance" in kwargs.keys():
            raise ValueError("Only one of `datachunk_sample_threshold` or `datachunk_sample_tolerance` "
                             "accepted at time.")

        if "datachunk_sample_threshold" in kwargs.keys():
            self._datachunk_sample_tolerance = 1 - kwargs.get("datachunk_sample_threshold", 0.98)
        else:
            self._datachunk_sample_tolerance = kwargs.get("datachunk_sample_tolerance", 0.02)

        padding_method = kwargs.get("zero_padding_method", "tapered_padded")
        try:
            padding_method_valid = ZeroPaddingMethod(padding_method)
        except ValueError:
            raise ValueError(f"Not supported padding method. Supported types are: {list(ZeroPaddingMethod)}, "
                             f"You provided {padding_method}")
        self._padding_method = padding_method_valid.value

        self._max_gap_for_merging = kwargs.get("max_gap_for_merging", 10)
        self._padding_taper_type = kwargs.get("padding_taper_type", "cosine")
        self._padding_taper_max_length = kwargs.get("padding_taper_max_length", 5)  # seconds
        self._padding_taper_max_percentage = kwargs.get("padding_taper_max_percentage", 0.1)  # percent

        self._correlation_max_lag = kwargs.get("correlation_max_lag", 60)  # deprecatethis

    def as_dict(self):
        return dict(
            datachunk_params_id=self.id,
            datachunk_params_sampling_rate=self.sampling_rate,
            datachunk_params_prefiltering_low=self.prefiltering_low,
            datachunk_params_prefiltering_high=self.prefiltering_high,
            datachunk_params_prefiltering_order=self.prefiltering_order,
            datachunk_params_preprocessing_taper_type=self.preprocessing_taper_type,
            datachunk_params_preprocessing_taper_side=self.preprocessing_taper_side,
            datachunk_params_preprocessing_taper_max_length=self.preprocessing_taper_max_length,
            datachunk_params_preprocessing_taper_max_percentage=self.preprocessing_taper_max_percentage,
            datachunk_params_remove_response=self.remove_response,
            datachunk_params_datachunk_sample_tolerance=self.datachunk_sample_tolerance,
            datachunk_params_max_gap_for_merging=self.max_gap_for_merging,
            datachunk_params_zero_padding_method=self.zero_padding_method,
            datachunk_params_padding_taper_type=self.padding_taper_type,
            datachunk_params_padding_taper_max_length=self.padding_taper_max_length,
            datachunk_params_padding_taper_max_percentage=self.padding_taper_max_percentage,

        )

    @property
    def sampling_rate(self):
        return self._sampling_rate

    @property
    def prefiltering_low(self):
        return self._prefiltering_low

    @property
    def prefiltering_high(self):
        return self._prefiltering_high

    @property
    def prefiltering_order(self):
        return self._prefiltering_order

    @property
    def preprocessing_taper_type(self):
        return self._preprocessing_taper_type

    @property
    def preprocessing_taper_side(self):
        return self._preprocessing_taper_side

    @property
    def preprocessing_taper_max_length(self):
        return self._preprocessing_taper_max_length

    @property
    def preprocessing_taper_max_percentage(self):
        return self._preprocessing_taper_max_percentage

    @property
    def remove_response(self):
        return self._remove_response

    @property
    def spectral_whitening(self):
        return self._spectral_whitening

    @property
    def one_bit(self):
        return self._one_bit

    @property
    def datachunk_sample_tolerance(self):
        """
        Tolerance in decimal percentage of how much overlap/gaps are tolerated in the datachunk candidate.

        :return:
        :rtype: float
        """
        return self._datachunk_sample_tolerance

    @property
    def max_gap_for_merging(self):
        return self._max_gap_for_merging

    @property
    def correlation_max_lag(self):
        """
        Correlation max lag in seconds
        :return Correlation max lag in seconds
        :rtype: float
        """
        return self._correlation_max_lag

    @property
    def zero_padding_method(self):
        return ZeroPaddingMethod(self._padding_method)

    @property
    def padding_taper_type(self):
        return self._padding_taper_type

    @property
    def padding_taper_max_length(self):
        return self._padding_taper_max_length

    @property
    def padding_taper_max_percentage(self):
        return self._padding_taper_max_percentage

    def get_correlation_max_lag_samples(self):
        return int(self._correlation_max_lag * self._sampling_rate)

    def get_correlation_time_vector(self) -> npt.ArrayLike:
        """
        Return a np.array containing time vector for cross correlation to required correlation max lag.
        Compatible with obspy's method to calculate correlation.
        It returns 2*(max_lag*sampling_rate)+1 samples.

        :return: Time vector for ccf
        :rtype: np.array
        """
        step = 1 / self._sampling_rate
        start = -self._correlation_max_lag
        stop = self._correlation_max_lag + step

        return np.arange(start=start, stop=stop, step=step)


@dataclass
class DatachunkParamsHolder:
    """
        This simple dataclass is just helping to validate :class:`~noiz.models.DatachunkParams` values loaded
        from the TOML file
    """
    sampling_rate: float
    prefiltering_low: float
    prefiltering_high: float
    prefiltering_order: int
    preprocessing_taper_type: str
    preprocessing_taper_side: str
    preprocessing_taper_max_length: float
    preprocessing_taper_max_percentage: float
    remove_response: bool
    datachunk_sample_tolerance: float
    zero_padding_method: str
    padding_taper_type: str
    padding_taper_max_length: float
    padding_taper_max_percentage: float


@dataclass
class ProcessedDatachunkParamsHolder:
    """
        This simple dataclass is just helping to validate :py:class:`~noiz.models.ProcessedDatachunkParams`
        values loaded from the TOML file
    """
    datachunk_params_id: int
    qcone_config_id: Optional[int]
    spectral_whitening: bool
    one_bit: bool


class ProcessedDatachunkParams(db.Model):
    __tablename__ = "processed_datachunk_params"

    id = db.Column("id", db.Integer, primary_key=True)
    datachunk_params_id = db.Column(
        "datachunk_params_id", db.Integer, db.ForeignKey("datachunk_params.id"), nullable=False
    )
    qcone_config_id = db.Column("qcone_config_id", db.Integer, db.ForeignKey("qcone_config.id"), nullable=False)

    _spectral_whitening = db.Column("spectral_whitening", db.Boolean, default=True, nullable=False)
    _one_bit = db.Column("one_bit", db.Boolean, default=True, nullable=False)

    datachunk_params = db.relationship(
        "DatachunkParams",
        foreign_keys=[datachunk_params_id],
        back_populates="processed_datachunk_params",
        lazy="joined",
    )
    qcone_config = db.relationship(
        "QCOneConfig",
        foreign_keys=[qcone_config_id],
        back_populates="processed_datachunk_params",
        lazy="joined",
    )

    crosscorrelation_params = db.relationship(
        "CrosscorrelationParams", uselist=True,
    )

    def __init__(self, **kwargs):
        self.datachunk_params_id = kwargs.get("datachunk_params_id")
        self.qcone_config_id = kwargs.get("qcone_config_id")
        self._spectral_whitening = kwargs.get("spectral_whitening", True)
        self._one_bit = kwargs.get("one_bit", True)

    def as_dict(self):
        return dict(
            processeddatachunk_params_id=self.id,
            processeddatachunk_params_datachunk_params_id=self.datachunk_params_id,
            processeddatachunk_params_qcone_config_id=self.qcone_config_id,
            processeddatachunk_params_spectral_whitening=self.spectral_whitening,
            processeddatachunk_params_one_bit=self.one_bit,
        )

    @property
    def spectral_whitening(self):
        return self._spectral_whitening

    @property
    def one_bit(self):
        return self._one_bit


@dataclass
class CrosscorrelationParamsHolder:
    """
        This simple dataclass is just helping to validate :py:class:`~noiz.models.CrosscorrelationParams`
        values loaded from the TOML file
    """
    processed_datachunk_params_id: int
    correlation_max_lag: int


class CrosscorrelationParams(db.Model):
    __tablename__ = "crosscorrelation_params"

    id = db.Column("id", db.Integer, primary_key=True)
    processed_datachunk_params_id = db.Column(
        "processed_datachunk_params_id", db.Integer, db.ForeignKey("processed_datachunk_params.id"), nullable=False
    )
    _correlation_max_lag = db.Column("correlation_max_lag", db.Float, nullable=False)
    _sampling_rate = db.Column("sampling_rate", db.Float, default=24, nullable=False)

    processed_datachunk_params = db.relationship(
        "ProcessedDatachunkParams",
        foreign_keys=[processed_datachunk_params_id],
        back_populates="crosscorrelation_params",
        lazy="joined",
    )

    def __init__(self, **kwargs):
        self.processed_datachunk_params_id = kwargs.get("processed_datachunk_params_id")
        # This is just duplication of the original param to avoid complications
        self._sampling_rate = kwargs.get("sampling_rate")
        self._correlation_max_lag = kwargs.get("correlation_max_lag", 60)

    def as_dict(self):
        return dict(
            crosscorrelation_params_id=self.id,
            crosscorrelation_params_processed_datachunk_params_id=self.processed_datachunk_params_id,
            crosscorrelation_params_sampling_rate=self.sampling_rate,
            crosscorrelation_params_correlation_max_lag=self.correlation_max_lag,
        )

    @property
    def sampling_rate(self):
        return self._sampling_rate

    @property
    def correlation_max_lag(self):
        return self._correlation_max_lag

    @cached_property
    def correlation_max_lag_samples(self) -> int:
        return int(self.correlation_max_lag * self.sampling_rate)

    @cached_property
    def correlation_time_vector(self) -> npt.ArrayLike:
        """
        Return a np.array containing time vector for cross correlation to required correlation max lag.
        Compatible with obspy's method to calculate correlation.
        It returns 2*(max_lag*sampling_rate)+1 samples.

        :return: Time vector for ccf
        :rtype: np.array
        """
        step = 1 / self.sampling_rate
        start = -self.correlation_max_lag
        stop = self.correlation_max_lag + step

        return np.arange(start=start, stop=stop, step=step)


@dataclass
class BeamformingParamsHolder:
    """
        This simple dataclass is just helping to validate :class:`~noiz.models.BeamformingParams` values loaded
        from the TOML file
    """
    qcone_config_id: int
    min_freq: float
    max_freq: float
    slowness_x_min: float
    slowness_x_max: float
    slowness_y_min: float
    slowness_y_max: float
    slowness_step: float
    window_length_minimum_periods: Optional[float] = None
    window_length: Optional[float] = None
    window_step_fraction: Optional[float] = None
    window_step: Optional[float] = None
    save_average_beamformer_abspower: bool = True
    save_all_beamformers_abspower: bool = False
    save_average_beamformer_relpower: bool = False
    save_all_beamformers_relpower: bool = False
    extract_peaks_average_beamformer_abspower: bool = True
    extract_peaks_all_beamformers_abspower: bool = False
    extract_peaks_average_beamformer_relpower: bool = False
    extract_peaks_all_beamformers_relpower: bool = False
    neighborhood_size: Optional[float] = None
    neighborhood_size_xaxis_fraction: Optional[float] = None
    maxima_threshold: float = 0.
    best_point_count: int = 10
    beam_portion_threshold: float = 0.1
    semblance_threshold: float = -1e9
    velocity_threshold: float = -1e9
    prewhiten: bool = False
    method: str = "beamforming"
    used_component_codes: Tuple[str, ...] = ("Z", )
    minimum_trace_count: int = 3


class BeamformingParams(db.Model):
    __tablename__ = "beamforming_params"
    id = db.Column("id", db.Integer, primary_key=True)
    qcone_config_id = NotNullColumn("qcone_config_id", db.Integer, db.ForeignKey("qcone_config.id"))
    min_freq = NotNullColumn("min_freq", db.Float)
    max_freq = NotNullColumn("max_freq", db.Float)
    slowness_x_min = NotNullColumn("slowness_x_min", db.Float)
    slowness_x_max = NotNullColumn("slowness_x_max", db.Float)
    slowness_y_min = NotNullColumn("slowness_y_min", db.Float)
    slowness_y_max = NotNullColumn("slowness_y_max", db.Float)
    slowness_step = NotNullColumn("slowness_step", db.Float)

    window_length = NotNullColumn("window_length", db.Float)
    window_step = NotNullColumn("window_step", db.Float)

    save_average_beamformer_abspower = NotNullColumn("save_average_beamformer_abspower", db.Boolean)
    save_all_beamformers_abspower = NotNullColumn("save_all_beamformers_abspower", db.Boolean)
    save_average_beamformer_relpower = NotNullColumn("save_average_beamformer_relpower", db.Boolean)
    save_all_beamformers_relpower = NotNullColumn("save_all_beamformers_relpower", db.Boolean)

    extract_peaks_average_beamformer_abspower = NotNullColumn("extract_peaks_average_beamformer_abspower", db.Boolean)
    extract_peaks_all_beamformers_abspower = NotNullColumn("extract_peaks_all_beamformers_abspower", db.Boolean)
    extract_peaks_average_beamformer_relpower = NotNullColumn("extract_peaks_average_beamformer_relpower", db.Boolean)
    extract_peaks_all_beamformers_relpower = NotNullColumn("extract_peaks_all_beamformers_relpower", db.Boolean)

    neighborhood_size = NotNullColumn("neighborhood_size", db.Float)
    maxima_threshold = NotNullColumn("maxima_threshold", db.Float)
    best_point_count = NotNullColumn("best_point_count", db.Integer)
    beam_portion_threshold = NotNullColumn("beam_portion_threshold", db.Float)

    semblance_threshold = NotNullColumn("semblance_threshold", db.Float)
    velocity_threshold = NotNullColumn("velocity_threshold", db.Float)
    prewhiten = NotNullColumn("prewhiten", db.Boolean)
    _method = NotNullColumn("method", db.String)
    _used_component_codes = NotNullColumn("used_component_codes", db.String)
    minimum_trace_count = NotNullColumn("minimum_trace_count", db.Integer)

    qcone_config = db.relationship(
        "QCOneConfig",
        foreign_keys=[qcone_config_id],
        back_populates="beamforming_params",
        lazy="joined",
    )

    def __init__(
            self,
            qcone_config_id: int,
            min_freq: float,
            max_freq: float,
            slowness_x_min: float,
            slowness_x_max: float,
            slowness_y_min: float,
            slowness_y_max: float,
            slowness_step: float,
            window_length_minimum_periods: Optional[float],
            window_length: Optional[float],
            window_step_fraction: Optional[float],
            window_step: Optional[float],
            save_average_beamformer_abspower: bool,
            save_all_beamformers_abspower: bool,
            save_average_beamformer_relpower: bool,
            save_all_beamformers_relpower: bool,
            extract_peaks_average_beamformer_abspower: bool,
            extract_peaks_all_beamformers_abspower: bool,
            extract_peaks_average_beamformer_relpower: bool,
            extract_peaks_all_beamformers_relpower: bool,
            neighborhood_size: Optional[float],
            neighborhood_size_xaxis_fraction: Optional[float],
            maxima_threshold: float,
            best_point_count: int,
            beam_portion_threshold: float,
            semblance_threshold: float,
            velocity_threshold: float,
            prewhiten: bool,
            method: str,
            used_component_codes: Tuple[str, ...],
            minimum_trace_count: int,

    ):
        validate_exactly_one_argument_provided(window_length, window_length_minimum_periods)
        validate_exactly_one_argument_provided(window_step, window_step_fraction)
        validate_exactly_one_argument_provided(neighborhood_size, neighborhood_size_xaxis_fraction)

        self.qcone_config_id = qcone_config_id
        self.min_freq = min_freq
        self.max_freq = max_freq
        self.slowness_x_min = slowness_x_min
        self.slowness_x_max = slowness_x_max
        self.slowness_y_min = slowness_y_min
        self.slowness_y_max = slowness_y_max
        self.slowness_step = slowness_step

        if window_length is not None:
            self.window_length = window_length
        elif window_length_minimum_periods is not None:
            self.window_length = window_length_minimum_periods/min_freq

        if window_step is not None:
            self.window_step = window_step
        elif window_step_fraction is not None:
            self.window_step = self.window_length*window_step_fraction

        self.save_average_beamformer_abspower = save_average_beamformer_abspower
        self.save_all_beamformers_abspower = save_all_beamformers_abspower
        self.save_average_beamformer_relpower = save_average_beamformer_relpower
        self.save_all_beamformers_relpower = save_all_beamformers_relpower

        self.extract_peaks_average_beamformer_abspower = extract_peaks_average_beamformer_abspower
        self.extract_peaks_all_beamformers_abspower = extract_peaks_all_beamformers_abspower
        self.extract_peaks_average_beamformer_relpower = extract_peaks_average_beamformer_relpower
        self.extract_peaks_all_beamformers_relpower = extract_peaks_all_beamformers_relpower

        if neighborhood_size is not None:
            self.neighborhood_size = neighborhood_size
        elif neighborhood_size_xaxis_fraction is not None:
            self.neighborhood_size = len(self.get_xaxis())*neighborhood_size_xaxis_fraction
        self.maxima_threshold = maxima_threshold
        self.best_point_count = int(best_point_count)
        self.beam_portion_threshold = beam_portion_threshold

        self.semblance_threshold = semblance_threshold
        self.velocity_threshold = velocity_threshold

        self.prewhiten = prewhiten
        if method in ("beamforming", "capon"):
            self._method = method
        else:
            raise ValueError(f"Expected either 'beamforming' or 'capon'. Got {method}")
        self._used_component_codes = ";".join(used_component_codes)
        if minimum_trace_count >= 1:
            self.minimum_trace_count = int(minimum_trace_count)
        else:
            ValueError("minimum_trace_count cannot be lower than one.")

    @property
    def method(self):
        if self._method == "beamforming":
            return 0
        if self._method == "capon":
            return 1

    @property
    def window_fraction(self):
        return self.window_length/self.window_step

    @property
    def used_component_codes(self):
        return tuple(self._used_component_codes.split(';'))

    def get_xaxis(self):
        return np.arange(
            start=self.slowness_x_min,
            stop=self.slowness_x_max+self.slowness_step/2,
            step=self.slowness_step
        )

    def get_yaxis(self):
        return np.arange(
            start=self.slowness_y_min,
            stop=self.slowness_y_max+self.slowness_step/2,
            step=self.slowness_step
        )

    @property
    def save_abspow(self):
        return any([
            self.save_average_beamformer_abspower,
            self.save_all_beamformers_abspower,
            self.extract_peaks_average_beamformer_abspower,
            self.extract_peaks_all_beamformers_abspower,

        ])

    @property
    def save_relpow(self):
        return any([
            self.save_average_beamformer_relpower,
            self.save_all_beamformers_relpower,
            self.extract_peaks_average_beamformer_relpower,
            self.extract_peaks_all_beamformers_relpower,

        ])


@dataclass
class PPSDParamsHolder:
    """
        This simple dataclass is just helping to validate :class:`~noiz.models.PPSDParams` values loaded
        from the TOML file
    """
    datachunk_params_id: int

    segment_length: float  # seconds
    segment_step: float  # seconds
    freq_min: float
    freq_max: float
    rejected_quantile: float
    save_all_windows: bool = False
    save_compressed: bool = True


class PPSDParams(db.Model):
    __tablename__ = "ppsd_params"
    id = db.Column("id", db.Integer, primary_key=True)

    datachunk_params_id = db.Column(
        "datachunk_params_id",
        db.Integer,
        db.ForeignKey("datachunk_params.id"),
        nullable=False
    )
    segment_length = db.Column("segment_length", db.Float, nullable=False)
    segment_step = db.Column("segment_overlap", db.Float, nullable=False)
    freq_min = db.Column("freq_min", db.Float, nullable=False)
    freq_max = db.Column("freq_max", db.Float, nullable=False)
    rejected_quantile = db.Column("rejected_quantile", db.Float, nullable=False)
    save_all_windows = db.Column("save_all_windows", db.Boolean, nullable=False)
    save_compressed = db.Column("save_compressed", db.Boolean, nullable=False)

    sampling_rate = db.Column("sampling_rate", db.Float, nullable=False)

    datachunk_params = db.relationship(
        "DatachunkParams",
        foreign_keys=[datachunk_params_id],
        lazy="joined",
    )

    def __init__(
            self,
            datachunk_params_id: int,
            segment_length: float,
            segment_step: float,
            sampling_rate: Union[float, int],
            freq_min: float,
            freq_max: float,
            rejected_quantile: float,
            save_all_windows: bool,
            save_compressed: bool,
    ):
        self.datachunk_params_id = datachunk_params_id
        self.segment_length = segment_length
        self.segment_step = segment_step
        self.freq_min = freq_min
        self.freq_max = freq_max
        self.rejected_quantile = rejected_quantile
        self.save_all_windows = save_all_windows
        self.save_compressed = save_compressed

        self.sampling_rate = float(sampling_rate)

    @property
    def expected_sample_count(self):
        return int(self.sampling_rate * self.segment_length)

    @property
    def sample_spacing(self):
        return 1/self.sampling_rate

    @cached_property
    def _all_fft_freqs(self):
        from scipy.fft import fftfreq
        return fftfreq(n=self.expected_sample_count, d=self.sample_spacing)

    @cached_property
    def _where_accepted_freqs(self):
        freqs = self._all_fft_freqs
        return np.where((freqs >= self.freq_min) & (freqs <= self.freq_max))[0]

    @cached_property
    def expected_fft_freq(self):
        return self._all_fft_freqs[self._where_accepted_freqs]

    # use_winter_time = db.Column("use_winter_time", db.Boolean)
    # f_sampling_out = db.Column("f_sampling_out", db.Integer)
    # downsample_filt_order = db.Column("downsample_filt_order", db.Integer)
    # # default value  chosen so that the window length corresponds to 2^n samples for sampling frequencies Fs=25*2^{m} with m any positive integer
    # window_length_spectrum_sec = db.Column("window_length_spectrum_sec", db.Float)
    # window_spectrum_overlap = db.Column("window_spectrum_overlap", db.Float)
    # # Method for rejecting the noisiest time windows for spectral estimation. The criterion is defined on the energy of the windows within the frequency band specified by f_min_reject_crit and f_max_reject_crit. The window population on which the criterion is applied is defined by window_spectrum_num_for_stats.
    # # possible values:
    # # - 'mean-std' : reject windows with energy above mean+(this)*std over the population
    # # - 'quantile': reject windows with energy above quantile(this) over the population
    # # - None
    # window_spectrum_reject_method = db.Column(
    #     "window_spectrum_reject_method", db.String(50)
    # )
    # # std reject criterion
    # # used if window_spectrum_reject_method=mean-std
    # window_spectrum_crit_std = db.Column("window_spectrum_crit_std", db.Float)
    # # quantile reject criterion
    # # used if window_spectrum_reject_method=quantile
    # window_spectrum_crit_quantile = db.Column("window_spectrum_crit_quantile", db.Float)
    # # number of adjacent spectral windows to take for computing statistics and defining reject criteria
    # window_spectrum_num_for_stat = db.Column("window_spectrum_num_for_stat", db.Integer)
    # # value in seconds set so that
    # # sequence=25*window_spectrum
    # sequence_length_sec = db.Column("sequence_length_sec", db.Float)
    # sequence_overlap = db.Column("sequence_overlap", db.Float)
    # # Method for rejecting the noisiest sequencies. The criterion is defined on the energy of the sequencies within the frequency band specified by f_min_reject_crit and f_max_reject_crit. The sequence population on which the criterion is applied is defined by sequence_num_for_stats.
    # # possible values:
    # # - 'mean-std' : reject sequences with energy above mean+(this)*std over the population
    # # - 'quantile': reject reject sequences with energy above quantile(this) over the population
    # sequence_reject_method = db.Column("sequence_reject_method", db.String(50))
    # # analog to window_spectrum_crit_std for sequences
    # sequence_crit_std = db.Column("sequence_crit_std", db.Float)
    # # analog to window_spectrum_crit_quantile for sequences
    # sequence_crit_quantile = db.Column("sequence_crit_quantile", db.Float)
    # # number of adjacent sequencies to take for computing statistics and defining reject criteria
    # sequence_num_for_stat = db.Column("sequence_num_for_stat", db.Integer)
    # # which proportion of the spectral estimation windows within a sequence are allowed to be rejected ? (if more that this, the whole sequence is rejected)
    # sequence_reject_tolerance_on_window_spectrum = db.Column(
    #     "sequence_reject_tolerance_on_window_spectrum", db.Float
    # )
    # f_min_reject_crit = db.Column("f_min_reject_crit", db.Float)
    # f_max_reject_crit = db.Column("f_max_reject_crit", db.Float)
    # # filter to be used for all filtering operations when time domain output needed
    # # 'fourier_cosine_taper' or 'butterworth'
    # filter_type = db.Column("filter_type", db.String(50))
    # # taper to be used in time domain before fft
    # # 'cosine'
    # # 'hanning'
    # #
    # taper_time_type = db.Column("taper_time_type", db.String(50))
    # # width of the  transition region in cosine taper
    # # relative to the maximum period of processing
    # taper_time_width_periods = db.Column("taper_time_width_periods", db.Float)
    # # minimum number of time samples in the transition region of cosine taper
    # taper_time_width_min_samples = db.Column("taper_time_width_min_samples", db.Integer)
    # # maximum length of the transition region in the cosine taper as ratio over the whole length of the processed time series
    # taper_time_width_max_proportion = db.Column(
    #     "taper_time_width_max_proportion", db.Float
    # )
    # # taper to be used in freq domain before ifft
    # # 'cosine'
    # # 'hanning'
    # taper_freq_type = db.Column("taper_freq_type", db.String(50))
    # # width of the transition region in cosine taper
    # # relative to the width of the queried frequency band
    # taper_freq_width_proportion = db.Column("taper_freq_width_proportion", db.Float)
    # # analog to taper_time_width_min_samples
    # taper_freq_width_min_samples = db.Column("taper_freq_width_min_samples", db.Integer)
    # # maximum width of the  transition region in cosine taper
    # # relative to the minimum frequency of processing
    # taper_freq_width_max_freqs = db.Column("taper_freq_width_max_freqs", db.Float)


ParamsLike = Union[
    PPSDParams,
    BeamformingParams,
]

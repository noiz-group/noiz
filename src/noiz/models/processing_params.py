from functools import cached_property
import numpy as np
import numpy.typing as npt
from pydantic.dataclasses import dataclass
from typing import Optional, Union, Tuple, TYPE_CHECKING

from noiz.database import db, NotNullColumn
from noiz.globals import ExtendedEnum
from noiz.validation_helpers import validate_exactly_one_argument_provided

if TYPE_CHECKING:
    # Use this to make hybrid_property's have the same typing as a normal property until stubs are improved.
    typed_hybrid_property = property
else:
    from sqlalchemy.ext.hybrid import hybrid_property as typed_hybrid_property


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

    _response_constant_coefficient = db.Column("response_constant_coefficient", db.Float, nullable=True)

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
        self._response_constant_coefficient = kwargs.get("response_constant_coefficient", 0.)

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
            datachunk_params_response_constant_coefficient=self.response_constant_coefficient,
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
    def response_constant_coefficient(self) -> float:
        """
        Constant response coefficient, used as a flag to either launch
        trace.remove_sensitivity() or as a multiplicative coefficient to
        correct data amplitude.

        :return:
        :rtype: float
        """
        return self._response_constant_coefficient

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
    response_constant_coefficient: Optional[float] = None

    def __post_init__(self):
        if self.response_constant_coefficient is not None:
            self.remove_response = False


@dataclass
class ProcessedDatachunkParamsHolder:
    """
        This simple dataclass is just helping to validate :py:class:`~noiz.models.ProcessedDatachunkParams`
        values loaded from the TOML file
    """
    datachunk_params_id: int
    qcone_config_id: Optional[int]
    filtering_low : float
    filtering_high : float
    filtering_order : int
    waterlevel_ratio_to_max : float
    convolution_sliding_window_min_samples : int
    convolution_sliding_window_max_ratio_to_fmin : float
    convolution_sliding_window_ratio_to_bandwidth : float
    quefrency_filter_lowpass_pct : float
    quefrency_filter_taper_min_samples : int
    quefrency_filter_taper_length_ratio_to_length_cepstrum : float
    spectral_whitening: bool
    one_bit: bool
    quefrency: bool


class ProcessedDatachunkParams(db.Model):
    __tablename__ = "processed_datachunk_params"

    id = db.Column("id", db.Integer, primary_key=True)
    datachunk_params_id = db.Column(
        "datachunk_params_id", db.Integer, db.ForeignKey("datachunk_params.id"), nullable=False
    )
    qcone_config_id = db.Column("qcone_config_id", db.Integer, db.ForeignKey("qcone_config.id"), nullable=False)

    _filtering_low = db.Column("filtering_low", db.Float, nullable=False)
    _filtering_high = db.Column("filtering_high", db.Float, nullable=False)
    _filtering_order = db.Column("filtering_order", db.Integer, nullable=False)
    _waterlevel_ratio_to_max = db.Column("waterlevel_ratio_to_max", db.Float, default=0.001, nullable=False)

    _convolution_sliding_window_min_samples = db.Column("convolution_sliding_window_min_samples", db.Integer, default=10,  nullable=False)
    _convolution_sliding_window_max_ratio_to_fmin = db.Column("convolution_sliding_window_max_ratio_to_fmin", db.Float, default=0.5, nullable=False)
    _convolution_sliding_window_ratio_to_bandwidth = db.Column("convolution_sliding_window_ratio_to_bandwidth", db.Float, default=0.15, nullable=False)
    _quefrency_filter_lowpass_pct = db.Column("quefrency_filter_lowpass_pct", db.Float, default=0.5, nullable=False)
    _quefrency_filter_taper_min_samples = db.Column("quefrency_filter_taper_min_samples", db.Integer, default=10,  nullable=False)
    _quefrency_filter_taper_length_ratio_to_length_cepstrum = db.Column("quefrency_filter_taper_length_ratio_to_length_cepstrum", db.Float, default=0.01, nullable=False)

    _spectral_whitening = db.Column("spectral_whitening", db.Boolean, default=True, nullable=False)
    _one_bit = db.Column("one_bit", db.Boolean, default=True, nullable=False)
    _quefrency = db.Column("quefrency", db.Boolean, default=True, nullable=False)

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
        self._filtering_low = kwargs.get("filtering_low")
        self._filtering_high = kwargs.get("filtering_high")
        self._filtering_order = kwargs.get("filtering_order")
        self._waterlevel_ratio_to_max = kwargs.get("waterlevel_ratio_to_max", 0.001)
        self._convolution_sliding_window_min_samples = kwargs.get("convolution_sliding_window_min_samples", 10)
        self._convolution_sliding_window_max_ratio_to_fmin = kwargs.get("convolution_sliding_window_max_ratio_to_fmin ", 0.5)
        self._convolution_sliding_window_ratio_to_bandwidth = kwargs.get("convolution_sliding_window_ratio_to_bandwidth", 0.15)
        self._quefrency_filter_lowpass_pct = kwargs.get("quefrency_filter_lowpass_pct", 0.5)
        self._quefrency_filter_taper_min_samples = kwargs.get("quefrency_filter_taper_min_samples", 10)
        self._quefrency_filter_taper_length_ratio_to_length_cepstrum = kwargs.get("quefrency_filter_taper_length_ratio_to_length_cepstrum", 0.01)
        self._spectral_whitening = kwargs.get("spectral_whitening", True)
        self._one_bit = kwargs.get("one_bit", True)
        self._quefrency = kwargs.get("quefrency", True)

    def as_dict(self):
        return dict(
            processeddatachunk_params_id=self.id,
            processeddatachunk_params_datachunk_params_id=self.datachunk_params_id,
            processeddatachunk_params_qcone_config_id=self.qcone_config_id,
            processeddatachunk_params_filtering_low=self.filtering_low,
            processeddatachunk_params_filtering_high=self.filtering_high,
            processeddatachunk_params_filtering_order=self.filtering_order,
            processeddatachunk_params_waterlevel_ratio_to_max=self.waterlevel_ratio_to_max,
            processeddatachunk_params_convolution_sliding_window_min_samples=self.convolution_sliding_window_min_samples,
            processeddatachunk_params_convolution_sliding_window_max_ratio_to_fmin=self.convolution_sliding_window_max_ratio_to_fmin,
            processeddatachunk_params_convolution_sliding_window_ratio_to_bandwidth=self.convolution_sliding_window_ratio_to_bandwidth,
            processeddatachunk_params_quefrency_filter_lowpass_pct=self.quefrency_filter_lowpass_pct,
            processeddatachunk_params_quefrency_filter_taper_min_samples=self.quefrency_filter_taper_min_samples,
            processeddatachunk_params_quefrency_filter_taper_length_ratio_to_length_cepstrum=self.quefrency_filter_taper_length_ratio_to_length_cepstrum,
            processeddatachunk_params_spectral_whitening=self.spectral_whitening,
            processeddatachunk_params_one_bit=self.one_bit,
            processeddatachunk_params_quefrency=self.quefrency,
        )

    @property
    def filtering_low(self):
        return self._filtering_low

    @property
    def filtering_high(self):
        return self._filtering_high

    @property
    def filtering_order(self):
        return self._filtering_order

    @property
    def waterlevel_ratio_to_max(self):
        return self._waterlevel_ratio_to_max

    @property
    def convolution_sliding_window_min_samples(self):
        return self._convolution_sliding_window_min_samples

    @property
    def convolution_sliding_window_max_ratio_to_fmin(self):
        return self._convolution_sliding_window_max_ratio_to_fmin

    @property
    def convolution_sliding_window_ratio_to_bandwidth(self):
        return self._convolution_sliding_window_ratio_to_bandwidth

    @property
    def quefrency_filter_lowpass_pct(self):
        return self._quefrency_filter_lowpass_pct

    @property
    def quefrency_filter_taper_min_samples(self):
        return self._quefrency_filter_taper_min_samples

    @property
    def quefrency_filter_taper_length_ratio_to_length_cepstrum(self):
        return self._quefrency_filter_taper_length_ratio_to_length_cepstrum

    @property
    def spectral_whitening(self):
        return self._spectral_whitening

    @property
    def one_bit(self):
        return self._one_bit

    @property
    def quefrency(self):
        return self._quefrency


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
        """filldocs"""
        return dict(
            crosscorrelation_params_id=self.id,
            crosscorrelation_params_processed_datachunk_params_id=self.processed_datachunk_params_id,
            crosscorrelation_params_sampling_rate=self.sampling_rate,
            crosscorrelation_params_correlation_max_lag=self.correlation_max_lag,
        )

    @property
    def sampling_rate(self):
        """filldocs"""
        return self._sampling_rate

    @property
    def correlation_max_lag(self):
        """filldocs"""
        return self._correlation_max_lag

    @cached_property
    def correlation_max_lag_samples(self) -> int:
        """filldocs"""
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
    window_length_minimum_periods: Optional[Union[int, float]] = None
    window_length: Optional[Union[int, float]] = None
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

    @typed_hybrid_property
    def central_freq(self):
        return (self.min_freq + self.max_freq)/2

    @property
    def method(self) -> int:
        """filldocs"""
        if self._method == "beamforming":
            return 0
        elif self._method == "capon":
            return 1
        else:
            raise ValueError("This should not have happened.")

    @property
    def window_fraction(self) -> float:
        """filldocs"""
        return self.window_length/self.window_step

    @property
    def used_component_codes(self) -> Tuple[str, ...]:
        """filldocs"""
        return tuple(self._used_component_codes.split(';'))

    def get_xaxis(self):  # -> npt.ArrayLike:
        """filldocs"""
        return np.arange(
            start=self.slowness_x_min,
            stop=self.slowness_x_max+self.slowness_step/2,
            step=self.slowness_step
        )

    def get_yaxis(self):  # -> npt.ArrayLike:
        """filldocs"""
        return np.arange(
            start=self.slowness_y_min,
            stop=self.slowness_y_max+self.slowness_step/2,
            step=self.slowness_step
        )

    @property
    def save_abspow(self) -> bool:
        """filldocs"""
        return any([
            self.save_average_beamformer_abspower,
            self.save_all_beamformers_abspower,
            self.extract_peaks_average_beamformer_abspower,
            self.extract_peaks_all_beamformers_abspower,

        ])

    @property
    def save_relpow(self) -> bool:
        """filldocs"""
        return any([
            self.save_average_beamformer_relpower,
            self.save_all_beamformers_relpower,
            self.extract_peaks_average_beamformer_relpower,
            self.extract_peaks_all_beamformers_relpower,

        ])

    @property
    def max_slowness(self):
        """
        Returns maximum possible value of slowness for provided values

        :return: Maximum possible slowness
        :rtype: float
        """
        return np.round(
            np.sqrt(max(abs(self.slowness_x_max), abs(self.slowness_x_min)) ** 2 +
                    max(abs(self.slowness_y_max), abs(self.slowness_y_min)) ** 2),
            2
        )


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
    resample: bool = False
    resampled_frequency_start: Optional[float] = None
    resampled_frequency_stop: Optional[float] = None
    resampled_frequency_step: Optional[float] = None
    rejected_windows_quantile: float = 0.1
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
    resample = db.Column("resample", db.Boolean, nullable=False)
    resampled_frequency_start = db.Column("resampled_frequency_start", db.Float, nullable=True)
    resampled_frequency_stop = db.Column("resampled_frequency_stop", db.Float, nullable=True)
    resampled_frequency_step = db.Column("resampled_frequency_step", db.Float, nullable=True)
    rejected_windows_quantile = db.Column("rejected_windows_quantile", db.Float, nullable=False)
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
            resample: bool,
            resampled_frequency_start: Optional[float],
            resampled_frequency_stop: Optional[float],
            resampled_frequency_step: Optional[float],
            rejected_windows_quantile: float,
            save_all_windows: bool,
            save_compressed: bool,
    ):
        if resample and any([
            resampled_frequency_start is None,
            resampled_frequency_stop is None,
            resampled_frequency_step is None,
        ]):
            raise ValueError("If you want to resample you have to provide all resampled_* params")

        if resampled_frequency_start is not None and resampled_frequency_start < freq_min:
            raise ValueError("Frequency vector to which you want to resample cannot start before the accepted "
                             "frequencies. It has to be resample_frequency_start >= freq_min")

        if resampled_frequency_stop is not None and resampled_frequency_stop > freq_max:
            raise ValueError("Frequency vector to which you want to resample cannot end after the accepted "
                             "frequencies. It has to be: resampled_frequency_stop <= freq_max")

        self.datachunk_params_id = datachunk_params_id
        self.segment_length = segment_length
        self.segment_step = segment_step
        self.freq_min = freq_min
        self.freq_max = freq_max

        self.resample = resample
        self.resampled_frequency_start = resampled_frequency_start
        self.resampled_frequency_stop = resampled_frequency_stop
        self.resampled_frequency_step = resampled_frequency_step

        self.rejected_windows_quantile = rejected_windows_quantile
        self.save_all_windows = save_all_windows
        self.save_compressed = save_compressed

        self.sampling_rate = float(sampling_rate)

    @property
    def expected_signal_sample_count(self):
        return int(self.sampling_rate * self.segment_length)

    @property
    def signal_sample_spacing(self):
        return 1/self.sampling_rate

    @cached_property
    def _all_fft_freqs(self):
        from scipy.fft import fftfreq
        return fftfreq(n=self.expected_signal_sample_count, d=self.signal_sample_spacing)

    @cached_property
    def _where_accepted_freqs(self):
        freqs = self._all_fft_freqs
        return np.where((freqs >= self.freq_min) & (freqs <= self.freq_max))[0]

    @cached_property
    def expected_fft_freq(self):
        return self._all_fft_freqs[self._where_accepted_freqs]

    @cached_property
    def resampled_frequency_vector(self):
        return np.arange(
            start=self.resampled_frequency_start,
            stop=self.resampled_frequency_stop,
            step=self.resampled_frequency_step,
        )

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


@dataclass
class EventDetectionParamsHolder:
    """
        This simple dataclass is just helping to validate :py:class:`~noiz.models.EventDetectionParams`
        values loaded from the TOML file
    """
    minimum_frequency: float
    maximum_frequency: float
    output_margin_length_sec: float
    datachunk_params_id: int
    detection_type: str
    trace_trimming_sec: Optional[float] = None
    n_short_time_average: Optional[int] = None
    n_long_time_average: Optional[int] = None
    trigger_value: Optional[float] = None
    detrigger_value: Optional[float] = None
    peak_ground_velocity_threshold: Optional[float] = None

    def __post_init__(self):
        if self.detection_type == "sta_lta":
            if None in (
                    self.n_short_time_average,
                    self.n_long_time_average,
                    self.trigger_value,
                    self.detrigger_value,
                    ):
                raise ValueError("EventDetectionParams is invalid:"
                                 "At least one parameter required for a StaLta detection is missing."
                                 "n_short_time_average, n_long_time_average, trigger_value"
                                 "and detrigger_value are all required.")
        elif self.detection_type == "amplitude_spike":
            if self.peak_ground_velocity_threshold is None:
                raise ValueError("EventDetectionParams is invalid:"
                                 "peak_ground_velocity_threshold is required for an AmplitudeSpike detection.")
        else:
            raise ValueError("EventDetectionParams is invalid:"
                             "detection_type is neither 'sta_lta' nor 'amplitude_spike'.")


class EventDetectionParams(db.Model):
    __tablename__ = "event_detection_params"

    id = db.Column("id", db.Integer, primary_key=True)

    datachunk_params_id = db.Column(
        "datachunk_params_id", db.Integer, db.ForeignKey("datachunk_params.id"), nullable=False
    )

    _detection_type = db.Column("detection_type", db.UnicodeText, nullable=False)
    _trace_trimming_sec = db.Column("trace_trimming_sec", db.Float, nullable=True)

    _n_short_time_average = db.Column("n_short_time_average", db.Integer, nullable=True)
    _n_long_time_average = db.Column("n_long_time_average", db.Integer, nullable=True)
    _trigger_value = db.Column("trigger_value", db.Float, nullable=True)
    _detrigger_value = db.Column("detrigger_value", db.Float, nullable=True)

    _peak_ground_velocity_threshold = db.Column("peak_ground_velocity_threshold", db.Float, nullable=True)

    _minimum_frequency = db.Column("minimum_frequency", db.Float, nullable=False)
    _maximum_frequency = db.Column("maximum_frequency", db.Float, nullable=False)
    _output_margin_length_sec = db.Column("output_margin_length_sec", db.Float, default=5, nullable=False)

    datachunk_params = db.relationship(
        "DatachunkParams",
        foreign_keys=[datachunk_params_id],
        lazy="joined",
    )

    def __init__(self, **kwargs):
        self.datachunk_params_id = kwargs.get("datachunk_params_id")
        self._detection_type = kwargs.get("detection_type")
        self._n_short_time_average = kwargs.get("n_short_time_average")
        self._n_long_time_average = kwargs.get("n_long_time_average")
        self._trigger_value = kwargs.get("trigger_value")
        self._detrigger_value = kwargs.get("detrigger_value")
        self._peak_ground_velocity_threshold = kwargs.get("peak_ground_velocity_threshold")
        self._minimum_frequency = kwargs.get("minimum_frequency")
        self._maximum_frequency = kwargs.get("maximum_frequency")
        self._output_margin_length_sec = kwargs.get("output_margin_length_sec")
        self._trace_trimming_sec = kwargs.get("trace_trimming_sec")

    def as_dict(self):
        return dict(
            event_detection_params_id=self.id,
            event_detection_params_datachunk_params_id=self.datachunk_params_id,
            event_detection_params_detection_type=self.detection_type,
            event_detection_params_n_short_time_average=self.n_short_time_average,
            event_detection_params_n_long_time_average=self.n_long_time_average,
            event_detection_params_trigger_value=self.trigger_value,
            event_detection_params_detrigger_value=self.detrigger_value,
            event_detection_params_peak_ground_velocity_threshold=self.peak_ground_velocity_threshold,
            event_detection_params_minimum_frequency=self.minimum_frequency,
            event_detection_params_maximum_frequency=self.maximum_frequency,
            event_detection_params_output_margin_length_sec=self.output_margin_length_sec,
            event_detection_params_trace_trimming_sec=self.trace_trimming_sec,
        )

    @property
    def detection_type(self):
        return self._detection_type

    @property
    def n_short_time_average(self):
        return self._n_short_time_average

    @property
    def n_long_time_average(self):
        return self._n_long_time_average

    @property
    def trigger_value(self):
        return self._trigger_value

    @property
    def detrigger_value(self):
        return self._detrigger_value

    @property
    def peak_ground_velocity_threshold(self):
        return self._peak_ground_velocity_threshold

    @property
    def minimum_frequency(self):
        return self._minimum_frequency

    @property
    def maximum_frequency(self):
        return self._maximum_frequency

    @property
    def output_margin_length_sec(self):
        return self._output_margin_length_sec

    @property
    def trace_trimming_sec(self):
        return self._trace_trimming_sec


@dataclass
class EventConfirmationParamsHolder:
    """
        This simple dataclass is just helping to validate :py:class:`~noiz.models.EventConfirmationParams`
        values loaded from the TOML file
    """
    datachunk_params_id: int
    event_detection_params_id: int
    time_lag: float
    sampling_step: float
    vote_threshold: int
    vote_weight: Optional[Tuple[str, ...]] = None


class EventConfirmationParams(db.Model):
    __tablename__ = "event_confirmation_params"

    id = db.Column("id", db.Integer, primary_key=True)

    datachunk_params_id = db.Column(
        "datachunk_params_id", db.Integer, db.ForeignKey("datachunk_params.id"), nullable=False
    )
    event_detection_params_id = db.Column(
        "event_detection_params_id", db.Integer, db.ForeignKey("event_detection_params.id"), nullable=False
    )
    _time_lag = db.Column("time_lag", db.Float, nullable=False)
    _vote_threshold = db.Column("vote_threshold", db.Integer, nullable=False)
    _sampling_step = db.Column("sampling_step", db.Float, nullable=False)
    _vote_weight = db.Column("vote_weight", db.String, nullable=True)

    datachunk_params = db.relationship(
        "DatachunkParams",
        foreign_keys=[datachunk_params_id],
        lazy="joined",
    )
    event_detection_params = db.relationship(
        "EventDetectionParams",
        foreign_keys=[event_detection_params_id],
        lazy="joined",
    )

    def __init__(self, **kwargs):
        self.datachunk_params_id = kwargs.get("datachunk_params_id")
        self.event_detection_params_id = kwargs.get("event_detection_params_id")
        self._time_lag = kwargs.get("time_lag")
        self._vote_threshold = kwargs.get("vote_threshold")
        self._sampling_step = kwargs.get("sampling_step")
        if kwargs.get("vote_weight") is not None:
            self._vote_weight = ";".join(kwargs.get("vote_weight"))  # type: ignore

    def as_dict(self):
        return dict(
            event_confirmation_params_id=self.id,
            event_confirmation_params_datachunk_params_id=self.datachunk_params_id,
            event_confirmation_params_event_detection_params_id=self.event_detection_params_id,
            event_confirmation_params_time_lag=self.time_lag,
            event_confirmation_params_vote_threshold=self.vote_threshold,
            event_confirmation_params_sampling_step=self.sampling_step,
            event_confirmation_params_vote_weight=self.vote_weight,
        )

    @property
    def time_lag(self):
        return self._time_lag

    @property
    def sampling_step(self):
        return self._sampling_step

    @property
    def vote_threshold(self):
        return self._vote_threshold

    @property
    def vote_weight(self):
        """filldocs"""
        if self._vote_weight is not None:
            return tuple(self._vote_weight.split(';'))
        return None

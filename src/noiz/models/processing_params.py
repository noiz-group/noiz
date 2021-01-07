from pydantic.dataclasses import dataclass
import datetime
import numpy as np

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
    _prefiltering_low = db.Column(
        "prefiltering_low", db.Float, default=0.01, nullable=False
    )
    _prefiltering_high = db.Column(
        "prefiltering_high", db.Float, default=12.0, nullable=False
    )
    _prefiltering_order = db.Column(
        "prefiltering_order", db.Integer, default=4, nullable=False
    )
    _preprocessing_taper_type = db.Column(
        "preprocessing_taper_type", db.UnicodeText, default="cosine", nullable=False
    )
    _preprocessing_taper_side = db.Column(
        "preprocessing_taper_side", db.UnicodeText, default="both", nullable=False
    )
    _preprocessing_taper_max_length = db.Column(
        "preprocessing_taper_max_length", db.Float, nullable=False
    )
    _preprocessing_taper_max_percentage = db.Column(
        "preprocessing_taper_max_percentage", db.Float, nullable=False
    )
    _remove_response = db.Column(
        "remove_response", db.Boolean, default=True, nullable=False
    )
    _spectral_whitening = db.Column(
        "spectral_whitening", db.Boolean, default=True, nullable=False
    )
    _one_bit = db.Column("one_bit", db.Boolean, default=True, nullable=False)
    _datachunk_sample_tolerance = db.Column(
        "datachunk_sample_threshold", db.Float, default=0.98, nullable=False
    )

    _correlation_max_lag = db.Column("correlation_max_lag", db.Float, nullable=True)

    _max_gap_for_merging = db.Column("max_gap_for_merging", db.Integer, default=10, nullable=False)

    _padding_method = db.Column(
        "padding_method", db.UnicodeText, default="padding_with_tapering", nullable=False
    )
    _padding_taper_type = db.Column("padding_taper_type", db.UnicodeText, nullable=True)
    _padding_taper_max_length = db.Column("padding_taper_max_length", db.Float, nullable=True)
    _padding_taper_max_percentage = db.Column("padding_taper_max_percentage", db.Float, nullable=True)

    # TODO add a relationship field with all datachunks

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

        self._padding_taper_type = kwargs.get("padding_taper_type", "cosine")
        self._padding_taper_max_length = kwargs.get("padding_taper_max_length", 5)  # seconds
        self._padding_taper_max_percentage = kwargs.get("padding_taper_max_percentage", 0.1)  # percent

        self._correlation_max_lag = kwargs.get("correlation_max_lag", 60)  # deprecatethis
        self._max_gap_for_merging = kwargs.get("max_gap_for_merging", 10)  # deprecatethis

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

    def get_correlation_time_vector(self) -> np.array:
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
    spectral_whitening: bool
    one_bit_normalization: bool


class BeamformingParams(db.Model):
    __tablename__ = "beamforming_params"
    id = db.Column("id", db.Integer, primary_key=True)

    min_freq = db.Column("min_freq", db.Float, nullable=False)
    max_freq = db.Column("max_freq", db.Float, nullable=False)
    slowness_limit = db.Column("slowness_limit", db.Float, nullable=False)
    slowness_step = db.Column("slowness_step", db.Float, nullable=False)
    window_length = db.Column("window_length", db.Float, nullable=False)
    window_step = db.Column("window_step", db.Float, nullable=False)
    window_length = db.Column("window_length", db.Float, nullable=False)

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

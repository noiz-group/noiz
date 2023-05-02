# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from typing import Tuple, Optional

from loguru import logger
import obspy
import numpy as np

from noiz.models.type_aliases import ProcessDatachunksInputs
from noiz.models.datachunk import Datachunk, ProcessedDatachunk, ProcessedDatachunkFile, DatachunkFile
from noiz.models.processing_params import ProcessedDatachunkParams, DatachunkParams
from noiz.models.timespan import Timespan
from noiz.models.component import Component
from noiz.processing.path_helpers import parent_directory_exists_or_create, assembly_filepath, assembly_preprocessing_filename, \
    assembly_sds_like_dir, increment_filename_counter
from noiz.globals import PROCESSED_DATA_DIR


def whiten_trace(tr: obspy.Trace,
                 f_max: float,
                 waterlevel_ratio_to_max: float,
                 filtering_low: float,
                 filtering_high: float,
                 convolution_sliding_window_min_samples: int,
                 convolution_sliding_window_max_ratio_to_fmin: float,
                 convolution_sliding_window_ratio_to_bandwidth: float,
                 quefrency_filter_lowpass_pct: float,
                 quefrency_filter_taper_min_samples: int,
                 quefrency_filter_taper_length_ratio_to_length_cepstrum: float,
                 quefrency: bool) -> obspy.Trace:
    """
    Spectrally whitens the trace. Calculates a spectrum of trace,
    divides it by its absolute value and
    inverts that spectrum back to time domain with a type conversion to real.

    :param tr: trace to be whitened
    :type tr: obspy.Trace
    :param f_max: sample rate Hz
    :type f_max: float
    :param waterlevel_ratio_to_max: Processing parameters: value to apply for waterlevel filter
    :type waterlevel_ratio_to_max: float
    :param filtering_low: Processing parameters: low filter
    :type filtering_low: float
    :param filtering_high: Processing parameters : high filter
    :type filtering_high: float
    :param convolution_sliding_window_min_samples: Processing parameters : minimum of samples for the sliding windows used for defining the convolution kernel
    :type convolution_sliding_window_min_samples: int
    :param convolution_sliding_window_max_ratio_to_fmin: Processing parameters : maximum percentage of width for the sliding windows :  do not used more than half of filtering_low
    :type convolution_sliding_window_max_ratio_to_fmin: float
    :param convolution_sliding_window_ratio_to_bandwidth: Processing parameters : pass band percentage; used for defining the convolution kernel
    :type convolution_sliding_window_ratio_to_bandwidth: float
    :param quefrency_filter_lowpass_pct: Processing parameters : double fft: cut the highest frequencies
    :type quefrency_filter_lowpass_pct: float
    :param quefrency_filter_taper_min_samples: Processing parameters :min signal for the taper
    :type quefrency_filter_taper_min_samples: int
    :param quefrency_filter_taper_length_ratio_to_length_cepstrum: Processing parameters : percentage max signal for taper
    :type quefrency_filter_taper_length_ratio_to_length_cepstrum: float
    :param quefrency: Processing parameters: using or not quefreq method
    :type quefrency: boolean
    :return: Spectrally whitened trace
    :rtype: obspy.Trace
    """

    f_niquist = f_max / 2  # to spectral domain
    convolution_sliding_window_max_width = filtering_low*convolution_sliding_window_max_ratio_to_fmin  # do not used more than half of filtering_low
    width_band_pass = filtering_high - filtering_low

    tr_tap = tr.copy()
    tr_tap = tr_tap.taper(0.5, max_length=1/filtering_low)
    data_use = tr_tap.data
    spectrum = np.fft.fft(data_use)

    s_waterlevel = _waterlevel_f(spectrum, waterlevel_ratio_to_max)

    if quefrency:
        s_waterlevel_log = np.log(s_waterlevel)
        min_waterlevel_log = np.min(s_waterlevel_log)
        s_waterlevel_log_shifted = s_waterlevel_log - min_waterlevel_log

        l_conv, smooth_vector = _convolution_kernel_def(convolution_sliding_window_ratio_to_bandwidth,
                                                        width_band_pass,
                                                        convolution_sliding_window_max_width,
                                                        convolution_sliding_window_min_samples,
                                                        f_niquist,
                                                        len(spectrum))  # definition convolution filter

        s_log_new = s_waterlevel_log_shifted.copy()  # convolution application
        s_log_new_conv = (1 / np.sum(smooth_vector)) * np.convolve(s_waterlevel_log_shifted[1:], smooth_vector, mode="same")
        s_log_new_conv_sym = 0.5*(s_log_new_conv + np.flip(s_log_new_conv))
        s_log_new[1:] = s_log_new_conv_sym
        spectrum_fft = np.fft.fft(s_log_new)
        spectrum_fft_shift = np.fft.fftshift(spectrum_fft)

        n = round(len(spectrum)*quefrency_filter_lowpass_pct/2)  # to spectral domain
        taper_qfr = _taper_quefrency(len(s_log_new), n, quefrency_filter_taper_min_samples, quefrency_filter_taper_length_ratio_to_length_cepstrum)
        spectrum_fft_shift_tap = spectrum_fft_shift*taper_qfr  # application du taper
        spectrum_fft_tap = np.fft.ifftshift(spectrum_fft_shift_tap)
        smooth_s = np.fft.ifft(spectrum_fft_tap)
        smooth_s_real = np.real(smooth_s)
        taper_to_td = _taper_to_timedomaine(len(smooth_s_real), filtering_low, filtering_high, f_niquist, l_conv)
        smooth_s_exp_conv = np.exp(s_log_new + min_waterlevel_log)
        psd_white = (spectrum/smooth_s_exp_conv)*taper_to_td
        s_white = np.fft.ifft(psd_white)
        tr.data = np.real(s_white)

    else:
        psd_white = (spectrum/np.abs(s_waterlevel))
        s_white = np.fft.ifft(psd_white)
        tr.data = np.real(s_white)

    return tr


def one_bit_normalization(tr: obspy.Trace) -> obspy.Trace:
    """
    One-bit amplitude normalization. Uses numpy.sign

    :param tr: Trace object to be normalized
    :type tr: obspy.Trace
    :return: Normalized Trace
    :rtype: obspy.Trace
    """

    tr.data = np.sign(tr.data)

    return tr


def process_datachunk_wrapper(
        inputs: ProcessDatachunksInputs,
) -> Tuple[ProcessedDatachunk, ...]:
    """
    Thin wrapper around :py:meth:`noiz.processing.datachunk_processing.process_datachunk` that converts a single
    TypedDict of input to standard keyword arguments. It also converts a single output to tuple so the upsertion method
    is able to properly process it.

    :param inputs: TypedDict with all required inputs
    :type inputs: noiz.api.type_aliases.ProcessDatachunksInputs
    :return: Tuple with processing result
    :rtype: Tuple[noiz.models.datachunk.ProcessedDatachunk, ...]
    """

    return (
        process_datachunk(
            datachunk=inputs["datachunk"],
            params=inputs["params"],
            datachunk_file=inputs["datachunk_file"],
        ),
    )


def process_datachunk(
        datachunk: Datachunk,
        params: ProcessedDatachunkParams,
        datachunk_file: Optional[DatachunkFile] = None,
) -> ProcessedDatachunk:
    """
    Method that allows for processing of the datachunks.
    It can perform spectral whitening in full spectrum as well as one bit normalization.

    :param datachunk: Datachunk to be processed
    :type datachunk: ~noiz.models.datachunk.Datachunk
    :param params: Processing parameters
    :type params: ~noiz.models.processing_params.ProcessedDatachunkParams
    :param datachunk_file: Optional DatachunkFile to be have data loaded from
    :type datachunk_file: Optional[~noiz.models.datachunk.DatachunkFile]
    :return:
    :rtype: noiz.models.datachunk.ProcessedDatachunk
    """

    if not isinstance(datachunk.timespan, Timespan):
        msg = 'The Timespan is not loaded with the Datachunk. Correct that.'
        logger.error('The Timespan is not loaded with the Datachunk. Correct that.')
        raise ValueError(msg)
    if not isinstance(datachunk.component, Component):
        msg = 'The Component is not loaded with the Datachunk. Correct that.'
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Starting processing of {datachunk}")

    logger.debug("Loading data")
    st = datachunk.load_data(datachunk_file=datachunk_file)

    if len(st) != 1:
        msg = f"There are more than one trace in stream in {datachunk}"
        logger.error(msg)
        raise ValueError(msg)

    if params.spectral_whitening:
        logger.debug("Performing spectral whitening")
        st[0] = whiten_trace(
                            st[0],
                            st[0].stats.sampling_rate,
                            params.waterlevel_ratio_to_max,
                            params.filtering_low,
                            params.filtering_high,
                            params.convolution_sliding_window_min_samples,
                            params.convolution_sliding_window_max_ratio_to_fmin,
                            params.convolution_sliding_window_ratio_to_bandwidth,
                            params.quefrency_filter_lowpass_pct,
                            params.quefrency_filter_taper_min_samples,
                            params.quefrency_filter_taper_length_ratio_to_length_cepstrum,
                            params.quefrency
                )
        logger.debug("Performing bandpass filter")
    st[0].filter(
        type="bandpass",
        freqmin=params.filtering_low,
        freqmax=params.filtering_high,
        corners=params.filtering_order,
    )

    if params.one_bit:
        logger.debug("Performing one bit normalization")
        st[0] = one_bit_normalization(st[0])

    filepath = assembly_filepath(
        PROCESSED_DATA_DIR,  # type: ignore
        "processed_datachunk",
        assembly_sds_like_dir(datachunk.component, datachunk.timespan) \
        .joinpath(assembly_preprocessing_filename(
            component=datachunk.component,
            timespan=datachunk.timespan,
            count=0
        )),
    )

    if filepath.exists():
        logger.debug(f"Filepath {filepath} exists. Trying to find next free one.")
        filepath = increment_filename_counter(filepath=filepath, extension=False)
        logger.debug(f"Free filepath found. Datachunk will be saved to {filepath}")

    logger.info(f"Chunk will be written to {str(filepath)}")
    parent_directory_exists_or_create(filepath)

    proc_datachunk_file = ProcessedDatachunkFile(filepath=str(filepath))

    logger.debug("Trying to write mseed file.")
    st.write(proc_datachunk_file.filepath, format="mseed")
    logger.info("File written succesfully")

    processed_datachunk = ProcessedDatachunk(
        processed_datachunk_params_id=params.id,
        datachunk_id=datachunk.id,
        file=proc_datachunk_file,
    )

    return processed_datachunk


def _taper_function(
        w: int,
        idx: int,
        taper_f: np.ndarray,
        factor: int,
) -> np.ndarray:
    """Fill out taper

    :param w: taper width
    :type w: int
    :param idx: starting taper index
    :type idx: float
    :param taper_f: taper vector to fill out
    :type taper_f: np.ndarray
    :param factor: 1 or -1 depend on starting or ending taper
    :type factor: int
    :return: taper
    :rtype: np.ndarray
    """

    for i_im in range(w):
        val_wid = np.sin((i_im)/(w-1)*np.pi/2)**2
        indice = idx+(-w+i_im+1)*factor
        taper_f[indice] = val_wid
        taper_f[-indice] = val_wid

    return taper_f


def _taper_def(
        spect_len: int,
        ind_b: int,
        ind_e: int,
        value: int,
        init_type: str,
) -> np.ndarray:
    """Definition/initialisation taper

    :param spect_len: spetrum length
    :type spect_len: int
    :param ind_b: indice of taper beggining
    :type ind_b: int
    :param ind_e: indice of taper ending
    :type ind_e: int
    :param value: taper value : 1 or 0
    :type value: int
    :param init_type: zeros or ones
    :type init_type: str
    :return: taper initialized
    :rtype: np.ndarray
    """

    if init_type == "zeros":
        taper_init = np.zeros(spect_len)
    elif init_type == "ones":
        taper_init = np.ones(spect_len)

    taper_init[ind_b:ind_e] = value

    return taper_init


def _taper_quefrency(
        len_s_log_new: int,
        n: int,
        quefrency_filter_taper_min_samples: int,
        quefrency_filter_taper_length_ratio_to_length_cepstrum: float,
) -> np.ndarray:
    """taper definition for quefrency domain

    :param len_s_log_new: signal length
    :type len_s_log_new: int
    :param n: cutting the highest frequencies
    :type n: int
    :param quefrency_filter_taper_min_samples: _description_
    :type quefrency_filter_taper_min_samples: int
    :param quefrency_filter_taper_length_ratio_to_length_cepstrum: _description_
    :type quefrency_filter_taper_length_ratio_to_length_cepstrum: float
    :return: taper for quefrency domain
    :rtype: np.ndarray
    """

    len_signal = len_s_log_new
    width_ff = max(quefrency_filter_taper_min_samples,
                   round(len_signal*quefrency_filter_taper_length_ratio_to_length_cepstrum))
    ind = int(round(len_s_log_new/2)-n)
    taper_qfr = _taper_def(len_signal, ind, len_signal-ind, 1, "zeros")
    taper_qfr = _taper_function(width_ff, ind, taper_qfr, 1)

    return taper_qfr


def _taper_to_timedomaine(
        len_smooth_s: int,
        filtering_low: float,
        filtering_high: float,
        f_niquist: float,
        l_conv: int,
) -> np.ndarray:
    """taper definition/creation for time domaine

    :param len_smooth_s: signal smooth spectrum length
    :type len_smooth_s: int
    :param filtering_low: low value for filtering
    :type filtering_low: float
    :param filtering_high: high value for filtering
    :type filtering_high: float
    :param f_niquist: niquist frequency
    :type f_niquist: float
    :param l_conv: convolution value
    :type l_conv: int
    :return: taper for time domain
    :rtype: np.ndarray
    """

    hz_axis = np.linspace(0, f_niquist, round(len_smooth_s/2))
    i_min = int(np.argmin(np.abs(hz_axis-filtering_low)))
    i_max = int(np.argmin(np.abs(hz_axis-filtering_high)))
    taper_min = _taper_def(len_smooth_s, i_min, len_smooth_s-(i_min), 1, "zeros")
    taper_max = _taper_def(len_smooth_s, i_max, len_smooth_s-(i_max-1), 0, "ones")
    taper_min = _taper_function(l_conv, i_min, taper_min, 1)
    taper_max = _taper_function(l_conv, i_max, taper_max, -1)
    taper_to_td = taper_min*taper_max

    return taper_to_td


def _waterlevel_f(
        spectrum: np.ndarray,
        waterlevel_ratio_to_max: float,
) -> np.ndarray:
    """waterlevel filter

    :param spectrum: signal spectrum
    :type spectrum: np.ndarray
    :param waterlevel_ratio_to_max: value to apply for performing the waterlevel filter
    :type waterlevel_ratio_to_max: float
    :return: spectrum filtered by waterlevel
    :rtype: np.ndarray
    """

    s_waterlevel = spectrum.copy()
    s_waterlevel[np.abs(s_waterlevel) <= waterlevel_ratio_to_max*np.max(np.abs(s_waterlevel))] = waterlevel_ratio_to_max*np.max(np.abs(s_waterlevel))
    s_waterlevel[0] = np.abs(spectrum[0])

    return np.abs(s_waterlevel)


def _convolution_kernel_def(
        convolution_sliding_window_ratio_to_bandwidth: float,
        width_band_pass: float,
        convolution_sliding_window_max_width: float,
        convolution_sliding_window_min_samples: int,
        f_niquist: float,
        len_spectrum: int,
) -> Tuple[int, np.ndarray]:
    """Definition of the convolution kernel

    :param convolution_sliding_window_ratio_to_bandwidth: pass band percentage
    :type convolution_sliding_window_ratio_to_bandwidth: float
    :param width_band_pass: width of the filter frequency (Hz)
    :type width_band_pass: float
    :param convolution_sliding_window_max_width: sliding window width : do not used more than half of filtering_low
    :type convolution_sliding_window_max_width: float
    :param convolution_sliding_window_min_samples: minimum of samples for the sliding windows
    :type convolution_sliding_window_min_samples: int
    :param f_niquist: niquist frequency
    :type f_niquist: float
    :param len_spectrum: spectrum length
    :type len_spectrum: int
    :return: convolution kernel : l_conv and smooth_vector
    :rtype: Tuple[int,np.ndarray]
    """

    width_conv = min(convolution_sliding_window_ratio_to_bandwidth * width_band_pass, convolution_sliding_window_max_width)
    l_conv = round(max(convolution_sliding_window_min_samples, width_conv / f_niquist * len_spectrum))
    smooth_vector = np.ones((l_conv))

    return l_conv, smooth_vector

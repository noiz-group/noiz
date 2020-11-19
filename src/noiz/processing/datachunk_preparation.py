import logging
import numpy as np
import obspy
from typing import Union, Optional, Tuple

from noiz.models.processing_params import DatachunkParams
from noiz.models.timespan import Timespan
from noiz.processing.validation_helpers import count_consecutive_trues, _validate_stream_with_single_trace

log = logging.getLogger("noiz.processing")


def expected_npts(timespan_length: float, sampling_rate: float) -> int:
    """
    Calculates expected number of npts in a trace based on timespan length and sampling rate.
    Casted to int with floor rounding.

    :param timespan_length: Length of timespan in seconds
    :type timespan_length: float
    :param sampling_rate: Sampling rate in samples per second
    :type sampling_rate: float
    :return: Number of samples in the timespan
    :rtype: int
    """
    return int(timespan_length * sampling_rate)


def next_pow_2(number: int) -> int:
    """
    Finds a number that is a power of two that is next after value provided to that method

    :param number: Value of which you need next power of 2
    :type number: int
    :return: Next power of two
    :rtype: int
    """
    return int(np.ceil(np.log2(number)))


def resample_with_padding(
    st: obspy.Stream, sampling_rate: Union[int, float]
) -> obspy.Stream:
    """
    Pads data of trace (assumes that stream has only one trace) with zeros up to next power of two
    and resamples it down to provided sampling rate. In the end it trims it to original starttime and endtime.

    :param st: Stream containing one trace to be resampled
    :type st: obspy.Stream
    :param sampling_rate: Target sampling rate
    :type sampling_rate: Union[int, float]
    :return: Resampled stream
    :rtype: obspy.Stream
    """

    tr = st[0]
    starttime = tr.stats.starttime
    endtime = tr.stats.endtime
    npts = tr.stats.npts

    log.info("Finding sample deficit up to next power of 2")
    deficit = 2 ** next_pow_2(npts) - npts

    log.info("Padding with zeros up to the next power of 2")
    tr.data = np.concatenate((tr.data, np.zeros(deficit)))

    log.info("Resampling")
    tr.resample(sampling_rate)

    log.info(
        f"Slicing data to fit them between starttime {starttime} and endtime {endtime}"
    )
    st = obspy.Stream(tr.slice(starttime=starttime, endtime=endtime))

    log.info("Resampling done!")
    return st


def preprocess_whole_day(
    st: obspy.Stream, preprocessing_config: DatachunkParams
) -> obspy.Stream:
    log.info("Trying to merge traces if more than 1")
    st.merge()

    if len(st) > 1:
        log.error("There are more than one trace in the stream, raising error.")
        raise ValueError(f"There are {len(st)} traces in the stream!")

    log.info(
        f"Resampling stream to {preprocessing_config.sampling_rate} Hz with padding to next power of 2"
    )
    st = resample_with_padding(st=st, sampling_rate=preprocessing_config.sampling_rate)
    log.info(
        f"Filtering with bandpass to "
        f"low: {preprocessing_config.prefiltering_low};"
        f"high: {preprocessing_config.prefiltering_high};"
        f"order: {preprocessing_config.prefiltering_order}"
    )
    st.filter(
        type="bandpass",
        freqmin=preprocessing_config.prefiltering_low,
        freqmax=preprocessing_config.prefiltering_high,
        corners=preprocessing_config.prefiltering_order,
    )
    log.info("Finished processing whole day")
    return st


def merge_traces_fill_zeros(st: obspy.Stream) -> obspy.Stream:
    """
    Merges Traces inside of Stream with use of zeros.

    :param st: stream to be trimmed
    :type st: obspy.Stream
    :return: Trimmed stream
    :rtype: obspy.Stream
    """
    try:
        st.merge(fill_value=0)
    except Exception as e:
        raise ValueError(f"Cannot merge traces. {e}")
    return st


def merge_traces_under_conditions(st: obspy.Stream, params: DatachunkParams) -> obspy.Stream:
    """
    This method first checks if passed stream :class:`obspy.Stream` is mergeable and then tries to merge it into
    single trace.

    :param st: Stream to be merged
    :type st: obspy.Stream
    :param params: Set of DatachunkParams with which datachunk is being prepared
    :type params: DatachunkParams
    :return: Merged stream
    :rtype: obspy.Stream
    """
    try:
        _check_if_gaps_short_enough(st, params)
    except ValueError as e:
        raise ValueError(f"Cannot merge traces. {e}")

    try:
        st.merge(method=1, interpolation_samples=-1, fill_value='interpolate')
    except Exception as e:
        raise ValueError(f"Cannot merge traces. {e}")
    return st


def _check_if_gaps_short_enough(st: obspy.Stream, params: DatachunkParams) -> bool:
    """
    This method takes a stream, tries to merge it with :meth:`obspy.Stream.merge` with params of `method=0`.
    This merging attempt, in case of both overlapping signal or gap, will produce a continuous trace with
    :class:`numpy.MaskedArray`. This allows for easy checking if the gaps and overlaps are longer than maximum
    that is defined in instance of :class:`noiz.models.DatachunkParams` in parameter
    :param:`noiz.models.DatachunkParams.max_gap_for_merging`.

    :param st: Stream to be checked for merging
    :type st: obspy.Stream
    :param params: DatachunkParams that datachunk is being processed with
    :type params: DatachunkParams
    :return: True if stream is okay for merging
    :rtype: bool
    :raises: ValueError
    """
    max_gap = params.max_gap_for_merging
    st_merged = st.copy().merge(method=0)

    if len(st_merged) > 1:
        raise ValueError(f"Cannot marge traces, probably they have different ids. {st}")
    elif len(st_merged) == 0:
        raise ValueError("Cannot marge traces, stream is empty.")

    if not isinstance(st_merged[0].data, np.ma.MaskedArray):
        return True

    gaps_mask: np.array = st_merged[0].data.mask
    gap_counts = count_consecutive_trues(gaps_mask)
    # noinspection PyTypeChecker
    if any(gap_counts > max_gap):
        raise ValueError(f"Some of the gaps or overlaps are longer than set maximum of {max_gap} samples. "
                         f"Found gaps have {gap_counts} samples.")
    else:
        return True


def pad_zeros_to_exact_time_bounds(
        st: obspy.Stream, timespan: Timespan, expected_no_samples: int
) -> obspy.Stream:
    """
    Takes a obspy Stream and trims it with Stream.trim to starttime and endtime of provided Timespan.
    It also verifies if resulting number of samples is as expected.

    :param st: stream to be trimmed
    :type st: obspy.Stream
    :param timespan: Timespan to be used for trimming
    :type timespan: Timespan
    :param expected_no_samples: Expected number of samples to be verified
    :type expected_no_samples: int
    :return: Trimmed stream
    :rtype: obspy.Stream
    :raises ValueError
    """
    st.trim(
        starttime=obspy.UTCDateTime(timespan.starttime),
        endtime=obspy.UTCDateTime(timespan.endtime),
        nearest_sample=False,
        pad=True,
        fill_value=0,
    )

    st = _check_and_remove_extra_samples_on_the_end(st, expected_no_samples)

    if st[0].stats.npts != expected_no_samples:
        raise ValueError(
            f"The try of padding with zeros to {expected_no_samples} was "
            f"not successful. Current length of data is {st[0].stats.npts}. "
        )
    return st


def interpolate_ends_to_zero_to_fit_timespan(
    st: obspy.Stream, timespan: Timespan, expected_no_samples: int
) -> obspy.Stream:
    """
    Takes a obspy Stream and trims it with Stream.trim to starttime and endtime of provided Timespan.
    It also verifies if resulting number of samples is as expected.

    :param st: stream to be trimmed
    :type st: obspy.Stream
    :param timespan: Timespan to be used for trimming
    :type timespan: Timespan
    :param expected_no_samples: Expected number of samples to be verified
    :type expected_no_samples: int
    :return: Trimmed stream
    :rtype: obspy.Stream
    :raises ValueError
    """

    _validate_stream_with_single_trace(st)

    tr_temp = obspy.Trace()
    tr_temp.stats = st[0].stats.copy()
    tr_temp.data = np.array([0], dtype=st[0].data.dtype)

    trace_list = [st[0]]
    if st[0].stats.starttime > timespan.starttime:
        tr_start = tr_temp.copy()
        tr_start.stats.starttime = timespan.starttime
        trace_list.insert(0, tr_start)
    if st[0].stats.endtime < timespan.endtime:
        tr_end = tr_temp.copy()
        tr_end.stats.starttime = timespan.endtime
        trace_list.append(tr_end)

    st_temp = obspy.Stream(traces=trace_list)
    st_temp.merge(method=1, fill_value='interpolate')

    st_res = _check_and_remove_extra_samples_on_the_end(st=st_temp, expected_no_samples=expected_no_samples)

    if st_res[0].stats.npts != expected_no_samples:
        raise ValueError(
            f"The try of padding with zeros to {expected_no_samples} was "
            f"not successful. Current length of data is {st_res[0].stats.npts}. "
        )
    return st_res


def _check_and_remove_extra_samples_on_the_end(st: obspy.Stream, expected_no_samples: int):
    """
    Takes a stream with a single trace and checks if the number of samples is higher than parameter
    ``expected_no_samples``. Usually used to remove the last sample, if there is any additional one.

    :param st:
    :type st:
    :param expected_no_samples:
    :type expected_no_samples:
    :return:
    :rtype:
    """
    _validate_stream_with_single_trace(st=st)

    tr = st[0]
    if len(tr.data) > expected_no_samples:
        tr.data = tr.data[:expected_no_samples]
    elif len(tr.data) < expected_no_samples:
        raise ValueError(f'Provided stream has less than expected number of samples. '
                         f'Expected {expected_no_samples}, found {len(tr.data)}. ')
    else:
        return st
    return obspy.Stream(traces=tr)


def preprocess_timespan(
    trimmed_st: obspy.Stream,
    inventory: obspy.Inventory,
    processing_params: DatachunkParams,
) -> obspy.Stream:
    """
    Applies standard preprocessing to a obspy.Stream. It consist of tapering, detrending,
    response removal and filtering.

    :param trimmed_st: Stream to be treated
    :type trimmed_st: obspy.Stream
    :param inventory: Inventory to have the response removed
    :type inventory: obspy.Inventory
    :param processing_params: Processing parameters object with all required info.
    :type processing_params: DatachunkParams
    :return: Processed Stream
    :rtype: obspy.Stream
    """
    # TODO add potential plotting option
    log.info("Detrending")
    trimmed_st.detrend(type="polynomial", order=3)

    log.info("Demeaning")
    trimmed_st.detrend(type="demean")

    log.info(
        f"Resampling stream to {processing_params.sampling_rate} Hz with padding to next power of 2"
    )
    trimmed_st = resample_with_padding(
        st=trimmed_st, sampling_rate=processing_params.sampling_rate
    )  # type: ignore

    expected_samples = processing_params.get_expected_no_samples()
    if trimmed_st[0].stats.npts > expected_samples:
        trimmed_st[0].data = trimmed_st[0].data[:expected_samples]

    log.info(
        f"Tapering stream with type: {processing_params.preprocessing_taper_type}; "
        f"width: {processing_params.preprocessing_taper_width}; "
        f"side: {processing_params.preprocessing_taper_side}"
    )
    trimmed_st.taper(
        type=processing_params.preprocessing_taper_type,
        max_percentage=processing_params.preprocessing_taper_width,
        side=processing_params.preprocessing_taper_side,
    )

    log.info(
        f"Filtering with bandpass to "
        f"low: {processing_params.prefiltering_low};"
        f"high: {processing_params.prefiltering_high};"
        f"order: {processing_params.prefiltering_order}"
    )
    trimmed_st.filter(
        type="bandpass",
        freqmin=processing_params.prefiltering_low,
        freqmax=processing_params.prefiltering_high,
        corners=processing_params.prefiltering_order,
    )

    log.info("Removing response")
    trimmed_st.remove_response(inventory)
    log.info(
        f"Filtering with bandpass;"
        f"low: {processing_params.prefiltering_low}; "
        f"high: {processing_params.prefiltering_high}; "
        f"order: {processing_params.prefiltering_order};"
    )
    trimmed_st.filter(
        type="bandpass",
        freqmin=processing_params.prefiltering_low,
        freqmax=processing_params.prefiltering_high,
        corners=processing_params.prefiltering_order,
        zerophase=True,
    )

    log.info("Finished preprocessing with success")
    return trimmed_st


def validate_slice(
    trimmed_st: obspy.Stream,
    timespan: Timespan,
    processing_params: DatachunkParams,
    raw_sps: Union[float, int],
    verbose_output: bool = False
) -> Tuple[obspy.Stream, Optional[int]]:

    deficit = None
    steps_dicts = {}

    if len(trimmed_st) == 0:
        ValueError("There was no data to be cut for that timespan")

    samples_in_stream = sum([x.stats.npts for x in trimmed_st])

    minimum_no_samples = processing_params.get_raw_minimum_no_samples(raw_sps)
    expected_no_samples = processing_params.get_raw_expected_no_samples(raw_sps)

    if samples_in_stream < minimum_no_samples:
        log.error(
            f"There were {samples_in_stream} samples in the trace"
            f" while {minimum_no_samples} were expected. "
            f"Skipping this chunk."
        )
        raise ValueError("Not enough data in a chunk.")

    if len(trimmed_st) > 1:
        log.warning(
            f"There are {len(trimmed_st)} traces in that stream. "
            f"Trying to merge with Stream.merge(fill_value=0) because its has enough of "
            f"samples to pass minimum_no_samples criterium."
        )
        if verbose_output:

            steps_dicts['original'] = trimmed_st.copy()
        try:
            trimmed_st = merge_traces_under_conditions(st=trimmed_st, params=processing_params)
        except ValueError as e:
            raise ValueError(e)

        if verbose_output:
            steps_dicts['merged'] = trimmed_st.copy()

        if len(trimmed_st) > 1:
            raise ValueError(f"Merging not successfull. "
                             f"There are still {len(trimmed_st)} traces in the "
                             f"stream!")

    if samples_in_stream == expected_no_samples + 1:
        trimmed_st[0].data = trimmed_st[0].data[:-1]
        if verbose_output:
            steps_dicts['last_sample_removed'] = trimmed_st.copy()

    if samples_in_stream < expected_no_samples:
        deficit = expected_no_samples - samples_in_stream
        log.warning(
            f"Datachunk has less samples than expected but enough to be accepted."
            f"It will be padded with {deficit} zeros to match exact length."
        )
        try:
            trimmed_st = pad_zeros_to_exact_time_bounds(
                trimmed_st, timespan, expected_no_samples
            )
        except ValueError as e:
            log.error(f"Padding was not successful. {e}")
            raise ValueError("Datachunk padding unsuccessful.")

    return trimmed_st, deficit

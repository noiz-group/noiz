import logging
import numpy as np
import obspy
from collections import OrderedDict
from loguru import logger as log
from typing import Union, Tuple, Dict, Collection

from noiz.exceptions import MissingDataFileException
from noiz.globals import PROCESSED_DATA_DIR
from noiz.models.component import Component
from noiz.models.datachunk import Datachunk, DatachunkFile
from noiz.models.processing_params import DatachunkParams, ZeroPaddingMethod
from noiz.models.timeseries import Tsindex
from noiz.models.timespan import Timespan
from noiz.processing.path_helpers import assembly_filepath, assembly_sds_like_dir, assembly_preprocessing_filename, \
    increment_filename_counter, directory_exists_or_create
from noiz.processing.signal_helpers import get_min_sample_count, get_expected_sample_count, get_max_sample_count
from noiz.processing.validation_helpers import count_consecutive_trues, _validate_stream_with_single_trace


def next_pow_2(number: Union[int, float]) -> int:
    """
    Finds a number that is a power of two that is next after value provided to that method.
    Accepts only positive values.

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

    tr: obspy.Trace = st[0]
    starttime: obspy.UTCDateTime = tr.stats.starttime
    endtime: obspy.UTCDateTime = tr.stats.endtime
    npts: int = tr.stats.npts

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
        st.merge(method=1, interpolation_samples=-1, fill_value='interpolate')
    except ValueError as e:
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


def _pad_zeros_to_timespan(
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


def _taper_and_pad_zeros_to_timespan(
        st: obspy.Stream,
        timespan: Timespan,
        expected_no_samples: int,
        params: DatachunkParams,
) -> obspy.Stream:
    """
    Takes a :class:`obspy.Stream` containing a single :class:`obspy.Trace` and trims it with
    :meth:`obspy.Stream.trim` to starttime and endtime of provided :class:`noiz.models.Timespan`.
    It also verifies if resulting number of samples is as expected.

    :param st: stream to be trimmed
    :type st: obspy.Stream
    :param timespan: Timespan to be used for trimming
    :type timespan: Timespan
    :param expected_no_samples: Expected number of samples to be verified
    :type expected_no_samples: int
    :param params: Parameters used for tapering
    :type params: DatachunkParams
    :return: Trimmed stream
    :rtype: obspy.Stream
    :raises ValueError
    """
    sides = []
    if st[0].stats.starttime > timespan.starttime:
        sides.append('left')
    if st[0].stats.endtime < timespan.endtime:
        sides.append('right')
    if len(sides) == 2:
        sides = ['both']

    st.taper(
        type=params.padding_taper_type,
        max_length=params.padding_taper_max_length,
        max_percentage=params.padding_taper_max_percentage,
        side=sides[0]
    )

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


def _interpolate_ends_to_zero_to_timespan(
    st: obspy.Stream, timespan: Timespan, expected_no_samples: int
) -> obspy.Stream:
    """
    Takes a obspy Stream and trims it with Stream.trim to starttime and endtime of provided Timespan.
    It also verifies if resulting number of samples is as expected.

    It works using internal mechanism of obspy for determining number of samples to be interpolated as well as
    the interpolation itself.
    If there are any samples missing on either end of the :class:`obspy.Trace`, it creates a new :class:`obspy.Stream`
    that contains the original :class:`obspy.Trace`.
    Afterwards, depending on which side there are samples missing, it creates a new traces with a single sample equal
    to zero, that starttime is either at :param:`noiz.models.Timespan.starttime` or
    :param:`noiz.models.Timespan.endtime` depending on the side.
    Finally, it merges resulting trace list with :meth:`obspy.Stream.merge` with parameters of `method=1`
    and `fill_value='interpolate'` which results in interpolating values between the core trace and those
    zeros on the start or on the end.

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
        trace_list.append(tr_start)
    if st[0].stats.endtime < timespan.endtime:
        tr_end = tr_temp.copy()
        tr_end.stats.starttime = timespan.endtime_at_last_sample(st[0].stats.sampling_rate)
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


def preprocess_sliced_stream_for_datachunk(
        trimmed_st: obspy.Stream,
        inventory: obspy.Inventory,
        processing_params: DatachunkParams,
        timespan: Timespan,
        verbose_output: bool = False
) -> Tuple[obspy.Stream, Dict[str, obspy.Stream]]:
    """
    Applies standard preprocessing to a :class:`~obspy.Stream`.
    It consist of tapering, detrending, response removal and filtering.

    :param trimmed_st: Stream to be treated
    :type trimmed_st: obspy.Stream
    :param inventory: Inventory to have the response removed
    :type inventory: obspy.Inventory
    :param processing_params: Processing parameters object with all required info.
    :type processing_params: DatachunkParams
    :param timespan: Timespan for which this datachunk is processed for
    :type timespan: Timespan
    :return: Processed Stream
    :rtype: obspy.Stream
    """
    # py39 This method should return in annotation OrderedDict but there is issue. It's fixed in Python 3.9

    steps_dict: OrderedDict[str, obspy.Stream] = OrderedDict()

    if verbose_output:
        steps_dict['original'] = trimmed_st.copy()

    log.info("Detrending")
    trimmed_st.detrend(type="polynomial", order=3)

    if verbose_output:
        steps_dict['detrended'] = trimmed_st.copy()

    log.info("Demeaning")
    trimmed_st.detrend(type="demean")
    if verbose_output:
        steps_dict['demeaned'] = trimmed_st.copy()

    log.info(
        f"Resampling stream to {processing_params.sampling_rate} Hz with padding to next power of 2"
    )
    trimmed_st = resample_with_padding(
        st=trimmed_st, sampling_rate=processing_params.sampling_rate
    )  # type: ignore
    if verbose_output:
        steps_dict['resampled'] = trimmed_st.copy()

    expected_samples = get_expected_sample_count(timespan=timespan, sampling_rate=processing_params.sampling_rate)

    if trimmed_st[0].stats.npts > expected_samples:
        trimmed_st[0].data = trimmed_st[0].data[:expected_samples]
        if verbose_output:
            steps_dict['trimmed_last_sample'] = trimmed_st.copy()

    log.info(f"Tapering stream with {processing_params.preprocessing_taper_type} taper ")
    trimmed_st.taper(
        type=processing_params.preprocessing_taper_type,
        max_length=processing_params.preprocessing_taper_max_length,
        max_percentage=processing_params.preprocessing_taper_max_percentage,
        side=processing_params.preprocessing_taper_side,
    )
    if verbose_output:
        steps_dict['tapered'] = trimmed_st.copy()

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
    if verbose_output:
        steps_dict['filtered'] = trimmed_st.copy()

    log.info("Removing response")
    trimmed_st.remove_response(inventory)
    if verbose_output:
        steps_dict['removed_response'] = trimmed_st.copy()

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
    if verbose_output:
        steps_dict['filtered_second_time'] = trimmed_st.copy()

    log.info("Finished preprocessing with success")

    return trimmed_st, steps_dict


def validate_slice(
    trimmed_st: obspy.Stream,
    timespan: Timespan,
    processing_params: DatachunkParams,
    original_samplerate: Union[float, int],
    verbose_output: bool = False
) -> Tuple[obspy.Stream, int, Dict[str, obspy.Stream]]:
    # py39 This method should return in annotation OrderedDict but there is issue. It's fixed in Python 3.9

    deficit = 0
    steps_dict: OrderedDict[str, obspy.Stream] = OrderedDict()

    if len(trimmed_st) == 0:
        ValueError("There was no data to be cut for that timespan")

    try:
        validate_sample_rate(original_samplerate, trimmed_st)
        validate_timebounds_agains_timespan(trimmed_st, timespan)
        validate_sample_count_in_stream(trimmed_st, processing_params, timespan)
    except ValueError as e:
        log.error(e)
        raise ValueError(e)

    if len(trimmed_st) > 1:
        log.warning(
            f"There are {len(trimmed_st)} traces in that stream. "
            f"Trying to merge with merge_traces_under_conditions because its has enough of "
            f"samples to pass minimum_no_samples criterion."
        )
        if verbose_output:
            steps_dict['original'] = trimmed_st.copy()
        try:
            trimmed_st = merge_traces_under_conditions(st=trimmed_st, params=processing_params)
        except ValueError as e:
            log.error(e)
            raise ValueError(e)

        if verbose_output:
            steps_dict['merged'] = trimmed_st.copy()

        if len(trimmed_st) > 1:
            message = (
                f"Merging not successfull. "
                f"There are still {len(trimmed_st)} traces in the "
                f"stream!"
            )
            log.error(message)
            raise ValueError(message)

    expected_no_samples = get_expected_sample_count(timespan=timespan, sampling_rate=original_samplerate)
    samples_in_stream = sum_samples_in_stream(st=trimmed_st)

    if samples_in_stream == expected_no_samples + 1:
        trimmed_st = _check_and_remove_extra_samples_on_the_end(st=trimmed_st, expected_no_samples=expected_no_samples)
        if verbose_output:
            steps_dict['last_sample_removed'] = trimmed_st.copy()

    if samples_in_stream < expected_no_samples:
        deficit = expected_no_samples - samples_in_stream
        log.warning(
            f"Datachunk has less samples than expected but enough to be accepted."
            f"It will be padded with {deficit} zeros to match exact length."
        )
        try:

            if verbose_output:
                steps_dict['padded'] = trimmed_st.copy()
        except ValueError as e:
            log.error(f"Padding was not successful. {e}")
            raise ValueError(f"Datachunk padding unsuccessful. {e}")

    return trimmed_st, deficit, steps_dict


def perform_padding_according_to_config(
        st: obspy.Stream,
        timespan: Timespan,
        expected_no_samples: int,
        params: DatachunkParams
) -> obspy.Stream:

    selected_method = params.zero_padding_method

    if selected_method is ZeroPaddingMethod.PADDED:
        return _pad_zeros_to_timespan(
            st=st,
            expected_no_samples=expected_no_samples,
            timespan=timespan,
        )
    elif selected_method is ZeroPaddingMethod.TAPERED_PADDED:
        return _taper_and_pad_zeros_to_timespan(
            st=st,
            expected_no_samples=expected_no_samples,
            timespan=timespan,
            params=params,
        )
    elif selected_method is ZeroPaddingMethod.INTERPOLATED:
        return _interpolate_ends_to_zero_to_timespan(
            st=st,
            timespan=timespan,
            expected_no_samples=expected_no_samples,
        )
    else:
        raise NotImplementedError('Selected zero padding method not supported. ')


def validate_sample_count_in_stream(st: obspy.Stream, params: DatachunkParams, timespan: Timespan) -> bool:
    """
    Checks if sample count in the whole stream is within tolerance bounds set in the
    :attr:`noiz.models.DatachunkParams.datachunk_sample_tolerance`

    :param st:
    :type st:
    :param params:
    :type params:
    :param timespan:
    :type timespan:
    :return:
    :rtype:
    """
    samples_in_stream = sum_samples_in_stream(st)

    sampling_rate = st[0].stats.sampling_rate

    min_no_samples = get_min_sample_count(timespan=timespan, params=params,
                                          sampling_rate=sampling_rate)
    max_no_samples = get_max_sample_count(timespan=timespan, params=params,
                                          sampling_rate=sampling_rate)

    if min_no_samples > samples_in_stream > max_no_samples:
        message = (
            f"The number of samples in signal exceed limits. "
            f"Expected more than {min_no_samples}, and less than {max_no_samples} found in stream {samples_in_stream}. "
            f"You should make sure that the sampling rate and all the rest of Stream params are okay. "
        )
        logging.error(message)
        raise ValueError(message)
    return True


def sum_samples_in_stream(st: obspy.Stream) -> int:
    """
    Sums up npts of all traces in the stream

    :param st: Stream to sum up samples of
    :type st: obspy.Stream
    :return: Sum of samples in stream
    :rtype: int
    """
    samples_in_stream = sum([x.stats.npts for x in st])
    return samples_in_stream


def validate_timebounds_agains_timespan(st, timespan):

    st.sort(keys=['starttime'])
    if st[0].stats.starttime < timespan.starttime:
        message = (
            f"Provided stream has starttime before timespan starts. "
            f"Are you sure you trimmed it first? "
            f"Stream starttime: {st[0].stats.starttime}, timespan starttime: {timespan.starttime}"
        )
        raise ValueError(message)
    if st[-1].stats.endtime > timespan.endtime:
        message = (
            f"Provided stream has endtime after timespan ends. "
            f"Are you sure you trimmed it first? "
            f"Stream endtime: {st[-1].stats.endtime}, timespan endtime: {timespan.endtime}"
        )
        raise ValueError(message)


def validate_sample_rate(original_samplerate, trimmed_st):
    sample_rates = list(set([x.stats.sampling_rate for x in trimmed_st]))
    if len(sample_rates) != 1:
        raise ValueError("The sampling rate in the stream is not uniform!")
    if sample_rates[0] != original_samplerate:
        message = (
            f"Sampling rate of provided stream is different than expected. "
            f"Found sampling_rate {sample_rates[0]}, expected {original_samplerate} "
        )
        raise ValueError(message)


def create_datachunks_for_component(
        component: Component,
        timespans: Collection[Timespan],
        time_series: Tsindex,
        processing_params: DatachunkParams
) -> Collection[Datachunk]:
    """
    All around method that is takes prepared Component, Tsindex,
    DatachunkParams and bunch of Timespans to slice the continuous seed file
    into shorter one, reflecting all the Timespans.
    It saves the file to the drive but it doesn't add entry to DB.

    Returns collection of Datachunks with DatachunkFile associated to it,
    ready to be added to DB.

    :param component:
    :type component: Component
    :param timespans: Timespans on base of which you want your
    datachunks to be created.
    :type timespans: Collection[Timespans]
    :param time_series: Tsindex object that hs information about
    location of continuous seed file
    :type time_series: Tsindex
    :param processing_params:
    :type processing_params: DatachunkParams
    :return: Datachunks ready to be sent to DB.
    :rtype: Collection[Datachunk]
    """

    log.info("Reading timeseries and inventory")
    try:
        st: obspy.Stream = time_series.read_file()
    except MissingDataFileException as e:
        log.warning(f"Data file is missing. Skipping. {e}")
        return []
    except Exception as e:
        log.warning(f"There was some general exception from "
                    f"obspy.Stream.read function. Here it is: {e} ")
        return []

    inventory: obspy.Inventory = component.read_inventory()

    finished_datachunks = []

    log.info(f"Splitting full day into timespans for {component}")
    for timespan in timespans:

        log.info(f"Slicing timespan {timespan}")
        trimmed_st: obspy.Trace = st.slice(
            starttime=timespan.starttime_obspy(),
            endtime=timespan.remove_last_microsecond(),
            nearest_sample=False,
        )

        try:
            trimmed_st, padded_npts, _ = validate_slice(
                trimmed_st=trimmed_st,
                timespan=timespan,
                processing_params=processing_params,
                original_samplerate=float(time_series.samplerate),
                verbose_output=False,
            )
        except ValueError as e:
            log.warning(f"There was a problem with trace validation. "
                        f"There was raised exception {e}")
            continue

        log.info("Preprocessing timespan")
        trimmed_st, _ = preprocess_sliced_stream_for_datachunk(
            trimmed_st=trimmed_st,
            inventory=inventory,
            processing_params=processing_params,
            timespan=timespan,
            verbose_output=False
        )

        filepath = assembly_filepath(
            PROCESSED_DATA_DIR,  # type: ignore
            "datachunk",
            assembly_sds_like_dir(component, timespan) \
                                 .joinpath(assembly_preprocessing_filename(
                                                component=component,
                                                timespan=timespan,
                                                count=0
                                            )),
        )

        if filepath.exists():
            log.info(f'Filepath {filepath} exists. '
                     f'Trying to find next free one.')
            filepath = increment_filename_counter(filepath=filepath)
            log.info(f"Free filepath found. "
                     f"Datachunk will be saved to {filepath}")

        log.info(f"Chunk will be written to {str(filepath)}")
        directory_exists_or_create(filepath)

        datachunk_file = DatachunkFile(filepath=str(filepath))
        trimmed_st.write(datachunk_file.filepath, format="mseed")

        sampling_rate: Union[str, float] = trimmed_st[0].stats.sampling_rate
        npts: int = trimmed_st[0].stats.npts

        datachunk = Datachunk(
            datachunk_params_id=processing_params.id,
            component_id=component.id,
            timespan_id=timespan.id,
            sampling_rate=sampling_rate,
            npts=npts,
            datachunk_file=datachunk_file,
            padded_npts=padded_npts,
        )
        log.info(
            "Checking if there are some chunks fot tht timespan and component in db"
        )

        finished_datachunks.append(datachunk)

    return finished_datachunks

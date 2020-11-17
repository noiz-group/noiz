import logging
import numpy as np
import obspy
from pathlib import Path
from typing import Union, Optional, Tuple


from noiz.models.component import Component
from noiz.models.processing_params import DatachunkParams
from noiz.models.timespan import Timespan


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


def assembly_preprocessing_filename(
        component: Component,
        timespan: Timespan,
        count: int = 0
) -> str:
    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    fname = ".".join([
        component.network,
        component.station,
        component.component,
        year,
        doy_time,
        str(count)
    ])

    return fname


def assembly_sds_like_dir(component: Component, timespan: Timespan) -> Path:
    """
    Asembles a Path object in a SDS manner. Object consists of year/network/station/component codes.

    Warning: The component here is a single letter component!

    :param component: Component object containing information about used channel
    :type component: Component
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return:  Pathlike object containing SDS-like directory hierarchy.
    :rtype: Path
    """
    return (
        Path(str(timespan.starttime.year))
        .joinpath(component.network)
        .joinpath(component.station)
        .joinpath(component.component)
    )


def assembly_filepath(
    processed_data_dir: Union[str, Path],
    processing_type: Union[str, Path],
    filepath: Union[str, Path],
) -> Path:
    """
    Assembles a filepath for processed files.
    It assembles a root processed-data directory with processing type and a rest of a filepath.

    :param processed_data_dir:
    :type processed_data_dir: Path
    :param processing_type:
    :type processing_type: Path
    :param filepath:
    :type filepath: Path
    :return:
    :rtype: Path
    """
    return Path(processed_data_dir).joinpath(processing_type).joinpath(filepath)


def directory_exists_or_create(filepath: Path) -> bool:
    """
    Checks if directory of a filepath exists. If doesnt, it creates it.
    Returns bool that indicates if the directory exists in the end. Should be always True.

    :param filepath: Path to the file you want to save and check it the parent directory exists.
    :type filepath: Path
    :return: If the directory exists in the end.
    :rtype: bool
    """
    directory = filepath.parent
    log.info(f"Checking if directory {directory} exists")
    if not directory.exists():
        log.info(f"Directory {directory} does not exists, trying to create.")
        directory.mkdir(parents=True)
    return directory.exists()


def increment_filename_counter(filepath: Path) -> Path:
    """
    Takes a filepath with int as suffix and returns a non existing filepath
     that has next free int value as suffix.

    :param filepath: Filepath to find next free path for
    :type filepath: Path
    :return: Free filepath
    :rtype: Path
    :raises: ValueError
    """

    while True:
        if not filepath.exists():
            return filepath

        suffix: str = filepath.suffix[1:]
        try:
            suffix_int: int = int(suffix)
        except ValueError:
            raise ValueError(f"The filepath's {filepath} suffix {suffix} "
                             f"cannot be casted to int")
        filepath = filepath.with_suffix(f".{suffix_int+1}")


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
    and resamples it down to provided sampling rate. Furtherwards trims it to original starttime and endtime.

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

    if st[0].stats.npts == expected_no_samples+1:
        st[0].data = st[0].data[:-1]

    if st[0].stats.npts != expected_no_samples:
        raise ValueError(
            f"The try of padding with zeros to {expected_no_samples} was "
            f"not successful. Current length of data is {st[0].stats.npts}. "
        )
    return st


def preprocess_timespan(
    trimed_st: obspy.Stream,
    inventory: obspy.Inventory,
    processing_params: DatachunkParams,
) -> obspy.Stream:
    """
    Applies standard preprocessing to a obspy.Stream. It consist of tapering, detrending,
    response removal and filtering.

    :param trimed_st: Stream to be treated
    :type trimed_st: obspy.Stream
    :param inventory: Inventory to have the response removed
    :type inventory: obspy.Inventory
    :param processing_params: Processing parameters object with all required info.
    :type processing_params: DatachunkParams
    :return: Processed Stream
    :rtype: obspy.Stream
    """
    # TODO add potential plotting option
    log.info("Detrending")
    trimed_st.detrend(type="polynomial", order=3)

    log.info("Demeaning")
    trimed_st.detrend(type="demean")

    log.info(
        f"Resampling stream to {processing_params.sampling_rate} Hz with padding to next power of 2"
    )
    trimed_st = resample_with_padding(
        st=trimed_st, sampling_rate=processing_params.sampling_rate
    )  # type: ignore

    expected_samples = processing_params.get_expected_no_samples()
    if trimed_st[0].stats.npts > expected_samples:
        trimed_st[0].data = trimed_st[0].data[:expected_samples]

    log.info(
        f"Tapering stream with type: {processing_params.preprocessing_taper_type}; "
        f"width: {processing_params.preprocessing_taper_width}; "
        f"side: {processing_params.preprocessing_taper_side}"
    )
    trimed_st.taper(
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
    trimed_st.filter(
        type="bandpass",
        freqmin=processing_params.prefiltering_low,
        freqmax=processing_params.prefiltering_high,
        corners=processing_params.prefiltering_order,
    )

    log.info("Removing response")
    trimed_st.remove_response(inventory)
    log.info(
        f"Filtering with bandpass;"
        f"low: {processing_params.prefiltering_low}; "
        f"high: {processing_params.prefiltering_high}; "
        f"order: {processing_params.prefiltering_order};"
    )
    trimed_st.filter(
        type="bandpass",
        freqmin=processing_params.prefiltering_low,
        freqmax=processing_params.prefiltering_high,
        corners=processing_params.prefiltering_order,
        zerophase=True,
    )

    log.info("Finished preprocessing with success")
    return trimed_st


def validate_slice(
    trimed_st: obspy.Stream,
    timespan: Timespan,
    processing_params: DatachunkParams,
    raw_sps: Union[float, int],
) -> Tuple[obspy.Stream, Optional[int]]:

    deficit = None

    if len(trimed_st) == 0:
        ValueError("There was no data to be cut for that timespan")

    samples_in_stream = sum([x.stats.npts for x in trimed_st])

    minimum_no_samples = processing_params.get_raw_minimum_no_samples(raw_sps)
    expected_no_samples = processing_params.get_raw_expected_no_samples(raw_sps)

    if samples_in_stream < minimum_no_samples:
        log.error(
            f"There were {samples_in_stream} samples in the trace"
            f" while {minimum_no_samples} were expected. "
            f"Skipping this chunk."
        )
        raise ValueError("Not enough data in a chunk.")

    if len(trimed_st) > 1:
        log.warning(
            f"There are {len(trimed_st)} traces in that stream. "
            f"Trying to merge with Stream.merge(fill_value=0) because its has enough of "
            f"samples to pass minimum_no_samples criterium."
        )

        try:
            trimed_st = merge_traces_fill_zeros(trimed_st)
        except ValueError as e:
            raise ValueError(e)

        if len(trimed_st) > 1:
            raise ValueError(f"Merging not successfull. "
                             f"There are still {len(trimed_st)} traces in the "
                             f"stream!")

    if samples_in_stream == expected_no_samples + 1:
        trimed_st[0].data = trimed_st[0].data[:-1]
        return trimed_st, deficit

    if samples_in_stream < expected_no_samples:
        deficit = expected_no_samples - samples_in_stream
        log.warning(
            f"Datachunk has less samples than expected but enough to be accepted."
            f"It will be padded with {deficit} zeros to match exact length."
        )
        try:
            trimed_st = pad_zeros_to_exact_time_bounds(
                trimed_st, timespan, expected_no_samples
            )
        except ValueError as e:
            log.error(f"Padding was not successful. {e}")
            raise ValueError("Datachunk padding unsuccessful.")

    return trimed_st, deficit


def count_consecutive_trues(arr: np.array) -> np.array:
    """
    This method takes an array of booleans and counts all how many consecutive True values are within it.
    It returns an array of counts.

    For example:

    >>> a = count_consecutive_trues([0,0,0,0,1,1,1,0,0,0], dtype=bool)
    >>> a == np.array([3])

    It can also handle multiple subvectors of True:

    >>> a = count_consecutive_trues(np.array([0,1,0,0,1,1,1,0,0,0,1,1,1,1,1,1], dtype=bool))
    >>> a == np.array([1,3,6])

    This method is copied from:
    https://stackoverflow.com/a/24343375/4308541

    :param arr: Array of booleans
    :type arr: np.array
    :return: Array of integers, with counts how many consecutive True values are in the input arr array
    :rtype: np.array
    """

    counted_true_vals = np.diff(
        np.where(
            np.concatenate(
                ([arr[0]], arr[:-1] != arr[1:], [True])
            )
        )[0]
    )[::2]

    return counted_true_vals

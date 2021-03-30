from collections import Counter

from loguru import logger
from typing import Union

import obspy
from noiz.exceptions import NotEnoughDataError, ValidationError
from noiz.models import Timespan, DatachunkParams


def get_expected_sample_count(timespan: Timespan, sampling_rate: Union[int, float]):
    """
    Expected number of samples from datachunk. Calculated as number of seconds in timespan time sampling rate.

    :param timespan:
    :type timespan:
    :param sampling_rate:
    :type sampling_rate:
    :return:
    :rtype:
    """
    return int(timespan.length.seconds * sampling_rate)


def get_min_sample_count(params: DatachunkParams, timespan: Timespan, sampling_rate: Union[float, int]):
    """
    Minimum acceptable number of samples for datachunk. Calculated as expected number of samples - tolerance defined in
    :class:`noiz.models.DatachunkParams`.

    :param params:
    :type params:
    :param timespan:
    :type timespan:
    :param sampling_rate:
    :type sampling_rate:
    :return:
    :rtype:
    """
    return int(get_expected_sample_count(timespan=timespan, sampling_rate=sampling_rate)
               * (1 - params.datachunk_sample_tolerance))


def get_max_sample_count(params: DatachunkParams, timespan: Timespan, sampling_rate: Union[float, int]):
    """
    Maximum acceptable number of samples for datachunk. Calculated as expected number of samples + tolerance defined in
    :class:`noiz.models.DatachunkParams`

    :param params:
    :type params:
    :param timespan:
    :type timespan:
    :param sampling_rate:
    :type sampling_rate:
    :return:
    :rtype:
    """
    return int(get_expected_sample_count(timespan=timespan, sampling_rate=sampling_rate)
               * (1 + params.datachunk_sample_tolerance))


def validate_and_fix_subsample_starttime_error(st: obspy.Stream) -> obspy.Stream:
    """
    In some cases, there might be occurring a tiny shift in starttime in some traces compared to the others.
    This method checks if the differences between starttimes of traces within a stream are negligible and if yes,
    it corrects them.

    This method assumes that all other parameters were already verified and are the same for all traces.
    All other parameters such as sampling rate.

    filldocs This is very quick abomination of a documentation. Fix it

    :param st: Stream to be validated
    :type st: obspy.Stream
    :return: Validated stream
    :rtype: obspy.Stream
    """
    if len(st) < 3:
        raise NotEnoughDataError("There should be at least 3 traces in a stream to perform this validation")

    delta = st[0].stats.delta
    starttimes = [tr.stats.starttime for tr in st]

    min_starttime = min(starttimes)
    max_starttime = max(starttimes)
    most_common_starttime = obspy.UTCDateTime(Counter([s.timestamp for s in starttimes]).most_common()[0][0])

    if 0 == abs(most_common_starttime - min_starttime) and 0 == abs(most_common_starttime - max_starttime):
        return st

    if abs(most_common_starttime - min_starttime) < delta / 2 and \
            abs(most_common_starttime - max_starttime) < delta / 2:
        logger.warning("Fixing the subsample time error")
        for tr in st:
            tr.stats.starttime = most_common_starttime
    else:
        raise ValidationError("The subsample time error is higher than half of the delta time between samples!")
    return st

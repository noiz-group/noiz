from typing import Union

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

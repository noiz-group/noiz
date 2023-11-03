# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
import numpy as np
import pandas as pd
from typing import Union, Optional, Tuple, Generator, Any, List

from noiz.models.timespan import Timespan


def generate_starttimes_endtimes(
    startdate: Union[datetime.datetime, np.datetime64],
    enddate: Union[datetime.datetime, np.datetime64],
    window_length: Union[float, int, pd.Timedelta, np.timedelta64],
    window_overlap: Optional[Union[float, int, pd.Timedelta, np.timedelta64]] = None,
    generate_midtimes: bool = False,
) -> Union[
    Tuple[List[pd.Timestamp], List[pd.Timestamp]],
    Tuple[List[pd.Timestamp], List[pd.Timestamp], List[pd.Timestamp]],
]:
    """
    FIXME: Add actual description

    :param startdate: Starting day of the dates range. Will be rounded to midnight.
    :type startdate: Union[datetime.datetime, np.datetime64]
    :param enddate:  Starting day of the dates range. Will be rounded to midnight.
    :type enddate: Union[datetime.datetime, np.datetime64],
    :param window_length: Length of the window. Should be number of seconds or timedelta.
    :type window_length: Union[float, int, pd.Timedelta, np.timedelta64]
    :param window_overlap: Length of overlap. Should be number of seconds or timedelta. Defaults to None.
    :type window_overlap: Optional[Union[float, int, pd.Timedelta, np.timedelta64]
    :param generate_midtimes: Generating midtimes flag. If True, there will be generated third timeindex
    representing midtime of each window.
    :type generate_midtimes: bool
    :return: Returns two or three iterables, first with start-times, optional second with mid-times, last with end times
    :rtype: Union[
            Tuple[List[pd.Timestamp], List[pd.Timestamp]],
            Tuple[List[pd.Timestamp], List[pd.Timestamp], List[pd.Timestamp]],
        ]
    """

    window_length = cast_to_timedelta(window_length)
    window_overlap = cast_to_timedelta(window_overlap) if window_overlap is not None else None

    freq = _calculate_frequency_for_generating_timespans(window_length, window_overlap)

    starttimes = pd.date_range(
        start=startdate, end=enddate, freq=freq, normalize=True, closed='left'
    )
    endtimes = starttimes + window_length
    if not generate_midtimes:
        return starttimes.to_list(), endtimes.to_list()
    else:
        midtimes = starttimes + window_length / 2
        return starttimes.to_list(), midtimes.to_list(), endtimes.to_list()


def _calculate_frequency_for_generating_timespans(
        window_length: pd.Timedelta,
        window_overlap: Optional[pd.Timedelta] = None
) -> pd.Timedelta:
    """
    Calculates frequency for generation of range of :class:`noiz.models.timespan.Timespan`
    It is later on passed into a :func:`pandas.date_range` function.
    It takes into account the window length and window overlap.

    :param window_length: Length of the timespan window
    :type window_length: pd.Timedelta
    :param window_overlap: Optional timespan window overlap
    :type window_overlap: Optional[pd.Timedelta]
    :return: Frequency for generation of windows
    :rtype: pd.Timedelta
    :raises: ValueError in case window overlap is longer or equal to the window length
    """
    if window_overlap is None:
        return window_length

    if window_overlap >= window_length:
        raise ValueError(
            f"The overlap time `{window_overlap}` cannot be equal or longer than window length `{window_length}`"
        )
    return pd.Timedelta(window_length - window_overlap)


def cast_to_timedelta(
        window: Union[float, int, pd.Timedelta, np.timedelta64],
        resolution: str = "s",
) -> Union[pd.Timedelta, np.timedelta64]:
    """
    Casts a float or int value to pd.Timedelta with a provided resolution.
    In case a pd.Timedelta or np.timedelta64 is provided, returns the same value.
    For any other type of value, a TypeError is raised.

    Resolution has to be provided as a string that is accepted as an Offset string in pandas.

    :param window: A time window to be cast
    :type window: Union[float, int, pd.Timedelta, np.timedelta64]
    :param resolution: Time resolution of the expected timedelta
    :type resolution: str
    :return: Expected timedelta object
    :rtype: Union[pd.Timedelta, np.timedelta64]
    :raises: TypeError
    """
    if isinstance(window, (float, int)):
        return pd.Timedelta(window, resolution)
    if isinstance(window, (pd.Timedelta, np.timedelta64)):
        return window
    raise TypeError(f"This function accepts only "
                    f"Union[float, int, pd.Timedelta, np.timedelta64]"
                    f"You provided value of type {type(window)}")


def generate_timespans(
    startdate: Union[datetime.datetime, np.datetime64],
    enddate: Union[datetime.datetime, np.datetime64],
    window_length: Union[float, int, pd.Timedelta, np.timedelta64],
    window_overlap: Optional[Union[float, int, pd.Timedelta, np.timedelta64]] = None,
    generate_over_midnight: bool = False,
) -> Generator[Timespan, Any, Any]:
    """
    Creates instances of :class:`noiz.models.timespan.Timespan` according to passed specifications.
    For generation of the series of those instances uses :func:`~noiz.processing.timespan.generate_timespans`
    It is being able to generate windows of specified length between two dates, with or without overlapping.
    It is also able to generate or not windows spanning over midnight since sometimes that can be problematic to have
    a window across two days.

    :param startdate: Starttime for requested timespans. Warning! It will be normalized to midnight.
    :type startdate: datetime.datetime
    :param enddate: Endtime for requested timespans. Warning! It will be normalized to midnight.
    :type enddate: datetime.datetime
    :param window_length: Window length in seconds
    :type window_length: Union[int, float]
    :param window_overlap: Window overlap in seconds
    :type window_overlap: Optional[Union[int, float]]
    :param generate_over_midnight: If windows spanning over midnight should be included
    :type generate_over_midnight: bool
    :return: Generator of timespans
    :rtype: Generator[Timespan, Any, Any]
    """
    timespans = generate_starttimes_endtimes(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_midtimes=True,
    )

    if generate_over_midnight:
        for starttime, midtime, endtime in zip(*timespans):
            yield Timespan(starttime=starttime, midtime=midtime, endtime=endtime)
    else:
        for starttime, midtime, endtime in zip(*timespans):
            timespan = Timespan(starttime=starttime, midtime=midtime, endtime=endtime)
            if timespan.same_day():
                yield timespan

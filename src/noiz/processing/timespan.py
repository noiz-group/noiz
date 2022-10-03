import datetime
import numpy as np
import pandas as pd
from typing import Union, Optional, Tuple, Iterable, Generator, Any

from noiz.models.timespan import Timespan


def generate_starttimes_endtimes(
    startdate: Union[datetime.datetime, np.datetime64],
    enddate: Union[datetime.datetime, np.datetime64],
    window_length: Union[float, int, pd.Timedelta, np.timedelta64],
    window_overlap: Optional[Union[float, int, pd.Timedelta, np.timedelta64]] = None,
    generate_midtimes: bool = False,
) -> Union[
    Tuple[pd.DatetimeIndex, pd.DatetimeIndex],
    Tuple[pd.DatetimeIndex, pd.DatetimeIndex, pd.DatetimeIndex],
]:
    """

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
    :return: Returns two iterables, first with starttimes second with endtimes.
    :rtype: Tuple[pd.DatetimeIndex, pd.DatetimeIndex]
    """

    if isinstance(window_length, (float, int)):
        window_length = pd.Timedelta(window_length, "s")
    if isinstance(window_overlap, (float, int)):
        window_overlap = pd.Timedelta(window_overlap, "s")

    if window_overlap is None:
        starttime_freq = window_length
    elif isinstance(window_overlap, (pd.Timedelta, np.timedelta64)):
        starttime_freq = window_length - window_overlap
        if window_overlap >= window_length:
            raise ValueError(
                f"The overlap time `{window_overlap}` cannot be equal or longer than window length `{window_length}`"
            )
    else:
        raise ValueError(
            "The overlap is expected to be eitherint or float of seconds \
                         or pd.Timedelta or np.timedelta64"
        )

    starttimes = pd.date_range(
        start=startdate, end=enddate, freq=starttime_freq, normalize=True, closed='left'
    )
    endtimes = starttimes + window_length
    if not generate_midtimes:
        return starttimes.to_list(), endtimes.to_list()
    else:
        midtimes = starttimes + window_length / 2
        return starttimes.to_list(), midtimes.to_list(), endtimes.to_list()


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

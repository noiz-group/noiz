import datetime

import pandas as pd
from typing import Union

import numpy as np
import numpy.typing as npt
import obspy


def count_consecutive_trues(arr: npt.ArrayLike) -> npt.ArrayLike:
    """
    This method takes an array of booleans and counts all how many consecutive True values are within it.
    It returns an array of counts.

    For example:

    >>> a = count_consecutive_trues([0, 0, 0, 0, 1, 1, 1, 0, 0, 0], dtype=bool)
    >>> a == np.array([3])

    It can also handle multiple subvectors of True:

    >>> a = count_consecutive_trues(np.array([0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1], dtype=bool))
    >>> a == np.array([1, 3, 6])

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
                ([arr[0]], arr[:-1] != arr[1:], [True])  # type: ignore
            )
        )[0]
    )[::2]

    return counted_true_vals


def _validate_stream_with_single_trace(st: obspy.Stream) -> None:
    """
    This is small helper method that checks if provided argument is :class:`obspy.Stream` and contains only one
    :class:`obspy.Trace` inside.

    :param st: Stream to be checked
    :type st: obspy.Stream
    :return: None
    :rtype: NoneType
    :raises: TypeError, ValueError
    """
    if not isinstance(st, obspy.Stream):
        raise TypeError(f"obspy.Stream was expected. Got {type(st)}")
    if len(st) != 1:
        raise ValueError(f"This method expects exactly one trace in the stream! There were {len(st)} traces found.")


def _validate_timedelta(timedelta: Union[pd.Timedelta, datetime.timedelta, str]):
    if timedelta is None:
        return None
    if isinstance(timedelta, pd.Timedelta):
        return timedelta.to_pytimedelta()
    if isinstance(timedelta, datetime.timedelta):
        return timedelta
    if isinstance(timedelta, str):
        return pd.Timedelta(timedelta).to_pytimedelta()

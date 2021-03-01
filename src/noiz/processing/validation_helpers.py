import datetime

import pandas as pd
from typing import Union, Optional

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


def _validate_timedelta_as_pytimedelta(
        timedelta: Union[pd.Timedelta, datetime.timedelta, str]
) -> datetime.timedelta:
    if isinstance(timedelta, pd.Timedelta):
        return timedelta.to_pytimedelta()
    elif isinstance(timedelta, datetime.timedelta):
        return timedelta
    elif isinstance(timedelta, str):
        return pd.Timedelta(timedelta).to_pytimedelta()
    else:
        raise TypeError(f"Valid types are: pd.Timedelta, datetime.timedelta and str that can be parsed as pd.Timedelta"
                        f"Provided variable is {type(timedelta)}")


def _validate_as_pytimedelta_or_none(
        var: Optional[Union[pd.Timedelta, datetime.timedelta, str]]
) -> Optional[datetime.timedelta]:
    if var is not None:
        return _validate_timedelta_as_pytimedelta(var)
    else:
        return None


def _validate_timestamp_as_pdtimestamp(
    time_obj: Union[pd.Timestamp, datetime.datetime, np.datetime64]
) -> pd.Timestamp:
    """
    Takes a time object and converts it to a pd.Timestamp if originally it was either datetime.datetime,
    np.datetime64 or pd.Timestamp
    :param time_obj: Time object to be validated
    :type time_obj: Union[pd.Timestamp, datetime.datetime, np.datetime64]
    :return: Validated Timestamp
    :rtype: pd.Timestamp
    """
    if isinstance(time_obj, pd.Timestamp):
        return time_obj
    elif isinstance(time_obj, (datetime.datetime, np.datetime64)):
        return pd.Timestamp(time_obj)

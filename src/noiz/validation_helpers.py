import datetime
import numpy as np
import numpy.typing as npt
import obspy
import pandas as pd
from typing import Union, Optional, Tuple, Any, Type, List


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
    """
    Checks if provided variable is either :py:class:`pandas.Timedelta`, :py:class:`datetime.timedelta` or
    a string that can be parsed by :py:class:`pandas.Timedelta` and converts it to :py:class:`datetime.timedelta`.

    :param var: Variable to be checked
    :type var: Union[pd.Timedelta, datetime.timedelta, str]
    :return: Validated timedelta
    :rtype: datetime.timedelta
    """
    if isinstance(timedelta, pd.Timedelta):
        return timedelta.to_pytimedelta()
    elif isinstance(timedelta, datetime.timedelta):
        return timedelta
    elif isinstance(timedelta, str):
        return pd.Timedelta(timedelta).to_pytimedelta()
    else:
        raise TypeError(f"Valid types are: pd.Timedelta, datetime.timedelta and str that can be parsed as pd.Timedelta"
                        f"Provided variable is {type(timedelta)}")


def _validate_timedelta_as_pdtimedelta(
        timedelta: Union[pd.Timedelta, datetime.timedelta, str]
) -> pd.Timedelta:
    """
    Checks if provided variable is either :py:class:`pandas.Timedelta`, :py:class:`datetime.timedelta` or
    a string that can be parsed by :py:class:`pandas.Timedelta` and converts it to :py:class:`datetime.timedelta`.

    :param var: Variable to be checked
    :type var: Union[pd.Timedelta, datetime.timedelta, str]
    :return: Validated timedelta
    :rtype: pd.Timedelta
    """
    if isinstance(timedelta, pd.Timedelta):
        return timedelta
    elif isinstance(timedelta, datetime.timedelta):
        return pd.Timedelta(timedelta)
    elif isinstance(timedelta, str):
        return pd.Timedelta(timedelta)
    else:
        raise TypeError(f"Valid types are: pd.Timedelta, datetime.timedelta and str that can be parsed as pd.Timedelta"
                        f"Provided variable is {type(timedelta)}")


def _validate_as_pytimedelta_or_none(
        var: Optional[Union[pd.Timedelta, datetime.timedelta, str]]
) -> Optional[datetime.timedelta]:
    """
    Checks if provided variable is either :py:class:`pandas.Timedelta`, :py:class:`datetime.timedelta` or
    a string that can be parsed by :py:class:`pandas.Timedelta` and converts it to :py:class:`datetime.timedelta`.

    If provided None, it returns None

    :param var: Variable to be checked
    :type var: Optional[Union[pd.Timedelta, datetime.timedelta, str]]
    :return: Validated timedelta or None
    :rtype: Optional[datetime.timedelta]
    """
    if var is not None:
        return _validate_timedelta_as_pytimedelta(var)
    else:
        return None


def _validate_timestamp_as_pydatetime(
    time_obj: Union[pd.Timestamp, datetime.datetime, np.datetime64, str]
) -> datetime.datetime:
    """
    Takes a time object and converts it to a pd.Timestamp if originally it was either datetime.datetime,
    np.datetime64 or pd.Timestamp

    Checks if provided variable is either :py:class:`pandas.Timestamp`, :py:class:`datetime.datetime`,
    :py:class:`np.datetime64` or a string that can be parsed by :py:class:`pandas.Timestamp`
    and converts it to :py:class:`datetime.datetime`.

    :param time_obj: Time object to be validated
    :type time_obj: Union[pd.Timestamp, datetime.datetime, np.datetime64, str]
    :return: Validated Timestamp
    :rtype: pd.Timestamp
    """
    if isinstance(time_obj, pd.Timestamp):
        return time_obj.to_pydatetime()
    elif isinstance(time_obj, np.datetime64):
        return pd.Timestamp(time_obj).to_pydatetime()
    elif isinstance(time_obj, datetime.datetime):
        return time_obj
    elif isinstance(time_obj, str):
        return _validate_timestamp_as_pydatetime(pd.Timestamp(time_obj))
    else:
        raise TypeError(f"Valid types are: pd.Timestamp, datetime.datetime or np.datetime64 "
                        f"or str that can be casted to pd.Timestamp. "
                        f"Provided variable is {type(time_obj)}")


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
    elif isinstance(time_obj, (datetime.datetime, np.datetime64, str)):
        return pd.Timestamp(time_obj)
    else:
        raise TypeError(f"Valid types are: pd.Timestamp, datetime.datetime or np.datetime64 "
                        f"or str that can be casted to pd.Timestamp. "
                        f"Provided variable is {type(time_obj)}")


def validate_to_tuple(
        val: Union[Tuple[Any], Any],
        accepted_type: Type,
) -> Tuple:
    """
    Method that checks if provided argument is a str, int or float or a tuple and returns
    the same thing but converted to a single element tuple

    :param val: Value to be validated
    :type val: Union[Tuple, str, int, float]
    :param accepted_type: Type to validate val against
    :type accepted_type: Type
    :return: Input tuple or a single element tuple with input val
    :rtype: Tuple
    """
    if isinstance(val, accepted_type):
        return (val,)
    elif isinstance(val, tuple):
        validate_uniformity_of_tuple(val=val, accepted_type=accepted_type)
        return val
    else:
        raise ValueError(
            f"Expecting a tuple or a single value of type {accepted_type}. Provided value was {type(val)}"
        )


def validate_uniformity_of_tuple(
        val: Tuple[Any, ...],
        accepted_type: Type,
        raise_errors: bool = True,
) -> bool:
    """
    Checks if all elements of provided tuple are of the same type.
    It can raise error or return False in case of negative validation.
    Returns true if tuple is uniform.

    :param val: Tuple to be checked for uniformity
    :type val: Tuple[Any, ...]
    :param accepted_type: Accepted type
    :type accepted_type: Type
    :param raise_errors: If errors should be raised
    :type raise_errors: bool
    :return: If provided tuple is uniform.
    :rtype: bool
    :raises: ValueError
    """

    types: List[Type] = []

    for item in val:
        types.append(type(item))
        if not isinstance(item, accepted_type):
            if raise_errors:
                raise ValueError(f'Values inside of provided tuple should be of type: {accepted_type}. '
                                 f'Value {item} is of type {type(item)}. ')
            else:
                return False

    if not len(list(set(types))) == 1:
        if raise_errors:
            raise ValueError(f"Type of values inside of tuple should be uniform. "
                             f"Inside of tuple {val} there were types: {set(types)}")
        else:
            return False

    return True


def validate_exactly_one_argument_provided(
        first: Optional[Any],
        second: Optional[Any],
) -> bool:
    """
    Method that checks if exactly one of provided arguments is not None.

    :param first: First value to check
    :type first: Optional[Any]
    :param second: Second value to check
    :type second: Optional[Any]
    :return: True if only one value is not None
    :rtype: bool
    :raises: ValueError
    """

    if (second is None and first is None) or (second is not None and first is not None):
        raise ValueError('There has to be exactly one argument provided.')
    else:
        return True


def validate_maximum_one_argument_provided(
        first: Optional[Any],
        second: Optional[Any],
) -> bool:
    """
    Method that checks if maximum one of provided arguments is not None.

    :param first: First value to check
    :type first: Optional[Any]
    :param second: Second value to check
    :type second: Optional[Any]
    :return: True if one or none of provided arguments is None
    :rtype: bool
    :raises: ValueError
    """

    if second is not None and first is not None:
        raise ValueError('There has to be maximum one argument provided.')
    else:
        return True

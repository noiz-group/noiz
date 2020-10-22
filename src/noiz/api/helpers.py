from typing import Iterable, Union, List

from noiz.models import Timespan, Component


def extract_object_ids(instances: Iterable[Union[Timespan, Component]]) -> \
        List[int]:
    """
    Extracts parameter .id from all provided instances of objects. It can either be a single object or iterbale of them.
    :param instances: instances of objects to be checked
    :type instances:
    :return: ids of objects
    :rtype: List[int]
    """
    if not isinstance(instances, Iterable):
        instances = list(instances)
    ids = [x.id for x in instances]
    return ids


def validate_to_tuple(val):
    """
    Method that checks if provided argument is a str, int or float or a tuple and returns
    the same thing but converted to a single element tuple

    :param val: Value to be validated
    :type val: Union[Tuple, str, int, float]
    :return: Input tuple or a single element tuple with input val
    :rtype: Tuple
    """
    if isinstance(val, str):
        return (val,)
    elif isinstance(val, int):
        return (val,)
    elif isinstance(val, float):
        return (val,)
    elif isinstance(val, tuple):
        return val
    else:
        raise ValueError(
            f"Expecting a tuple of values or a str, int or float. Provided value was {type(val)}"
        )

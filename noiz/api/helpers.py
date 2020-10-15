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


def validate_tuple_str(val):
    """
    Method that checks if provided argument is a string or a tuple and returnes
    the same thing but converted to tuple of str
    :param val:
    :type val:
    :return:
    :rtype:
    """
    if isinstance(val, str):
        return (val,)
    if isinstance(val, tuple):
        return val
    else:
        raise ValueError(
            f"Expecting a tuple of strings or a string. Provided value was {type(val)}"
        )

from typing import Iterable, Union, List, Tuple, Type, Collection, Any

from noiz.models import Timespan, Component


def extract_object_ids(
        instances: Iterable[Union[Timespan, Component]],
) -> List[int]:
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


def validate_to_tuple(
        val: Union[Tuple[Any], Any],
        accepted_type: Type,
) -> Tuple:
    """
    Method that checks if provided argument is a str, int or float or a tuple and returns
    the same thing but converted to a single element tuple

    :param val: Value to be validated
    :type val: Union[Tuple, str, int, float]
    :param accepted_type:
    :type accepted_type:
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
            f"Expecting a tuple of values or a str, int or float. Provided value was {accepted_type(val)}"
        )


def validate_uniformity_of_tuple(
        val: Tuple[Any, ...],
        accepted_type: Type,
        raise_errors: bool = True,
) -> bool:

    types: List[Type] = []

    for i in val:
        if not isinstance(i, (str, int, float)):
            if raise_errors:
                raise ValueError(f'Values inside of provided tuple should be of type: str, int, float. '
                                 f'Value {i} is of type {accepted_type(i)}. ')
            else:
                return False
    if not len(list(set(types))) == 1:
        if raise_errors:
            raise ValueError(f"Type of values inside of tuple should be uniform. "
                             f"Inside of tuple {val} there were types: {set(types)}")
        else:
            return False

    return True

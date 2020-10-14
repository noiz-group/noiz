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
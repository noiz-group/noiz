from typing import List, Iterable, Union, Sized

from collections import Collection

from noiz.models import Component, Datachunk, Timespan


def fetch_datachunks_for_timespan(
    timespans: Union[Timespan, Iterable[Timespan]]
) -> List[Datachunk]:
    """
    Fetches all datachunks associated with provided timespans. Timespan can be a single one or Iterable of timespans.
    :param timespans: Instances of timespans to be checked
    :type timespans: Union[Timespan, Iterable[Timespan]]
    :return: List of Datachunks
    :rtype: List[Datachunk]
    """
    timespan_ids = extract_object_ids(timespans)
    ret = Datachunk.query.filter(Datachunk.timespan_id.in_(timespan_ids)).all()
    return ret


def count_datachunks_for_timespans(
    components: Collection[Component], timespans: Collection[Timespan]
) -> int:
    """
    Counts number of datachunks for all provided components associated with all provided timespans.

    :param components: Components to be checked
    :type components: Iterable[Component]
    :param timespans: Timespans to be checked
    :type timespans: Iterable[Timespan]
    :return: Count fo datachunks
    :rtype: int
    """
    timespan_ids = extract_object_ids(timespans)
    component_ids = extract_object_ids(components)
    count = Datachunk.query.filter(
        Datachunk.component_id.in_(component_ids),
        Datachunk.timespan_id.in_(timespan_ids),
    ).count()
    return count


def extract_object_ids(instances: Iterable[Union[Timespan, Component]]) -> List[int]:
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

from typing import List, Iterable

from noiz.models import Component, Datachunk, Timespan


def fetch_datachunks_for_timespan(timespan: Timespan) -> List[Datachunk]:
    """

    :param timespan:
    :type timespan:
    :return:
    :rtype:
    """
    ret = Datachunk.query.filter(Datachunk.timespan_id == timespan.id).all()
    return ret


def count_datachunks_for_timespans(
    components: Iterable[Component], timespans: Iterable[Timespan]
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
    timespan_ids = [x.id for x in timespans]
    component_ids = [x.id for x in components]
    count = Datachunk.query.filter(
        Datachunk.component_id.in_(component_ids),
        Datachunk.timespan_id.in_(timespan_ids),
    ).count()
    return count

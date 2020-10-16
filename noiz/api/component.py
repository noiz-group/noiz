import logging

from typing import List, Iterable, Optional, Collection

from noiz.api.helpers import validate_tuple_str
from noiz.models import Component

log = logging.getLogger("noiz.api")


def fetch_components_by_id(component_ids: Collection[int]) -> List[Component]:
    """
    DEPRECATED. Use noiz.api.component.fetch_components instead

    Fetches components based on their ids

    :param component_ids: IDs of components to be fetched
    :type component_ids: Iterable[int]
    :return: List of all fetched components
    :rtype: List[Component]
    """
    log.warning("DEPRACATION. Method depracted. "
                "Use noiz.api.component.fetch_components")
    return fetch_components(component_ids=component_ids)


def fetch_components(
    networks: Optional[Collection[str]] = None,
    stations: Optional[Collection[str]] = None,
    components: Optional[Collection[str]] = None,
    component_ids: Optional[Collection[int]] = None,
) -> List[Component]:
    """
    Fetches components based on provided network codes, station codes and component codes.
    If none of the arguments are provided, will raise ValueError.

    :param networks: Networks of components to be fetched
    :type networks: Optional[Collection[str]]
    :param stations: Stations of components to be fetched
    :type stations: Optional[Collection[str]]
    :param components: Component letters to be fetched
    :type components: Optional[Collection[str]]
    :param components: Ids of components objects to be fetched
    :type components: Optional[Collection[int]]
    :return: Component fetched based on provided values.
    :rtype: List[Component]
    :raises: ValueError
    """

    filters = []

    if networks is not None:
        filters.append(Component.network.in_(validate_tuple_str(networks)))
    if stations is not None:
        filters.append(Component.station.in_(validate_tuple_str(stations)))
    if components is not None:
        filters.append(Component.component.in_(validate_tuple_str(components)))
    if component_ids is not None:
        filters.append(Component.id.in_(component_ids))

    if len(filters) == 0:
        filters.append(True)

    return Component.query.filter(*filters).all()

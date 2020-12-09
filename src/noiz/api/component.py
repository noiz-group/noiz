import logging

from typing import List, Iterable, Optional, Collection, Union

from noiz.api.helpers import validate_to_tuple
from noiz.models.component import Component

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
    networks: Optional[Union[Collection[str], str]] = None,
    stations: Optional[Union[Collection[str], str]] = None,
    components: Optional[Union[Collection[str], str]] = None,
    component_ids: Optional[Union[Collection[int], int]] = None,
) -> List[Component]:
    """
    Fetches components based on provided network codes, station codes and component codes.
    If none of the arguments are provided, will raise ValueError.

    :param networks: Networks of components to be fetched
    :type networks: Optional[Union[Collection[str], str]]
    :param stations: Stations of components to be fetched
    :type stations: Optional[Union[Collection[str], str]]
    :param components: Component letters to be fetched
    :type components: Optional[Union[Collection[str], str]]
    :param components: Ids of components objects to be fetched
    :type components: Optional[Union[Collection[int], int]]
    :return: Component fetched based on provided values.
    :rtype: List[Component]
    :raises: ValueError
    """

    filters = []

    if networks is not None:
        filters.append(Component.network.in_(validate_to_tuple(networks, accepted_type=str)))
    if stations is not None:
        filters.append(Component.station.in_(validate_to_tuple(stations, accepted_type=str)))
    if components is not None:
        filters.append(Component.component.in_(validate_to_tuple(components, accepted_type=str)))
    if component_ids is not None:
        filters.append(Component.id.in_(validate_to_tuple(component_ids, accepted_type=int)))

    if len(filters) == 0:
        filters.append(True)

    return Component.query.filter(*filters).all()

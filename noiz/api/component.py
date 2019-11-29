from typing import List, Iterable, Tuple, Optional

from noiz.models import Component


def validate_tuple_str(val):
    if isinstance(val, str):
        return (val,)
    if isinstance(val, tuple):
        return val
    else:
        raise ValueError(
            f"Expecting a tuple of strings or a string. Provided value was {type(val)}"
        )


def fetch_components_by_id(component_ids: Iterable[int]) -> List[Component]:
    """
    Fetches components based on their ids
    :param component_ids: IDs of components to be fetched
    :type component_ids: Iterable[int]
    :return: List of all fetched components
    :rtype: List[Component]
    """
    ret = Component.query.filter(Component.id.in_(component_ids)).all()
    return ret


def fetch_components(
    networks: Optional[Tuple[str]],
    stations: Optional[Tuple[str]],
    components: Optional[Tuple[str]],
) -> List[Component]:
    """
    Fetches components based on provided network codes, station codes and component codes.
    :param networks: Networks of components
    :type networks: Optional[Tuple[str]]
    :param stations: Stations of components
    :type stations: Optional[Tuple[str]]
    :param components: Components to be fetched
    :type components: Optional[Tuple[str]]
    :return: Component fetched based on provided values.
    :rtype: List[Component]
    """

    filters = []

    if networks is not None:
        filters.append(Component.network.in_(validate_tuple_str(networks)))
    if stations is not None:
        filters.append(Component.station.in_(validate_tuple_str(stations)))
    if networks is not None:
        filters.append(Component.component.in_(validate_tuple_str(components)))

    ret = Component.query.filter(*filters).all()
    return ret

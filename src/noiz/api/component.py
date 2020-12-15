from flask import current_app
from loguru import logger
from pathlib import Path
from typing import List, Iterable, Optional, Collection, Union

from noiz.api.helpers import validate_to_tuple
from noiz.database import db
from noiz.models.component import Component
from noiz.processing.component import parse_inventory_for_single_component_db_entries


def fetch_components_by_id(component_ids: Collection[int]) -> List[Component]:
    """
    DEPRECATED. Use noiz.api.component.fetch_components instead

    Fetches components based on their ids

    :param component_ids: IDs of components to be fetched
    :type component_ids: Iterable[int]
    :return: List of all fetched components
    :rtype: List[Component]
    """
    logger.warning("DEPRACATION. Method depracted. "
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


def parse_inventory_insert_stations_and_components_into_db(inventory_path: Path, filetype: str = "STATIONXML") -> None:

    inventory_dir = get_processed_inventory_dir()

    components, devices = parse_inventory_for_single_component_db_entries(
        inventory_path=inventory_path,
        inventory_dir=inventory_dir,
        filetype=filetype,
    )

    logger.info("Finished parsing inventory")

    logger.info("Trying to add devices to DB")
    db.session.add_all(devices)
    logger.info("Commiting devices in db")
    db.session.commit()

    logger.info("Adding components to db")
    db.session.add_all(components)
    logger.info("Commiting components to db")
    db.session.commit()
    logger.info('Success')
    return


def get_processed_inventory_dir() -> Path:
    """
    Prepares directory inside of a processed_data_dir that will hold the processed inventory files.

    :return: Processed inventory dir
    :rtype: Path
    """
    processed_data_dir = Path(current_app.noiz_config.get("processed_data_dir")).absolute()
    inventory_dir = processed_data_dir.joinpath("inventory")
    inventory_dir.mkdir(exist_ok=True)
    return inventory_dir

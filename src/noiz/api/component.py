# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from flask import current_app
from loguru import logger
from pathlib import Path
from typing import List, Iterable, Optional, Collection, Union

import datetime
import pandas as pd
import numpy as np
from numpy import deprecate_with_doc
from obspy import UTCDateTime

from noiz.validation_helpers import validate_to_tuple, validate_timestamp_as_pydatetime
from noiz.database import db
from noiz.models.component import Component
from noiz.processing.component import parse_inventory_for_single_component_db_entries
from noiz.globals import PROCESSED_DATA_DIR


@deprecate_with_doc(msg="This function is deprecated. Use noiz.api.component.fetch_components instead.")
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
    starttime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
    endtime: Optional[Union[pd.Timestamp, datetime.datetime, np.datetime64, UTCDateTime, str]] = None,
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
    if starttime is not None:
        filters.append(validate_timestamp_as_pydatetime(starttime) <= Component.end_date)
    if endtime is not None:
        filters.append(Component.start_date <= validate_timestamp_as_pydatetime(endtime))
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
    processed_data_dir = Path(PROCESSED_DATA_DIR).absolute()
    inventory_dir = processed_data_dir.joinpath("inventory")
    inventory_dir.mkdir(exist_ok=True)
    return inventory_dir

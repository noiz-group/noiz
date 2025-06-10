# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import obspy
from pathlib import Path

from loguru import logger
from typing import Iterable, Dict, List, Tuple

from obspy import Inventory
from obspy.core.inventory import Network, Station, Channel

from noiz.models.component import Component, ComponentFile, Device
from noiz.processing.path_helpers import _assembly_single_component_invenontory_path


def _assembly_single_component_inventory(
    inventory_to_clean: Inventory,
    network_to_clean: Network,
    station_to_clean: Station,
    components: Iterable[Channel],
) -> Inventory:
    """
    Makes a new inventory object on basis of provided sub objects
    :param inventory_to_clean:
    :type inventory_to_clean: Inventory
    :param network_to_clean:
    :type network_to_clean: Network
    :param station_to_clean:
    :type station_to_clean: Station
    :param components:
    :type components: Iterable[Channel]
    :return:
    :rtype: Inventory
    """
    network_new = network_to_clean.copy()
    station_new = station_to_clean.copy()
    station_new.channels = components
    network_new.stations = [station_new]
    inventory_single_component = Inventory(networks=[network_new], source=inventory_to_clean.source)
    return inventory_single_component


def divide_channels_by_component(
    channels: Iterable[Channel],
) -> Dict[str, List[Channel]]:
    """
    Groups provided channels by component letter (last letter of channel code)

    :param channels: Iterable with channels to be groupped
    :type channels: Iterable[Channel]
    :return: Dict where key is component letter and items are iterables with Channels
    :rtype: Dict[str: Iterable[Channel]]
    """

    logger.info("Parsing channels to find components")
    components: Dict[str, List[Channel]] = {}
    for channel in channels:
        component = channel.code[-1]
        logger.info(f"Found component {component}")
        if components.get(component) is None:
            components[component] = []
        components[component].append(channel)
    return components


def read_inventory(filepath: Path, filetype: str = "stationxml") -> Inventory:
    """
    Wrapper for path-like objects to be read passed to :func:`~obspy.read_inventory`

    :param filepath: Path to an inventory file
    :type filepath: Path
    :param filetype: type of inventory file
    :type filetype: str
    :return: Inventory object
    :rtype: obspy.Inventory
    """
    return obspy.read_inventory(str(filepath), filetype)


def parse_inventory_for_single_component_db_entries(
    inventory_path: Path,
    inventory_dir: Path,
    filetype: str = "STATIONXML",
) -> Tuple[Tuple[Component, ...], Tuple[Device, ...]]:
    """
    Reads provided inventory file and tries to split it into a single component files that will be saved
    inside the provided inventory_dir.
    It creates a database object of Component and Component files ready to be stored in the DB.

    :param inventory_path:
    :type inventory_path: Path
    :param inventory_dir:
    :type inventory_dir: Path
    :param filetype: Filetype of the inventory. Passed directly to :func:`~obspy.read_inventory`
    :type filetype: str
    :return: Tuple of Component objects ready to be added to database
    :rtype: Tuple[Component]
    """
    components_to_commit = []
    devices_to_commit = []

    inventory = obspy.read_inventory(str(inventory_path), format=filetype)

    logger.info("Parsing inventory")

    for network in inventory:
        logger.info(f"Found network {network.code}")
        for station in network:
            logger.info(f"Found station {station.code}")
            logger.info(f"Creating Device for {network.code}.{station.code}")
            device = Device(
                network=network.code,
                station=station.code,
            )
            logger.info(f"Created Device object {device}")
            devices_to_commit.append(device)

            logger.info("Dividing channels by component")
            components = divide_channels_by_component(station.channels)

            for component, channels in components.items():
                logger.info(f"Creating inventory for component {component}")
                inventory_single_component = _assembly_single_component_inventory(
                    inventory, network, station, channels
                )

                single_cmp_inv_path = _assembly_single_component_invenontory_path(
                    network, station, component, inventory_dir
                )

                inventory_single_component.write(str(single_cmp_inv_path), format="stationxml")
                logger.info("Saving of the inventory file successful!")

                cmp_file = ComponentFile(filepath=str(single_cmp_inv_path))

                if station.start_date:
                    startdate = station.start_date
                else:
                    ns = 0
                    for channel in station:
                        if channel.start_date:
                            if ns == 0:
                                startdate = channel.start_date
                                ns += 1
                            else:
                                delta = startdate - channel.start_date
                                if delta >= 0:
                                    startdate = channel.start_date
                        else:
                            startdate = network.start_date

                if station.end_date:
                    enddate = station.end_date
                else:
                    ns = 0
                    for channel in station:
                        if channel.end_date:
                            if ns == 0:
                                enddate = channel.end_date
                                ns += 1
                            else:
                                delta = enddate - channel.end_date
                                if delta < 0:
                                    enddate = channel.end_date
                        else:
                            enddate = startdate + 10 * 365 * 24 * 60 * 60

                db_component = Component(
                    network=network.code,
                    station=station.code,
                    component=component,
                    lat=station.latitude,
                    lon=station.longitude,
                    elevation=station.elevation,
                    inventory_filepath=str(single_cmp_inv_path),
                    component_file=cmp_file,
                    device=device,
                    start_date=startdate,
                    end_date=enddate,
                )

                logger.info(f"Created Component object {db_component}")
                components_to_commit.append(db_component)
                logger.info(f"Finished with component {component}")

    return tuple(components_to_commit), tuple(devices_to_commit)

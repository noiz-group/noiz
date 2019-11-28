import obspy

from obspy import Inventory
from obspy.core.inventory.network import Network
from obspy.core.inventory.station import Station
from obspy.core.inventory.channel import Channel
from pathlib import Path
from typing import Iterable, Dict, List

import logging


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
    inventory_single_component = Inventory(
        networks=[network_new], source=inventory_to_clean.source
    )
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

    logging.info("Parsing channels to find components")
    components: Dict[str, List[Channel]] = {}
    for channel in channels:
        component = channel.code[-1]
        logging.info(f"Found component {component}")
        if components.get(component) is None:
            components[component] = []
        components[component].append(channel)
    return components


def _assembly_stationxml_filename(
    network: Network, station: Station, component: str
) -> str:
    """"""
    return f"inventory_{network.code}.{station.code}.{component}.xml"


def read_inventory(filepath: Path, filetype: str = "stationxml") -> Inventory:
    """
    Wrapper for path-like objects to be read passed to obspy.read_inventory
    :param filepath: Path to an inventory file
    :type filepath: Path
    :param filetype: type of inventory file
    :type filetype: str
    :return: Inventory object
    :rtype: obspy.Inventory
    """
    return obspy.read_inventory(str(filepath), filetype)

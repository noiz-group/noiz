import obspy
from pathlib import Path

import logging


def _assembly_single_component_inventory(
    inventory_to_clean, network_to_clean, station_to_clean, components
):
    network_new = network_to_clean.copy()
    station_new = station_to_clean.copy()
    station_new.channels = components
    network_new.stations = [station_new]
    inventory_single_component = obspy.Inventory(
        networks=[network_new], source=inventory_to_clean.source
    )
    return inventory_single_component


def divide_channels_by_component(channels):
    logging.info("Parsing channels to find components")
    components = {}
    for channel in channels:
        component = channel.code[-1]
        logging.info(f"Found component {component}")
        if components.get(component) is None:
            components[component] = []
        components[component].append(channel)
    return components


def _assembly_stationxml_filename(network, station, component):
    return f"inventory_{network.code}.{station.code}.{component}.xml"


def read_inventory(filepath: Path, filetype: str = "stationxml") -> obspy.Inventory:
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

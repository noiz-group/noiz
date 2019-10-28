import obspy
from pathlib import Path

from noiz.models import Component
from noiz.database import db
import logging

logger = logging.getLogger()


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
    logger.info("Parsing channels to find components")
    components = {}
    for channel in channels:
        component = channel.code[-1]
        logger.info(f"Found component {component}")
        if components.get(component) is None:
            components[component] = []
        components[component].append(channel)
    return components


def _assembly_stationxml_filename(network, station, component):
    return f"inventory_{network.code}.{station.code}.{component}.xml"


def parse_inventory_for_single_component_db_entries(inventory, inventory_dir):
    objects_to_commit = []
    added_filepaths = []

    logger.info("Parsing inventory")

    for network in inventory:
        logger.info(f"Found network {network.code}")
        for station in network:
            logger.info(f"Found station {station.code}")
            components = divide_channels_by_component(station.channels)

            for component, channels in components.items():
                logger.info(f"Creating inventory for component {component}")
                inventory_single_component = _assembly_single_component_inventory(
                    inventory, network, station, channels
                )
                filename = _assembly_stationxml_filename(network, station, component)

                inventory_filepath = inventory_dir.joinpath(filename)
                logger.info(
                    f"Inventory for component {component} will be saved to {inventory_filepath}"
                )

                if not inventory_filepath.exists():
                    added_filepaths.append(inventory_filepath)

                else:
                    logger.warning("The inventory_file_exists")

                inventory_single_component.write(
                    str(inventory_filepath), format="stationxml"
                )
                logger.info(f"Saving of the inventory file successful!")

                db_component = Component(
                    network=network.code,
                    station=station.code,
                    component=component,
                    lat=station.latitude,
                    lon=station.longitude,
                    elevation=station.elevation,
                    inventory_filepath=str(inventory_filepath),
                )

                logger.info(f"Created Component object {db_component}")
                objects_to_commit.append(db_component)
                logger.info(f"Finished with component {component}")

    return objects_to_commit, added_filepaths


def parse_inventory_insert_stations_and_components_into_db(app, inventory):
    processed_data_dir = Path(app.noiz_config.get("processed_data_dir"))
    inventory_dir = processed_data_dir.joinpath("inventory")
    inventory_dir.mkdir(exist_ok=True)

    objects_to_commit, added_filepaths = parse_inventory_for_single_component_db_entries(
        inventory, inventory_dir
    )
    with app.app_context() as ctx:
        for obj in objects_to_commit:
            db.session.merge(obj)
        db.session.commit()
    return


def read_inventory(filepath: Path, filetype: str) -> obspy.Inventory:
    return obspy.read_inventory(str(filepath), filetype)

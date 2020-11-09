from pathlib import Path
import logging

from noiz.database import db
from noiz.models.component import Component
from noiz.processing.inventory import (
    divide_channels_by_component,
    _assembly_single_component_inventory,
    _assembly_stationxml_filename,
)


def parse_inventory_for_single_component_db_entries(inventory, inventory_dir):
    objects_to_commit = []
    added_filepaths = []

    logging.info("Parsing inventory")

    for network in inventory:
        logging.info(f"Found network {network.code}")
        for station in network:
            logging.info(f"Found station {station.code}")
            components = divide_channels_by_component(station.channels)

            for component, channels in components.items():
                logging.info(f"Creating inventory for component {component}")
                inventory_single_component = _assembly_single_component_inventory(
                    inventory, network, station, channels
                )
                filename = _assembly_stationxml_filename(network, station, component)

                inventory_filepath = inventory_dir.joinpath(filename)
                logging.info(
                    f"Inventory for component {component} will be saved to {inventory_filepath}"
                )

                if not inventory_filepath.exists():
                    added_filepaths.append(inventory_filepath)

                else:
                    logging.warning("The inventory_file_exists")

                inventory_single_component.write(
                    str(inventory_filepath), format="stationxml"
                )
                logging.info("Saving of the inventory file successful!")

                db_component = Component(
                    network=network.code,
                    station=station.code,
                    component=component,
                    lat=station.latitude,
                    lon=station.longitude,
                    elevation=station.elevation,
                    inventory_filepath=str(inventory_filepath),
                )

                logging.info(f"Created Component object {db_component}")
                objects_to_commit.append(db_component)
                logging.info(f"Finished with component {component}")

    return objects_to_commit, added_filepaths


def parse_inventory_insert_stations_and_components_into_db(app, inventory):
    processed_data_dir = Path(app.noiz_config.get("processed_data_dir"))
    inventory_dir = processed_data_dir.joinpath("inventory")
    inventory_dir.mkdir(exist_ok=True)

    (
        objects_to_commit,
        added_filepaths,
    ) = parse_inventory_for_single_component_db_entries(inventory, inventory_dir)
    for obj in objects_to_commit:
        db.session.merge(obj)
    db.session.commit()
    return

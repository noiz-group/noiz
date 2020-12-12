from flask import current_app
from pathlib import Path

from noiz.database import db
from noiz.processing.inventory import parse_inventory_for_single_component_db_entries


def parse_inventory_insert_stations_and_components_into_db(inventory_path: Path) -> None:

    inventory_dir = get_processed_inventory_dir()

    objects_to_commit, added_filepaths = parse_inventory_for_single_component_db_entries(
        inventory_path=inventory_path,
        inventory_dir=inventory_dir,
    )

    for obj in objects_to_commit:
        db.session.merge(obj)
    db.session.commit()
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

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from loguru import logger
from obspy.core.inventory import Network, Station
import os
from pathlib import Path
from typing import Union, Optional

from noiz.models import Component, Timespan


def assembly_preprocessing_filename(
        component: Component,
        timespan: Timespan,
        count: int = 0
) -> str:
    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    fname = ".".join([
        component.network,
        component.station,
        component.component,
        year,
        doy_time,
        str(count)
    ])

    return fname


def assembly_sds_like_dir(component: Component, timespan: Timespan) -> Path:
    """
    Asembles a Path object in a SDS manner. Object consists of year/network/station/component codes.

    Warning: The component here is a single letter component!

    :param component: Component object containing information about used channel
    :type component: Component
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return:  Pathlike object containing SDS-like directory hierarchy.
    :rtype: Path
    """
    return (
        Path(str(timespan.starttime.year))
        .joinpath(component.network)
        .joinpath(component.station)
        .joinpath(component.component)
    )


def assembly_filepath(
    processed_data_dir: Union[str, Path],
    processing_type: Union[str, Path],
    filepath: Union[str, Path],
) -> Path:
    """
    Assembles a filepath for processed files.
    It assembles a root processed-data directory with processing type and a rest of a filepath.

    :param processed_data_dir:
    :type processed_data_dir: Path
    :param processing_type:
    :type processing_type: Path
    :param filepath:
    :type filepath: Path
    :return:
    :rtype: Path
    """
    return Path(processed_data_dir).joinpath(processing_type).joinpath(filepath)


def parent_directory_exists_or_create(filepath: Path) -> bool:
    """
    Checks if directory of a filepath exists. If doesn't, it creates it.
    Returns bool that indicates if the directory exists in the end. Should be always True.

    :param filepath: Path to the file you want to save and check it the parent directory exists.
    :type filepath: Path
    :return: If the directory exists in the end.
    :rtype: bool
    """
    directory = filepath.parent
    logger.debug(f"Checking if directory {directory} exists")
    if not directory.exists():
        logger.debug(f"Directory {directory} does not exists, trying to create.")
        directory.mkdir(parents=True, exist_ok=True)
    return directory.exists()


def directory_exists_or_create(dirpath: Path) -> bool:
    """
    Checks if directory of a filepath exists. If doesn't, it creates it.
    Returns bool that indicates if the directory exists in the end. Should be always True.

    :param dirpath: Path to the directory tree to be created
    :type dirpath: Path
    :return: If the directory exists in the end.
    :rtype: bool
    """
    logger.debug(f"Checking if directory {dirpath} exists")
    if not dirpath.exists():
        logger.debug(f"Directory {dirpath} does not exists, trying to create.")
        dirpath.mkdir(parents=True, exist_ok=True)
    return dirpath.exists()


def increment_filename_counter(filepath: Path, extension: bool) -> Path:
    """
    Takes a filepath with int as suffix and returns a non existing filepath
     that has next free int value as last element before the extension.

    :param filepath: Filepath to find next free path for
    :type filepath: Path
    :param extension: If the filename uses a standard extension such as ".npz" or ".xml"
    :type extension: bool
    :return: Free filepath
    :rtype: Path
    :raises: ValueError
    """

    while True:
        if not filepath.exists():
            return filepath

        name_parts = filepath.name.split(os.extsep)

        try:
            if extension:
                name_parts[-2] = str(int(name_parts[-2])+1)
            else:
                name_parts[-1] = str(int(name_parts[-1])+1)
        except ValueError:
            raise ValueError(f"The filepath's {filepath} suffix cannot be casted to int. "
                             f"Check if filename has extension and try to pass "
                             f"argument extension {not extension}")

        filepath = filepath.with_name(os.extsep.join(name_parts))


def _assembly_stationxml_filename(
    network: Network, station: Station, component: str, counter: int = 0
) -> str:
    """
    Assembles a filename of a single component StationXML file.

    :param network: Network object from inventory being parsed
    :type network: Network
    :param station: Station object from inventory being parsed
    :type station: Station
    :param component: Component name (single letter)
    :type component: str
    :param counter: Value that will be added on the end of filename to keep track of new version of the file
    :type counter: int
    :return: Filename that can be tried to name file with
    :rtype: str
    """
    return f"inventory_{network.code}.{station.code}.{component}.xml.{counter}"


def _assembly_single_component_invenontory_path(
        network: Network,
        station: Station,
        component: str,
        inventory_dir: Path
) -> Path:
    """
    Creates a filepath for a new inventory file based on standard schema.
    If there already exists a file with that name, it will increment counter on the end of name until it find next
    free filepath.

    :param network: Network object from inventory being parsed
    :type network: Network
    :param station: Station object from inventory being parsed
    :type station: Station
    :param component: Component name (single letter)
    :type component: str
    :param inventory_dir: Base directory where inventory files will be stored
    :type inventory_dir: Path
    :return: Filepath to new inventory file
    :rtype: Path
    """

    filename = _assembly_stationxml_filename(network, station, component, counter=0)

    single_cmp_inv_path = inventory_dir.joinpath(filename)

    if single_cmp_inv_path.exists():
        logger.info(f'Filepath {single_cmp_inv_path} exists. '
                    f'Trying to find next free one.')
        single_cmp_inv_path = increment_filename_counter(filepath=single_cmp_inv_path, extension=False)
        logger.info(f"Free filepath found. "
                    f"Inventory will be saved to {single_cmp_inv_path}")
    logger.info(
        f"Inventory for component {component} will be saved to {single_cmp_inv_path}"
    )
    return single_cmp_inv_path

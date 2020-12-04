from typing import Union

from pathlib import Path

from noiz.models import Component, Timespan
from noiz.processing.datachunk import log


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


def directory_exists_or_create(filepath: Path) -> bool:
    """
    Checks if directory of a filepath exists. If doesnt, it creates it.
    Returns bool that indicates if the directory exists in the end. Should be always True.

    :param filepath: Path to the file you want to save and check it the parent directory exists.
    :type filepath: Path
    :return: If the directory exists in the end.
    :rtype: bool
    """
    directory = filepath.parent
    log.info(f"Checking if directory {directory} exists")
    if not directory.exists():
        log.info(f"Directory {directory} does not exists, trying to create.")
        directory.mkdir(parents=True)
    return directory.exists()


def increment_filename_counter(filepath: Path) -> Path:
    """
    Takes a filepath with int as suffix and returns a non existing filepath
     that has next free int value as suffix.

    :param filepath: Filepath to find next free path for
    :type filepath: Path
    :return: Free filepath
    :rtype: Path
    :raises: ValueError
    """

    while True:
        if not filepath.exists():
            return filepath

        suffix: str = filepath.suffix[1:]
        try:
            suffix_int: int = int(suffix)
        except ValueError:
            raise ValueError(f"The filepath's {filepath} suffix {suffix} "
                             f"cannot be casted to int")
        filepath = filepath.with_suffix(f".{suffix_int+1}")

from pathlib import Path

from loguru import logger
import obspy

from noiz.exceptions import MissingDataFileException, CorruptedMiniseedFileException
from noiz.processing.warning_handling import CatchWarningAsError


def _read_single_miniseed(
        filename: Path,
        format: str,
) -> obspy.Stream:

    if not Path(filename).exists():
        raise MissingDataFileException("Data file is missing")

    with CatchWarningAsError(
            warning_filter_action="error",
            warning_filter_message='(?s).* Data integrity check for Steim1 failed'
    ):
        try:
            return obspy.read(filename, format)
        except Warning:
            logger.warning("Data integrity check for Steim1 failed")
            raise CorruptedMiniseedFileException(f"File {filename} is corrupted. Steim1 integrity check failed.")

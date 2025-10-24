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

    # TODO: Parse MIME types from mseedindex (e.g., "application/vnd.fdsn.mseed;version=2")
    # into ObsPy-expected format names (e.g., "MSEED"). For now, let ObsPy auto-detect
    # the format by not passing the format parameter.
    # See: https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html

    with CatchWarningAsError(
        warning_filter_action="error", warning_filter_message="(?s).* Data integrity check for Steim1 failed"
    ):
        try:
            # Don't pass format parameter - let ObsPy auto-detect
            return obspy.read(filename)
        except Warning as e:
            logger.warning("Data integrity check for Steim1 failed")
            raise CorruptedMiniseedFileException(
                f"File {filename} is corrupted. Steim1 integrity check failed."
            ) from e

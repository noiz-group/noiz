from pathlib import Path
from typing import Optional

from loguru import logger
import obspy

try:
    from obspy.io.mseed.core import _read_mseed as _obspy_read_mseed
except ImportError:
    _obspy_read_mseed = None

from noiz.exceptions import MissingDataFileException, CorruptedMiniseedFileException
from noiz.processing.warning_handling import CatchWarningAsError


def _read_single_miniseed(
    filename: Path,
    format: Optional[str],
) -> obspy.Stream:
    if not Path(filename).exists():
        raise MissingDataFileException("Data file is missing")

    with CatchWarningAsError(
        warning_filter_action="error", warning_filter_message="(?s).* Data integrity check for Steim1 failed"
    ):
        try:
            if format is not None and format.upper() == "MSEED" and _obspy_read_mseed is not None:
                return _obspy_read_mseed(str(filename))
            if format is None:
                return obspy.read(str(filename))
            return obspy.read(str(filename), format=format)
        except Warning as e:
            logger.warning("Data integrity check for Steim1 failed")
            raise CorruptedMiniseedFileException(
                f"File {filename} is corrupted. Steim1 integrity check failed."
            ) from e

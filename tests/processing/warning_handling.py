import warnings
from obspy.io.mseed import InternalMSEEDWarning
import pytest

from noiz.processing.warning_handling import CatchWarningAsError


def test_catching_integrity_check_warning():
    warning_message = (
        "InternalMSEEDWarning: EN_ES23_00_CHE_D: Warning: Data integrity check for Steim1 failed, "
        "Last sample=-4978, Xn=-4970"
    )

    with pytest.raises(Warning):
        with CatchWarningAsError(
            warning_filter_action="error", warning_filter_message="(?s).* Data integrity check for Steim1 failed"
        ):
            warnings.warn(warning_message, InternalMSEEDWarning, stacklevel=2)


def test_catching_custom_warning():
    warning_message = "That's a very important warning"

    with pytest.raises(Warning):
        with CatchWarningAsError(warning_filter_action="error", warning_filter_message="(?s).*warning(?s).*"):
            warnings.warn(warning_message, stacklevel=2)

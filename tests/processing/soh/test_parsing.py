from pathlib import Path
import pandas as pd
import pytest

from noiz.processing.soh.soh_column_names import SohType, SohInstrumentNames, load_parsing_parameters
from noiz.processing.soh.parsing import read_single_soh_miniseed


def test_read_single_soh_miniseed():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED.value)

    res = read_single_soh_miniseed(filepath=filepath, parsing_params=parsing_params)

    assert isinstance(res, pd.DataFrame)

from pathlib import Path
import pandas as pd
import pytest

from noiz.processing.soh.parsing_params import SohType, SohInstrumentNames, load_parsing_parameters, \
    _read_single_soh_miniseed_centaur


def test__read_single_soh_miniseed_centaur_all_channels_present():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_GPSTIME.value)

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 9)


def test__read_single_soh_miniseed_centaur_empty_channels():
    filepath = Path(__file__).parent.joinpath("data", "SI.SI09.D0.SOH_centaur-3_1492_20180616_040000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_GPSTIME.value)

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 9)

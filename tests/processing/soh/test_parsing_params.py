import pytest

import numpy as np
from pathlib import Path
import pandas as pd

from noiz.processing.soh.parsing_params import SohType, SohInstrumentNames, load_parsing_parameters, \
    _read_single_soh_miniseed_centaur, _postprocess_soh_miniseed_instrument_centaur


def test__read_single_soh_miniseed_gpstime_centaur_all_channels_present():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_GPSTIME.value)

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    expected_values = np.array([[0.00000000e+00, 1.00000000e+01, 9.00000000e+01,
                                 3.00000000e+00, 8.32400000e+03, 0.00000000e+00],
                                [0.00000000e+00, 1.00000000e+01, 9.00000000e+01,
                                 3.00000000e+00, 8.32700000e+03, 0.00000000e+00]])
    expected_index = pd.DatetimeIndex(['2017-11-25 10:00:00+00:00', '2017-11-25 10:30:00+00:00'],
                                      dtype='datetime64[ns, UTC]', freq=None)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 6)
    assert np.array_equal(res.values, expected_values)
    assert res.index == expected_index


def test__read_single_soh_miniseed_instrument_centaur_all_channels_present():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_INSTRUMENT.value)

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    expected_values = np.array([[13302., 89., 17000.],
                                [13369., 90., 17000.]])

    expected_index = pd.DatetimeIndex(['2017-11-25 10:00:00+00:00', '2017-11-25 10:30:00+00:00'],
                                      dtype='datetime64[ns, UTC]', freq=None)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 3)
    assert np.array_equal(res.values, expected_values)
    assert res.index == expected_index


def test__postprocess_soh_miniseed_instrument_centaur_all_channels_present():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_INSTRUMENT.value)

    part = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)
    res = _postprocess_soh_miniseed_instrument_centaur(df=part)

    expected_values = np.array([[13.302, 0.089, 17.],
                                [13.369, 0.09, 17.]])

    expected_index = pd.DatetimeIndex(['2017-11-25 10:00:00+00:00', '2017-11-25 10:30:00+00:00'],
                                      dtype='datetime64[ns, UTC]', freq=None)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 3)
    assert np.array_equal(res.values, expected_values)
    assert res.index == expected_index


def test__read_single_soh_miniseed_gpstime_centaur_empty_channels():
    filepath = Path(__file__).parent.joinpath("data", "SI.SI09.D0.SOH_centaur-3_1492_20180616_040000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_GPSTIME.value)

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    expected_values = np.array([], shape=(0, 6), dtype=np.float64)
    expected_index = pd.DatetimeIndex([], dtype='datetime64[ns, UTC]', freq=None)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (0, 6)
    assert np.array_equal(res.values, expected_values)
    assert res.index == expected_index


def test__read_single_soh_miniseed_instrument_centaur_empty_channels():
    filepath = Path(__file__).parent.joinpath("data", "SI.SI09.D0.SOH_centaur-3_1492_20180616_040000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_INSTRUMENT.value)

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    expected_values = np.array([[np.nan, np.nan, 25250.],
                                [np.nan, np.nan, 25000.]])
    expected_index = pd.DatetimeIndex(['2018-06-16 04:00:00+00:00', '2018-06-16 04:30:00+00:00'],
                                      dtype='datetime64[ns, UTC]', freq=None)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 3)
    assert np.array_equal(res.values, expected_values)
    assert res.index == expected_index


def test__postprocess_soh_miniseed_instrument_centaur_empty_channels():
    filepath = Path(__file__).parent.joinpath("data", "SI.SI09.D0.SOH_centaur-3_1492_20180616_040000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_INSTRUMENT.value)

    part = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)
    res = _postprocess_soh_miniseed_instrument_centaur(df=part)

    expected_values = np.array([[np.nan, np.nan, 25.250],
                                [np.nan, np.nan, 25.000]])
    expected_index = pd.DatetimeIndex(['2018-06-16 04:00:00+00:00', '2018-06-16 04:30:00+00:00'],
                                      dtype='datetime64[ns, UTC]', freq=None)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 3)
    assert np.array_equal(res.values, expected_values)
    assert res.index == expected_index

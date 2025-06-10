# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pytest

import numpy as np
from pandas._testing import assert_frame_equal
from pathlib import Path
import pandas as pd
from pandas.testing import assert_index_equal

from noiz.processing.soh.parsing_params import (
    SohType,
    SohInstrumentNames,
    load_parsing_parameters,
    _read_single_soh_miniseed_centaur,
    _postprocess_soh_miniseed_instrument_centaur,
    _postprocess_soh_miniseed_gpstime_centaur,
)


def test__read_single_soh_miniseed_gpstime_centaur_all_channels_present():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_GPSTIME.value
    )

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    expected_columns = pd.Index(
        [
            "GPS receiver status",
            "GPS satellites used",
            "Timing status",
            "Phase lock loop status",
            "Timing DAC count",
            "Time error(ms)",
            "Time uncertainty(ms)",
        ],
        dtype="object",
    )
    expected_values = np.array(
        [
            [0.00000000e00, 1.00000000e01, 9.00000000e01, 3.00000000e00, 8.32400000e03, 0.00000000e00, np.nan],
            [0.00000000e00, 1.00000000e01, 9.00000000e01, 3.00000000e00, 8.32700000e03, 0.00000000e00, np.nan],
        ]
    )
    expected_index = pd.DatetimeIndex(
        ["2017-11-25 10:00:00+00:00", "2017-11-25 10:30:00+00:00"], dtype="datetime64[ns, UTC]", freq=None
    )
    expected_df = pd.DataFrame(columns=expected_columns, index=expected_index, data=expected_values)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 7)
    assert_frame_equal(res, expected_df)


def test__postprocess_soh_miniseed_gpstime_centaur_all_channels_present():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_GPSTIME.value
    )

    df = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)
    orig_vals = np.array([2657, 2356])
    df["Time error(ms)"] = orig_vals

    res = _postprocess_soh_miniseed_gpstime_centaur(df=df)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 7)
    assert np.array_equal(res["Time error(ms)"].values, orig_vals / 1000)


def test__read_single_soh_miniseed_instrument_centaur_all_channels_present():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_INSTRUMENT.value
    )

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    expected_values = np.array([[13302.0, 89.0, 17000.0], [13369.0, 90.0, 17000.0]])

    expected_index = pd.DatetimeIndex(
        ["2017-11-25 10:00:00+00:00", "2017-11-25 10:30:00+00:00"], dtype="datetime64[ns, UTC]", freq=None
    )

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 3)
    assert np.array_equal(res.values, expected_values)
    assert_index_equal(res.index, expected_index)


def test__postprocess_soh_miniseed_instrument_centaur_all_channels_present():
    filepath = Path(__file__).parent.joinpath("data", "XX.S0001.D0.SOH_centaur-3_1479_20171125_100000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_INSTRUMENT.value
    )

    part = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)
    res = _postprocess_soh_miniseed_instrument_centaur(df=part)

    expected_values = np.array([[13.302, 0.089, 17.0], [13.369, 0.09, 17.0]])

    expected_index = pd.DatetimeIndex(
        ["2017-11-25 10:00:00+00:00", "2017-11-25 10:30:00+00:00"], dtype="datetime64[ns, UTC]", freq=None
    )

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 3)
    assert np.array_equal(res.values, expected_values)
    assert_index_equal(res.index, expected_index)


def test__read_single_soh_miniseed_gpstime_centaur_empty_channels():
    filepath = Path(__file__).parent.joinpath("data", "TD.TD09.D0.SOH_centaur-3_1492_20180616_040000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_GPSTIME.value
    )

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    expected_index = pd.DatetimeIndex([], dtype="datetime64[ns, UTC]", freq=None)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (0, 7)
    assert_index_equal(res.index, expected_index)


def test__read_single_soh_miniseed_instrument_centaur_empty_channels():
    filepath = Path(__file__).parent.joinpath("data", "TD.TD09.D0.SOH_centaur-3_1492_20180616_040000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_INSTRUMENT.value
    )

    res = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)

    expected_columns = pd.Index(["Supply voltage(V)", "Total current(A)", "Temperature(C)"], dtype="object")
    expected_values = np.array([[np.nan, np.nan, 25250.0], [np.nan, np.nan, 25000.0]])
    expected_index = pd.DatetimeIndex(
        ["2018-06-16 04:00:00+00:00", "2018-06-16 04:30:00+00:00"], dtype="datetime64[ns, UTC]", freq=None
    )
    expected_df = pd.DataFrame(index=expected_index, columns=expected_columns, data=expected_values)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 3)
    assert_frame_equal(res, expected_df)


def test__postprocess_soh_miniseed_instrument_centaur_empty_channels():
    filepath = Path(__file__).parent.joinpath("data", "TD.TD09.D0.SOH_centaur-3_1492_20180616_040000.miniseed")
    parsing_params = load_parsing_parameters(
        station_type=SohInstrumentNames.CENTAUR.value, soh_type=SohType.MINISEED_INSTRUMENT.value
    )

    part = _read_single_soh_miniseed_centaur(filepath=filepath, parsing_params=parsing_params)
    res = _postprocess_soh_miniseed_instrument_centaur(df=part)

    expected_columns = pd.Index(["Supply voltage(V)", "Total current(A)", "Temperature(C)"], dtype="object")
    expected_values = np.array([[np.nan, np.nan, 25.250], [np.nan, np.nan, 25.000]])
    expected_index = pd.DatetimeIndex(
        ["2018-06-16 04:00:00+00:00", "2018-06-16 04:30:00+00:00"], dtype="datetime64[ns, UTC]", freq=None
    )
    expected_df = pd.DataFrame(index=expected_index, columns=expected_columns, data=expected_values)

    assert isinstance(res, pd.DataFrame)
    assert isinstance(res.index, pd.DatetimeIndex)
    assert res.shape == (2, 3)
    assert_frame_equal(res, expected_df)

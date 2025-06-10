# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from datetime import timedelta
import numpy as np

from noiz.exceptions import NotEnoughDataError, ValidationError
from noiz.models.timespan import Timespan
from obspy import Stream
import os
import pytest
from pandas import Timestamp

from noiz.processing.signal_helpers import validate_and_fix_subsample_starttime_error


def test_validate_and_fix_subsample_starttime_error_valid():
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 100 samples",
        "AA.XXY..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 60.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 100 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    assert validate_and_fix_subsample_starttime_error(st) == st


def test_validate_and_fix_subsample_starttime_error_not_enough_traces():
    s = [
        "",
        "",
        "2 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 60.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 60.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    with pytest.raises(NotEnoughDataError):
        validate_and_fix_subsample_starttime_error(st)


def test_validate_and_fix_subsample_starttime_error_valid_start_later():
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st_expected = Stream._dummy_stream_from_string(s)
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T00:00:00.010000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st_invalid = Stream._dummy_stream_from_string(s)
    st_validated = validate_and_fix_subsample_starttime_error(st_invalid)
    assert st_validated == st_expected


def test_validate_and_fix_subsample_starttime_error_valid_start_earlier():
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st_expected = Stream._dummy_stream_from_string(s)
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T00:59:59.990000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st_invalid = Stream._dummy_stream_from_string(s)
    st_validated = validate_and_fix_subsample_starttime_error(st_invalid)
    assert st_validated == st_expected


def test_validate_and_fix_subsample_starttime_error_valid_start_earlier_and_later():
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "AA.XZZ..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st_expected = Stream._dummy_stream_from_string(s)
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T00:59:59.990000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "AA.XZZ..HH2 | 2016-01-07T01:00:00.0490000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st_invalid = Stream._dummy_stream_from_string(s)
    st_validated = validate_and_fix_subsample_starttime_error(st_invalid)
    assert st_validated == st_expected


def test_validate_and_fix_subsample_starttime_error_valid_start_later_too_big():
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T00:00:01.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T00:00:00.000000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    with pytest.raises(ValidationError):
        validate_and_fix_subsample_starttime_error(st)


def test_validate_and_fix_subsample_starttime_error_valid_start_earlier_too_big():
    s = [
        "",
        "",
        "3 Trace(s) in Stream:",
        "AA.XXX..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXY..HH2 | 2016-01-07T01:00:00.000000Z - 2016-01-07T00:00:09.900000Z | 10.0 Hz, 120 samples",
        "AA.XXZ..HH2 | 2016-01-07T00:59:59.000000Z - 2016-01-07T00:00:21.900000Z | 10.0 Hz, 120 samples",
        "",
        "",
    ]

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    with pytest.raises(ValidationError):
        validate_and_fix_subsample_starttime_error(st)

from datetime import datetime
from types import GeneratorType

import pandas as pd
import pytest

from noiz.processing.timespan import generate_starttimes_endtimes, generate_timespans, cast_to_timedelta, \
    _calculate_frequency_for_generating_timespans


def test_generate_starttimes_endtimes():
    startdate = datetime(2023, 1, 1)
    enddate = datetime(2023, 1, 2)
    window_length = 360
    window_overlap = 0
    generate_midtimes = False

    times = generate_starttimes_endtimes(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_midtimes=generate_midtimes,
    )
    assert len(times) == 2
    assert len(times[0]) == 240
    assert len(times[0]) == len(times[-1])
    assert isinstance(times[0], list)
    assert isinstance(times[-1], list)
    assert all([isinstance(x, pd.Timestamp) for x in times[0]])
    assert all([isinstance(x, pd.Timestamp) for x in times[-1]])

    assert times[0][0] == pd.Timestamp("2023-01-01 00:00:00")
    assert times[0][-1] == pd.Timestamp("2023-01-01 23:54:00")

    assert times[-1][0] == pd.Timestamp("2023-01-01 00:06:00")
    assert times[-1][-1] == pd.Timestamp("2023-01-02 00:00:00")


def test_generate_starttimes_endtimes_with_midtime():
    startdate = datetime(2023, 1, 1)
    enddate = datetime(2023, 1, 2)
    window_length = 360
    window_overlap = 0
    generate_midtimes = True

    times = generate_starttimes_endtimes(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_midtimes=generate_midtimes,
    )
    assert len(times) == 3
    assert len(times[0]) == 240
    assert len(times[0]) == len(times[-1])
    assert isinstance(times[0], list)
    assert isinstance(times[-1], list)
    assert all([isinstance(x, pd.Timestamp) for x in times[0]])
    assert all([isinstance(x, pd.Timestamp) for x in times[-1]])

    assert times[0][0] == pd.Timestamp("2023-01-01 00:00:00")
    assert times[0][-1] == pd.Timestamp("2023-01-01 23:54:00")

    assert times[1][0] == pd.Timestamp("2023-01-01 00:03:00")
    assert times[1][-1] == pd.Timestamp("2023-01-01 23:57:00")

    assert times[-1][0] == pd.Timestamp("2023-01-01 00:06:00")
    assert times[-1][-1] == pd.Timestamp("2023-01-02 00:00:00")


def test_generate_starttimes_endtimes_with_overlap():
    startdate = datetime(2023, 1, 1)
    enddate = datetime(2023, 1, 2)
    window_length = 360
    window_overlap = 180
    generate_midtimes = False

    times = generate_starttimes_endtimes(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_midtimes=generate_midtimes,
    )
    assert len(times) == 2
    assert len(times[0]) == 480
    assert len(times[0]) == len(times[-1])
    assert isinstance(times[0], list)
    assert isinstance(times[-1], list)
    assert all([isinstance(x, pd.Timestamp) for x in times[0]])
    assert all([isinstance(x, pd.Timestamp) for x in times[-1]])

    assert times[0][0] == pd.Timestamp("2023-01-01 00:00:00")
    assert times[0][1] == pd.Timestamp("2023-01-01 00:03:00")
    assert times[0][-1] == pd.Timestamp("2023-01-01 23:57:00")

    assert times[-1][0] == pd.Timestamp("2023-01-01 00:06:00")
    assert times[-1][-3] == pd.Timestamp("2023-01-01 23:57:00")
    assert times[-1][-2] == pd.Timestamp("2023-01-02 00:00:00")
    assert times[-1][-1] == pd.Timestamp("2023-01-02 00:03:00")


def test_generate_starttimes_endtimes_with_overlap_and_midtimes():
    startdate = datetime(2023, 1, 1)
    enddate = datetime(2023, 1, 2)
    window_length = 360
    window_overlap = 180
    generate_midtimes = True

    times = generate_starttimes_endtimes(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_midtimes=generate_midtimes,
    )
    assert len(times) == 3
    assert len(times[0]) == 480
    assert len(times[0]) == len(times[-1])
    assert isinstance(times[0], list)
    assert isinstance(times[-1], list)
    assert all([isinstance(x, pd.Timestamp) for x in times[0]])
    assert all([isinstance(x, pd.Timestamp) for x in times[-1]])

    assert times[0][0] == pd.Timestamp("2023-01-01 00:00:00")
    assert times[0][1] == pd.Timestamp("2023-01-01 00:03:00")
    assert times[0][-1] == pd.Timestamp("2023-01-01 23:57:00")

    assert times[1][0] == pd.Timestamp("2023-01-01 00:03:00")
    assert times[1][1] == pd.Timestamp("2023-01-01 00:06:00")
    assert times[1][-1] == pd.Timestamp("2023-01-02 00:00:00")

    assert times[-1][0] == pd.Timestamp("2023-01-01 00:06:00")
    assert times[-1][-3] == pd.Timestamp("2023-01-01 23:57:00")
    assert times[-1][-2] == pd.Timestamp("2023-01-02 00:00:00")
    assert times[-1][-1] == pd.Timestamp("2023-01-02 00:03:00")


def test_generate_timespans():
    startdate = datetime(2023, 1, 1)
    enddate = datetime(2023, 1, 3)
    window_length = 360
    window_overlap = 180
    generate_over_midnight = False

    spans = generate_timespans(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_over_midnight=generate_over_midnight,
    )

    assert isinstance(spans, GeneratorType)
    sp = list(spans)
    assert len(sp) == 958
    assert sp[0].starttime == startdate
    assert sp[-1].endtime == enddate


def test_generate_timespans_over_midnight():
    startdate = datetime(2023, 1, 1)
    enddate = datetime(2023, 1, 3)
    window_length = 360
    window_overlap = 180
    generate_over_midnight = True

    spans = generate_timespans(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_over_midnight=generate_over_midnight,
    )

    assert isinstance(spans, GeneratorType)
    sp = list(spans)
    assert len(sp) == 960
    assert sp[0].starttime == startdate
    assert sp[-1].endtime == datetime(2023, 1, 3, 0, 3)


def test_generate_timespans_over_midnight_single_day():
    startdate = datetime(2023, 1, 1)
    enddate = datetime(2023, 1, 2)
    window_length = 360
    window_overlap = 180
    generate_over_midnight = True

    spans = generate_timespans(
        startdate=startdate,
        enddate=enddate,
        window_length=window_length,
        window_overlap=window_overlap,
        generate_over_midnight=generate_over_midnight,
    )

    assert isinstance(spans, GeneratorType)
    sp = list(spans)
    assert len(sp) == 480
    assert sp[0].starttime == startdate
    assert sp[-1].endtime == datetime(2023, 1, 2, 0, 3)


@pytest.mark.parametrize(
    "window, resolution, expected_result",
    (
            (10, "s", pd.Timedelta(10, "s")),
            (7.3, "s", pd.Timedelta(7.3, "s")),
            (pd.Timedelta(10, "s"), "s", pd.Timedelta(10, "s")),
            (pd.Timedelta(55.3, "s"), "s", pd.Timedelta(55.3, "s")),
            (pd.Timedelta(55.3, "D"), "s", pd.Timedelta(55.3, "D")),
            (55.3, "D", pd.Timedelta(55.3, "D")),
    )
)
def test_cast_to_timedelta(window, resolution, expected_result):
    assert expected_result == cast_to_timedelta(
        window=window,
        resolution=resolution,
    )


@pytest.mark.parametrize(
    "window, resolution",
    (
            ("wrongval", 11),
            ("wrongval", "s"),
            ((11, 12), "s"),
            ([11, 12], "s"),
            (11, 3),
            (None, "s"),
            (None, None),
    )
)
def test_cast_to_timedelta_raising(window, resolution):
    with pytest.raises(TypeError):
        cast_to_timedelta(
            window=window,
            resolution=resolution,
        )


def test__calculate_frequency_for_generating_timespans_no_overlap():
    window_length = pd.Timedelta(10, "s")
    assert window_length == _calculate_frequency_for_generating_timespans(window_length=window_length)


def test__calculate_frequency_for_generating_timespans():
    window_length = pd.Timedelta(10, "s")
    window_overlap = pd.Timedelta(5, "s")
    assert pd.Timedelta(window_length - window_overlap) == _calculate_frequency_for_generating_timespans(
        window_length=window_length,
        window_overlap=window_overlap
    )


def test__calculate_frequency_for_generating_timespans_longer_overlap_than_window():
    window_length = pd.Timedelta(10, "s")
    window_overlap = pd.Timedelta(15, "s")
    with pytest.raises(ValueError):
        _calculate_frequency_for_generating_timespans(
            window_length=window_length,
            window_overlap=window_overlap
        )

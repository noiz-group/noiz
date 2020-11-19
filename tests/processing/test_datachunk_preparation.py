from datetime import timedelta
import numpy as np
from noiz.models.timespan import Timespan
from obspy import Stream, Trace, UTCDateTime
import os
import pytest
from pandas import Timestamp

from noiz.models.processing_params import DatachunkParams
from noiz.processing.datachunk_preparation import (
    merge_traces_fill_zeros,
    merge_traces_under_conditions,
    _check_if_gaps_short_enough,
    _check_and_remove_extra_samples_on_the_end,
    pad_zeros_to_exact_time_bounds,
    interpolate_ends_to_zero_to_fit_timespan,
    next_pow_2, validate_slice,
)


@pytest.mark.xfail
def test_expected_npts():
    assert False


@pytest.mark.parametrize(["val", "expected"], [
    (3, 2),
    (3.3, 2),
    (100, 7),
    (55.57666664, 6)
])
def test_next_pow_2(val, expected):
    output = next_pow_2(val)

    assert isinstance(output, int)
    assert expected == output


@pytest.mark.xfail
def test_resample_with_padding():
    assert False


def test_merge_traces_fill_zeros():
    s = ['', '', '2 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T00:00:09.900000Z | 10.0 Hz, 100 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:12.000000Z - '
         '2016-01-07T00:00:21.900000Z | 10.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    st_merged = merge_traces_fill_zeros(st)

    assert len(st_merged) == 1
    assert np.count_nonzero(st[0].data == 0) == 20


@pytest.mark.filterwarnings("ignore: Incompatible traces")
def test_merge_traces_fill_zeros_different_sampling_rates_of_traces():
    s = ['', '', '2 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T00:00:09.900000Z | 10.0 Hz, 100 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:12.000000Z - '
         '2016-01-07T00:00:21.900000Z | 25.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    with pytest.raises(ValueError):
        merge_traces_fill_zeros(st)


def test__check_if_gaps_short_enough_positive():
    s = ['', '', '3 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 98 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 103 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:20.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    params = DatachunkParams(max_gap_for_merging=3)

    assert _check_if_gaps_short_enough(st=st, params=params)


def test__check_if_gaps_short_enough_positive_zero_gap_permitted():
    s = ['', '', '3 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:20.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    params = DatachunkParams(max_gap_for_merging=0)

    assert _check_if_gaps_short_enough(st=st, params=params)


def test__check_if_gaps_short_enough_positive_zero_gap_permitted_negative():
    s = ['', '', '3 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 99 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:20.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    params = DatachunkParams(max_gap_for_merging=0)

    with pytest.raises(ValueError):
        _check_if_gaps_short_enough(st=st, params=params)


def test__check_if_gaps_short_enough_positive_zero_gap_permitted_negative_overlap():
    s = ['', '', '3 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 101 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:20.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[1].data *= 3

    params = DatachunkParams(max_gap_for_merging=0)

    with pytest.raises(ValueError):
        _check_if_gaps_short_enough(st=st, params=params)


def test__check_if_gaps_short_enough_too_long_gap():
    s = ['', '', '3 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 95 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 103 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:20.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    params = DatachunkParams(max_gap_for_merging=2)

    with pytest.raises(ValueError):
        _check_if_gaps_short_enough(st=st, params=params)


def test__check_if_gaps_short_enough_too_long_overlap():
    s = ['', '', '3 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 103 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:20.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[-1].data *= 3

    params = DatachunkParams(max_gap_for_merging=2)

    with pytest.raises(ValueError):
        _check_if_gaps_short_enough(st=st, params=params)


def test__check_if_gaps_short_enough_different_ids():
    s = ['', '', '3 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 98 samples',
         'AA.XXX..HH3 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 103 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:20.000000Z - '
         '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    params = DatachunkParams(max_gap_for_merging=2)

    with pytest.raises(ValueError):
        _check_if_gaps_short_enough(st=st, params=params)


def test__check_if_gaps_short_enough_zero_traces():
    st = Stream()

    params = DatachunkParams(max_gap_for_merging=2)

    with pytest.raises(ValueError):
        _check_if_gaps_short_enough(st=st, params=params)


def test_merge_traces_under_conditions():
    s_in = ['', '', '2 Trace(s) in Stream:',
            'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
            '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
            'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
            '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
            '', '']
    s_out = ['', '', '1 Trace(s) in Stream:',
             'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
             '2016-01-07T03:00:00.000000Z | 10.0 Hz, 200 samples',
             '', '']

    s_in = os.linesep.join(s_in)
    st_in = Stream._dummy_stream_from_string(s_in)
    s_out = os.linesep.join(s_out)
    st_expected = Stream._dummy_stream_from_string(s_out)

    params = DatachunkParams(max_gap_for_merging=2)

    st_merged = merge_traces_under_conditions(st=st_in, params=params)

    assert st_merged == st_expected


def test_merge_traces_under_conditions_single_trace():
    s_in = ['', '', '1 Trace(s) in Stream:',
            'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
            '2016-01-07T03:00:00.000000Z | 10.0 Hz, 100 samples',
            '', '']

    s_in = os.linesep.join(s_in)
    st_in = Stream._dummy_stream_from_string(s_in)

    params = DatachunkParams(max_gap_for_merging=2)

    st_merged = merge_traces_under_conditions(st=st_in, params=params)
    assert st_merged == st_in


def test_merge_traces_under_conditions_with_interpolation():
    s = ['', '', '2 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 8 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 10 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[1].data *= 4
    params = DatachunkParams(max_gap_for_merging=2)

    st_merged = merge_traces_under_conditions(st=st, params=params)

    trace_data_result = np.array([1., 1., 1., 1., 1., 1., 1., 1., 2., 3., 4., 4., 4.,
                                  4., 4., 4., 4., 4., 4., 4.])

    assert isinstance(st_merged, Stream)
    assert len(st_merged) == 1
    assert np.array_equal(trace_data_result, st_merged[0].data)


def test_merge_traces_under_conditions_failing():
    s = ['', '', '2 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 5 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 10 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    params = DatachunkParams(max_gap_for_merging=2)

    with pytest.raises(ValueError):
        merge_traces_under_conditions(st=st, params=params)


def test_pad_zeros_to_exact_time_bounds_end_only():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 7 samples',
         '', '']

    ts = Timespan(
        starttime=Timestamp('2016-01-07T00:00:00.000000Z'),
        midtime=Timestamp('2016-01-07T00:00:05.000000Z'),
        endtime=Timestamp('2016-01-07T00:00:10.000000Z'),
    )

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[0].data *= 4
    expected_no_samples = 10

    expected_data = np.array([4., 4., 4., 4., 4., 4., 4., 0., 0., 0.])

    st_res = pad_zeros_to_exact_time_bounds(st=st, timespan=ts, expected_no_samples=expected_no_samples)

    assert len(st_res) == 1
    assert len(st_res[0].data) == expected_no_samples
    assert np.array_equal(st_res[0].data, expected_data)


def test_pad_zeros_to_exact_time_bounds_beginning_only():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:03.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 7 samples',
         '', '']

    ts = Timespan(
        starttime=Timestamp('2016-01-07T00:00:00.000000Z'),
        midtime=Timestamp('2016-01-07T00:00:05.000000Z'),
        endtime=Timestamp('2016-01-07T00:00:10.000000Z'),
    )

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[0].data *= 4
    expected_no_samples = 10

    expected_data = np.array([0., 0., 0., 4., 4., 4., 4., 4., 4., 4.])

    st_res = pad_zeros_to_exact_time_bounds(st=st, timespan=ts, expected_no_samples=expected_no_samples)

    assert len(st_res) == 1
    assert len(st_res[0].data) == expected_no_samples
    assert np.array_equal(st_res[0].data, expected_data)


def test_pad_zeros_to_exact_time_bounds_both_ends():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:03.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 4 samples',
         '', '']

    ts = Timespan(
        starttime=Timestamp('2016-01-07T00:00:00.000000Z'),
        midtime=Timestamp('2016-01-07T00:00:05.000000Z'),
        endtime=Timestamp('2016-01-07T00:00:10.000000Z'),
    )

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[0].data *= 4
    expected_no_samples = 10

    expected_data = np.array([0., 0., 0., 4., 4., 4., 4., 0., 0., 0.])

    st_res = pad_zeros_to_exact_time_bounds(st=st, timespan=ts, expected_no_samples=expected_no_samples)

    assert len(st_res) == 1
    assert len(st_res[0].data) == expected_no_samples
    assert np.array_equal(st_res[0].data, expected_data)


def test_interpolate_ends_to_zero_to_fit_timespan_end_only():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 6 samples',
         '', '']

    ts = Timespan(
        starttime=Timestamp('2016-01-07T00:00:00.000000Z'),
        midtime=Timestamp('2016-01-07T00:00:05.000000Z'),
        endtime=Timestamp('2016-01-07T00:00:10.000000Z'),
    )

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[0].data *= 4
    expected_no_samples = 10

    expected_data = np.array([4., 4., 4., 4., 4., 4., 3., 2., 1., 0.])

    st_res = interpolate_ends_to_zero_to_fit_timespan(st=st, timespan=ts, expected_no_samples=expected_no_samples)

    assert len(st_res) == 1
    assert len(st_res[0].data) == expected_no_samples
    assert np.array_equal(st_res[0].data, expected_data)


def test_interpolate_ends_to_zero_to_fit_timespan_beginning_only():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:04.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 7 samples',
         '', '']

    ts = Timespan(
        starttime=Timestamp('2016-01-07T00:00:00.000000Z'),
        midtime=Timestamp('2016-01-07T00:00:05.000000Z'),
        endtime=Timestamp('2016-01-07T00:00:10.000000Z'),
    )

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[0].data *= 4
    expected_no_samples = 10

    expected_data = np.array([0., 1., 2., 3., 4., 4., 4., 4., 4., 4.])

    st_res = interpolate_ends_to_zero_to_fit_timespan(st=st, timespan=ts, expected_no_samples=expected_no_samples)

    assert len(st_res) == 1
    assert len(st_res[0].data) == expected_no_samples
    assert np.array_equal(st_res[0].data, expected_data)


def test_interpolate_ends_to_zero_to_fit_timespan_both_ends():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:04.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 2 samples',
         '', '']

    ts = Timespan(
        starttime=Timestamp('2016-01-07T00:00:00.000000Z'),
        midtime=Timestamp('2016-01-07T00:00:05.000000Z'),
        endtime=Timestamp('2016-01-07T00:00:10.000000Z'),
    )

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    st[0].data *= 4
    expected_no_samples = 10

    expected_data = np.array([0., 1., 2., 3., 4., 4., 3., 2., 1., 0.])

    st_res = interpolate_ends_to_zero_to_fit_timespan(st=st, timespan=ts, expected_no_samples=expected_no_samples)

    assert len(st_res) == 1
    assert len(st_res[0].data) == expected_no_samples
    assert np.array_equal(st_res[0].data, expected_data)


@pytest.mark.xfail
def test_preprocess_timespan():
    assert False


@pytest.mark.xfail
def test_create_datachunks_add_to_db():
    assert False


@pytest.mark.xfail
def test_create_datachunks_for_component():
    assert False


@pytest.mark.xfail
def test_add_or_upsert_datachunks_in_db():
    assert False


def test_validate_slice():
    expected_npts = 120
    expected_sampling = 2

    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         f'2016-01-07T03:00:00.000000Z | {expected_sampling} Hz, {expected_npts} samples',
         '', '']

    ts = Timespan(
        starttime=Timestamp('2016-01-07T00:00:00.000000Z'),
        midtime=Timestamp('2016-01-07T00:00:30.000000Z'),
        endtime=Timestamp('2016-01-07T00:01:00.000000Z'),
    )

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    dp = DatachunkParams(sampling_rate=expected_sampling, timespan_length=timedelta(seconds=60))
    validated_stream, deficit, verbosity_dict = validate_slice(
        trimmed_st=st, timespan=ts, original_samplerate=2, processing_params=dp)

    assert isinstance(validated_stream, Stream)
    assert len(validated_stream) == 1
    assert len(validated_stream[0].data) == expected_npts
    assert validated_stream[0].stats.sampling_rate == expected_sampling
    assert validated_stream[0].stats.starttime == ts.starttime
    assert validated_stream[0].stats.endtime == ts.endtime_at_last_sample(sampling_rate=expected_sampling)


@pytest.mark.xfail
def test_validate_slice_mutli_traces_mergeable():
    assert False


@pytest.mark.xfail
def test_validate_slice_multi_traces_not_mergeabe():
    assert False


@pytest.mark.xfail
def test_validate_slice_not_enough_samples():
    assert False


@pytest.mark.xfail
def test_validate_slice_samples_over_minimum():
    assert False


@pytest.mark.xfail
def test_validate_slice_no_traces_inside():
    assert False


def test__check_and_remove_extra_samples_on_the_end_no_action():
    expected_no_samples = 10
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         f'2016-01-07T03:00:00.000000Z | 1.0 Hz, {expected_no_samples} samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    assert st == _check_and_remove_extra_samples_on_the_end(st=st, expected_no_samples=expected_no_samples)


def test__check_and_remove_extra_samples_on_the_end_not_enough_samples():
    expected_no_samples = 50
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         f'2016-01-07T03:00:00.000000Z | 1.0 Hz, {expected_no_samples-10} samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    with pytest.raises(ValueError):
        _check_and_remove_extra_samples_on_the_end(st=st, expected_no_samples=expected_no_samples)


def test__check_and_remove_extra_samples_on_the_end_trim():
    expected_no_samples = 10
    s_out = ['', '', '1 Trace(s) in Stream:',
             'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
             f'2016-01-07T03:00:00.000000Z | 1.0 Hz, {expected_no_samples} samples',
             '', '']

    s_in = ['', '', '1 Trace(s) in Stream:',
            'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
            '2016-01-07T03:00:00.000000Z | 1.0 Hz, 15 samples',
            '', '']

    s_in = os.linesep.join(s_in)
    st_in = Stream._dummy_stream_from_string(s_in)
    s_out = os.linesep.join(s_out)
    st_out = Stream._dummy_stream_from_string(s_out)

    assert st_out == _check_and_remove_extra_samples_on_the_end(st=st_in, expected_no_samples=expected_no_samples)


def test__check_and_remove_extra_samples_on_the_end_many_traces():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:00.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 8 samples',
         'AA.XXX..HH2 | 2016-01-07T00:00:10.00000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 10 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    with pytest.raises(ValueError):
        _check_and_remove_extra_samples_on_the_end(st=st, expected_no_samples=10)


def test__check_and_remove_extra_samples_on_the_end_zero_traces():
    st = Stream()

    with pytest.raises(ValueError):
        _check_and_remove_extra_samples_on_the_end(st=st, expected_no_samples=10)

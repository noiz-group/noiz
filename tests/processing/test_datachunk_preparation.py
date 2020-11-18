import numpy as np
from obspy import Stream
import os
import pytest

from noiz.models.processing_params import DatachunkParams
from noiz.processing.datachunk_preparation import (
    merge_traces_fill_zeros,
    merge_traces_under_conditions,
    _check_if_gaps_short_enough,
    _check_and_remove_extra_samples_on_the_end,
)


@pytest.mark.xfail
def test_assembly_sds_like_dir():
    assert False


@pytest.mark.xfail
def test_expected_npts():
    assert False


@pytest.mark.xfail
def test_assembly_preprocessing_filename():
    assert False


@pytest.mark.xfail
def test_assembly_filepath():
    assert False


@pytest.mark.xfail
def test_directory_exists_or_create():
    assert False


@pytest.mark.xfail
def test_increment_filename_counter():
    assert False


@pytest.mark.xfail
def test_next_pow_2():
    assert False


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


@pytest.mark.xfail
def test_pad_zeros_to_exact_time_bounds():
    assert False


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


@pytest.mark.xfail
def test_validate_slice():
    assert False


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

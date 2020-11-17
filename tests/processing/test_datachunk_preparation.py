import numpy as np
from obspy import Stream
import os
import pytest

from noiz.models.processing_params import DatachunkParams
from noiz.processing.datachunk_preparation import (
    merge_traces_fill_zeros,
    merge_traces_under_conditions,
    _check_if_samples_short_enough,
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


def test__check_if_samples_short_enough_positive():
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

    assert _check_if_samples_short_enough(st=st, params=params)


def test__check_if_samples_short_enough_positive_zero_gap_permitted():
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

    assert _check_if_samples_short_enough(st=st, params=params)


def test__check_if_samples_short_enough_positive_zero_gap_permitted_negative():
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
        _check_if_samples_short_enough(st=st, params=params)


def test__check_if_samples_short_enough_positive_zero_gap_permitted_negative_overlap():
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
        _check_if_samples_short_enough(st=st, params=params)


def test__check_if_samples_short_enough_too_long_gap():
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

    params = DatachunkParams(max_gap_for_merging=2)

    with pytest.raises(ValueError):
        _check_if_samples_short_enough(st=st, params=params)


def test__check_if_samples_short_enough_different_ids():
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
        _check_if_samples_short_enough(st=st, params=params)


def test__check_if_samples_short_enough_zero_traces():
    st = Stream()

    params = DatachunkParams(max_gap_for_merging=2)

    with pytest.raises(ValueError):
        _check_if_samples_short_enough(st=st, params=params)


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

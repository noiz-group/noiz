import os
import pytest

import numpy as np

from obspy import Stream

from noiz.processing.datachunk_preparation import merge_traces_fill_zeros

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

    assert len(st) == 2
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

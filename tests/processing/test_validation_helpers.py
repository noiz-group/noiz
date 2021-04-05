import numpy as np
import os
import pytest

from obspy import Stream

from noiz.validation_helpers import count_consecutive_trues, validate_stream_with_single_trace


@pytest.mark.parametrize(["input", "output"],
                         [
                             (np.array([0, 0, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0], dtype=bool), np.array([7])),
                             (np.array([0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0, 0], dtype=bool), np.array([3, 3])),
                             (np.array([0, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0, 1], dtype=bool), np.array([3, 3, 1])),
                             (np.array([1, 0, 1, 1, 1, 0, 1, 1, 1, 0, 0, 1], dtype=bool), np.array([1, 3, 3, 1])),
                             (np.array([1, 1, 1], dtype=bool), np.array([3])),
                             (np.array([0, 0], dtype=bool), np.array([])),
                             (np.array([1, 1], dtype=bool), np.array([2])),
                         ])
def test_count_consecutive_trues(input, output):
    assert np.array_equal(output, count_consecutive_trues(input))


def test__validate_stream_with_single_trace():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:04.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 30 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    assert None is validate_stream_with_single_trace(st=st)


def test__validate_stream_with_single_trace_multiple_traces():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:04.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 30 samples',
         'AA.XXX..HH3 | 2016-01-07T00:00:04.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 30 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)

    with pytest.raises(ValueError):
        validate_stream_with_single_trace(st=st)


def test__validate_stream_with_single_trace_wrong_type():
    s = ['', '', '1 Trace(s) in Stream:',
         'AA.XXX..HH2 | 2016-01-07T00:00:04.000000Z - '
         '2016-01-07T03:00:00.000000Z | 1.0 Hz, 30 samples',
         '', '']

    s = os.linesep.join(s)
    st = Stream._dummy_stream_from_string(s)
    tr = st[0]

    with pytest.raises(TypeError):
        validate_stream_with_single_trace(st=tr)

import pytest
import numpy as np

from noiz.processing.validation_helpers import count_consecutive_trues


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

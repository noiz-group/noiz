import numpy as np


def count_consecutive_trues(arr: np.array) -> np.array:
    """
    This method takes an array of booleans and counts all how many consecutive True values are within it.
    It returns an array of counts.

    For example:

    >>> a = count_consecutive_trues([0,0,0,0,1,1,1,0,0,0], dtype=bool)
    >>> a == np.array([3])

    It can also handle multiple subvectors of True:

    >>> a = count_consecutive_trues(np.array([0,1,0,0,1,1,1,0,0,0,1,1,1,1,1,1], dtype=bool))
    >>> a == np.array([1,3,6])

    This method is copied from:
    https://stackoverflow.com/a/24343375/4308541

    :param arr: Array of booleans
    :type arr: np.array
    :return: Array of integers, with counts how many consecutive True values are in the input arr array
    :rtype: np.array
    """

    counted_true_vals = np.diff(
        np.where(
            np.concatenate(
                ([arr[0]], arr[:-1] != arr[1:], [True])
            )
        )[0]
    )[::2]

    return counted_true_vals

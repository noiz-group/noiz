# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from dataclasses import dataclass

import pytest

from noiz.api.helpers import extract_object_ids
from noiz.validation_helpers import (
    validate_to_tuple,
    validate_uniformity_of_tuple,
    validate_exactly_one_argument_provided,
)


@pytest.mark.parametrize("first, second", [(1, None), ("test_string", None), (None, 2), [None, "test_string"]])
def test_validate_exactly_one_argument_provided(first, second):
    assert validate_exactly_one_argument_provided(first=second, second=first)


@pytest.mark.parametrize("first, second", [(None, None), (1, 1)])
def test_validate_exactly_one_argument_provided_invalid(first, second):
    with pytest.raises(ValueError):
        validate_exactly_one_argument_provided(first=second, second=first)


@pytest.mark.parametrize(
    "tup, typ",
    [
        ((1, 2, 3, 4, 5), int),
        ((1.0, 2.0, 3.0, 0.3, 4.0), float),
        (("aa", "bb", "cc", "dd"), str),
    ],
)
def test_validate_uniformity_of_tuple(tup, typ):
    assert validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=False)


@pytest.mark.parametrize(
    "tup, typ",
    [
        ((1, 2, 3, 4, 5), float),
        ((1, 2, 3, 4, 5), str),
        ((1.0, 2.0, 3.0, 0.3, 4.0), str),
        ((1.0, 2.0, 3.0, 0.3, 4.0), int),
        (("aa", "bb", "cc", "dd"), int),
        (("aa", "bb", "cc", "dd"), dict),
    ],
)
def test_validate_uniformity_of_tuple_wrong_type_non_raising(tup, typ):
    assert validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=False) is False


@pytest.mark.parametrize(
    "tup, typ",
    [
        ((1, 2, 3, 4, 5), float),
        ((1, 2, 3, 4, 5), str),
        ((1.0, 2.0, 3.0, 0.3, 4.0), str),
        ((1.0, 2.0, 3.0, 0.3, 4.0), int),
        (("aa", "bb", "cc", "dd"), int),
        (("aa", "bb", "cc", "dd"), dict),
    ],
)
def test_validate_uniformity_of_tuple_wrong_type_raising(tup, typ):
    with pytest.raises(ValueError):
        validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=True)


@pytest.mark.parametrize(
    "tup, typ",
    [
        ((1, 2, 3.0, 4, 5), int),
        ((1.0, "a", 3.0, 0.3, 4.0), float),
        (("aa", 1, "cc", "dd"), str),
    ],
)
def test_validate_uniformity_of_tuple_mixed_types_non_raising(tup, typ):
    assert validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=False) is False


@pytest.mark.parametrize(
    "tup, typ",
    [
        ((1, 2, 3.0, 4, 5), int),
        ((1.0, "a", 3.0, 0.3, 4.0), float),
        (("aa", 1, "cc", "dd"), str),
    ],
)
def test_validate_uniformity_of_tuple_mixed_types_raising(tup, typ):
    with pytest.raises(ValueError):
        validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=True)


@pytest.mark.parametrize(
    "tup, typ, expected",
    [
        (1, int, (1,)),
        (1.0, float, (1.0,)),
        ("aa", str, ("aa",)),
        ((1, 2), int, (1, 2)),
        ((1.0, 77.0), float, (1.0, 77.0)),
        (("aa", "zzs"), str, ("aa", "zzs")),
    ],
)
def test_validate_to_tuple(tup, typ, expected):
    assert expected == validate_to_tuple(val=tup, accepted_type=typ)


@pytest.mark.parametrize(
    "tup, typ",
    [
        ((1, 2, 3, 4, 5), float),
        (1, float),
        ((1, 2, 3, 4, 5), str),
        ((1, 2, 3, 4, 5), str),
        ((1.0, 2.0, 3.0, 0.3, 4.0), str),
        ((1.0, 2.0, 3.0, 0.3, 4.0), int),
        (("aa", "bb", "cc", "dd"), int),
        (("aa", "bb", "cc", "dd"), dict),
        (5, str),
        (2.0, str),
        (3.0, int),
        ("dd", int),
        ("bb", dict),
    ],
)
def test_validate_to_tuple_wrong_type(tup, typ):
    with pytest.raises(ValueError):
        validate_to_tuple(val=tup, accepted_type=typ)


def test_extract_object_ids():
    @dataclass
    class TestingClassWithID:
        id: int

    expected_ids = [1, 2, 3, 4, 5, 6, 15, 20, 77]
    input = [TestingClassWithID(id=i) for i in expected_ids]

    assert expected_ids == extract_object_ids(instances=input)

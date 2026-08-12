# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from noiz.api.helpers import (
    CCF_PREDELETE_CHUNK_SIZE,
    _delete_conflicting_ccf_records,
    _extract_exact_ccf_conflict_keys,
    extract_object_ids,
)
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


def test_extract_exact_ccf_conflict_keys_keeps_exact_tuples():
    @dataclass
    class FakeCrosscorrelationCartesian:
        timespan_id: int
        componentpair_id: int
        crosscorrelation_cartesian_params_id: int

    objects = (
        FakeCrosscorrelationCartesian(timespan_id=1, componentpair_id=10, crosscorrelation_cartesian_params_id=3),
        FakeCrosscorrelationCartesian(timespan_id=2, componentpair_id=20, crosscorrelation_cartesian_params_id=3),
        FakeCrosscorrelationCartesian(timespan_id=1, componentpair_id=10, crosscorrelation_cartesian_params_id=3),
    )

    assert set(_extract_exact_ccf_conflict_keys(objects)) == {
        (1, 10, 3),
        (2, 20, 3),
    }


def test_delete_conflicting_ccf_records_chunks_large_delete(monkeypatch):
    @dataclass
    class FakeCrosscorrelationCartesian:
        timespan_id: int
        componentpair_id: int
        crosscorrelation_cartesian_params_id: int

    class FakeColumn:
        pass

    fake_model = type(
        "FakeCrosscorrelationCartesianModel",
        (),
        {
            "timespan_id": FakeColumn(),
            "componentpair_id": FakeColumn(),
            "crosscorrelation_cartesian_params_id": FakeColumn(),
        },
    )
    fake_query = MagicMock()
    fake_query.filter.return_value = fake_query
    fake_query.delete.side_effect = [7, 0]
    fake_session = MagicMock()
    fake_session.query.return_value = fake_query

    monkeypatch.setattr("noiz.api.helpers.db.session", fake_session)
    monkeypatch.setattr("noiz.models.CrosscorrelationCartesian", fake_model)
    tuple_inputs = []

    class FakeTuplePredicate:
        def in_(self, value):
            tuple_inputs.append(tuple(value))
            return ("fake_predicate", value)

    monkeypatch.setattr("noiz.api.helpers.tuple_", lambda *args: FakeTuplePredicate())
    objects = tuple(
        FakeCrosscorrelationCartesian(timespan_id=i, componentpair_id=i + 100, crosscorrelation_cartesian_params_id=3)
        for i in range(CCF_PREDELETE_CHUNK_SIZE + 1)
    )

    _delete_conflicting_ccf_records(objects)

    assert fake_session.query.call_count == 2
    assert [len(batch) for batch in tuple_inputs] == [CCF_PREDELETE_CHUNK_SIZE, 1]
    fake_session.commit.assert_called_once()


def test_delete_conflicting_ccf_records_skips_commit_when_nothing_deleted(monkeypatch):
    @dataclass
    class FakeCrosscorrelationCartesian:
        timespan_id: int
        componentpair_id: int
        crosscorrelation_cartesian_params_id: int

    class FakeColumn:
        pass

    fake_model = type(
        "FakeCrosscorrelationCartesianModel",
        (),
        {
            "timespan_id": FakeColumn(),
            "componentpair_id": FakeColumn(),
            "crosscorrelation_cartesian_params_id": FakeColumn(),
        },
    )
    fake_query = MagicMock()
    fake_query.filter.return_value = fake_query
    fake_query.delete.return_value = 0
    fake_session = MagicMock()
    fake_session.query.return_value = fake_query

    monkeypatch.setattr("noiz.api.helpers.db.session", fake_session)
    monkeypatch.setattr("noiz.models.CrosscorrelationCartesian", fake_model)

    class FakeTuplePredicate:
        def in_(self, value):
            return ("fake_predicate", value)

    monkeypatch.setattr("noiz.api.helpers.tuple_", lambda *args: FakeTuplePredicate())

    _delete_conflicting_ccf_records(
        (FakeCrosscorrelationCartesian(timespan_id=1, componentpair_id=10, crosscorrelation_cartesian_params_id=3),)
    )

    fake_session.commit.assert_not_called()

import pytest

from noiz.api.helpers import validate_exactly_one_argument_provided, validate_uniformity_of_tuple, validate_to_tuple


@pytest.mark.parametrize("first, second", [(1, None), ('test_string', None), (None, 2), [None, 'test_string']])
def test_validate_exactly_one_argument_provided(first, second):
    assert validate_exactly_one_argument_provided(first=second, second=first)


@pytest.mark.parametrize("first, second", [(None, None), (1, 1)])
def test_validate_exactly_one_argument_provided_invalid(first, second):
    with pytest.raises(ValueError):
        validate_exactly_one_argument_provided(first=second, second=first)


@pytest.mark.parametrize("tup, typ", [
    ((1, 2, 3, 4, 5), int),
    ((1.0, 2.0, 3.0, .3, 4.0), float),
    (('aa', 'bb', 'cc', 'dd'), str),
])
def test_validate_uniformity_of_tuple(tup, typ):
    assert validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=False)


@pytest.mark.parametrize("tup, typ", [
    ((1, 2, 3, 4, 5), float),
    ((1, 2, 3, 4, 5), str),
    ((1.0, 2.0, 3.0, .3, 4.0), str),
    ((1.0, 2.0, 3.0, .3, 4.0), int),
    (('aa', 'bb', 'cc', 'dd'), int),
    (('aa', 'bb', 'cc', 'dd'), dict),
])
def test_validate_uniformity_of_tuple_wrong_type_non_raising(tup, typ):
    assert validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=False) is False


@pytest.mark.parametrize("tup, typ", [
    ((1, 2, 3, 4, 5), float),
    ((1, 2, 3, 4, 5), str),
    ((1.0, 2.0, 3.0, .3, 4.0), str),
    ((1.0, 2.0, 3.0, .3, 4.0), int),
    (('aa', 'bb', 'cc', 'dd'), int),
    (('aa', 'bb', 'cc', 'dd'), dict),
])
def test_validate_uniformity_of_tuple_wrong_type_raising(tup, typ):
    with pytest.raises(ValueError):
        validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=True)


@pytest.mark.parametrize("tup, typ", [
    ((1, 2, 3.0, 4, 5), int),
    ((1.0, 'a', 3.0, .3, 4.0), float),
    (('aa', 1, 'cc', 'dd'), str),
])
def test_validate_uniformity_of_tuple_mixed_types_non_raising(tup, typ):
    assert validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=False) is False


@pytest.mark.parametrize("tup, typ", [
    ((1, 2, 3.0, 4, 5), int),
    ((1.0, 'a', 3.0, .3, 4.0), float),
    (('aa', 1, 'cc', 'dd'), str),
])
def test_validate_uniformity_of_tuple_mixed_types_raising(tup, typ):
    with pytest.raises(ValueError):
        validate_uniformity_of_tuple(val=tup, accepted_type=typ, raise_errors=True)

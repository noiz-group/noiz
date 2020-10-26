import pytest

from noiz.api.helpers import validate_exactly_one_argument_provided


@pytest.mark.parametrize("first, second", [(1, None), ('test_string', None), (None, 2), [None, 'test_string']])
def test_validate_exactly_one_argument_provided(first, second):
    assert validate_exactly_one_argument_provided(first=second, second=first)


@pytest.mark.parametrize("first, second", [(None, None), (1, 1)])
def test_validate_exactly_one_argument_provided_invalid(first, second):
    with pytest.raises(ValueError):
        validate_exactly_one_argument_provided(first=second, second=first)

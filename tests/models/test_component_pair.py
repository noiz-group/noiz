import pytest

from noiz.models.component_pair import ComponentPair


class TestComponentPair:
    @pytest.mark.xfail
    def test_set_same_station(self):
        assert False

    @pytest.mark.xfail
    def test_set_autocorrelation(self):
        assert False

    @pytest.mark.xfail
    def test_set_intracorrelation(self):
        assert False

    @pytest.mark.xfail
    def test_set_params_from_distaz(self):
        assert False

    @pytest.mark.xfail
    def test__verify_east_west(self):
        assert False

import pytest

from noiz.models import Component


class TestComponent:
    @pytest.mark.xfail
    def test_init(self):
        assert False

    @pytest.mark.xfail
    def test_make_station_string(self):
        assert False

    @pytest.mark.xfail
    def test__str_(self):
        assert False

    @pytest.mark.xfail
    def test__set_xy_from_latlon(self):
        assert False

    @pytest.mark.xfail
    def test__set_latlon_from_xy(self):
        assert False

    @pytest.mark.xfail
    def test_read_inventory(self):
        assert False

    @pytest.mark.xfail
    def test___checkif_zone_letter_in_northern(self):
        assert False

    @pytest.mark.xfail
    def test____validate_zone_hemisphere(self):
        assert False

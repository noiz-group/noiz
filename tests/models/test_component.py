import pytest

from obspy.core.util import AttribDict

from noiz.models.component import Component


class TestComponent:
    def test_init(self):
        with pytest.raises(ValueError):
            Component(x=452484.15, y=5411718.72)

        with pytest.raises(ValueError):
            Component(x=452484.15)

        with pytest.raises(ValueError):
            Component()

        assert isinstance(Component(lat=10, lon=1), Component)
        assert isinstance(Component(x=452484.15, y=5411718.72, zone=31), Component)

    def test_get_location_as_attribdict(self):
        latitude = 10
        longitude = 1
        elevation = 100
        cmp = Component(lat=latitude, lon=longitude, elevation=elevation)
        assert isinstance(cmp.get_location_as_attribdict(), AttribDict)
        assert cmp.get_location_as_attribdict()["latitude"] == latitude
        assert cmp.get_location_as_attribdict()["longitude"] == longitude
        assert cmp.get_location_as_attribdict()["elevation"] == elevation

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

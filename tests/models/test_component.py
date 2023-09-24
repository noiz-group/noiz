# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pytest

from obspy.core.util import AttribDict

from noiz.models.component import Component
import datetime


class TestComponent:
    def test_init(self):
        with pytest.raises(ValueError):
            Component(x=452484.15, y=5411718.72, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1))

        with pytest.raises(ValueError):
            Component(x=452484.15, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1))

        with pytest.raises(ValueError):
            Component(start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1))

        assert isinstance(Component(lat=10, lon=1, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)), Component)
        assert isinstance(Component(x=452484.15, y=5411718.72, zone=31, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)), Component)

    def test_get_location_as_attribdict(self):
        latitude = 10
        longitude = 1
        elevation = 100
        startdate = datetime.datetime(2016, 1, 1)
        enddate = datetime.datetime(2020, 1, 1)
        cmp = Component(lat=latitude, lon=longitude, elevation=elevation, start_date=startdate, end_date=enddate)
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

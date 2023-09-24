# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pytest

from noiz.models.component import Component
from noiz.models.component_pair import ComponentPairCartesian
import datetime

from noiz.processing.component_pair import (
    is_autocorrelation,
    is_intrastation_correlation,
    _calculate_distance_backazimuth,
    is_east_to_west,
)


class TestIsAutocorrelation:
    @pytest.mark.parametrize(
        "cmp_a, cmp_b",
        [
            (
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="XYZ", station="GB", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="XYZ", station="GB", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
        ],
    )
    def test_same_component_same_station_same_network(self, cmp_a, cmp_b):
        assert is_autocorrelation(cmp_a=cmp_a, cmp_b=cmp_b)

    @pytest.mark.parametrize(
        "cmp_a, cmp_b",
        [
            (
                Component(
                    network="TD", station="TD03", component="E", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="TD", station="TD03", component="E", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="TD", station="TD13", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="XX", station="TD03", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="TD", station="TD03", component="E", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="XZ", station="TD13", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
        ],
    )
    def test_different_component_parameters(self, cmp_a, cmp_b):
        assert is_autocorrelation(cmp_a=cmp_a, cmp_b=cmp_b) is False


class TestIsIntrastationCorrelation:
    @pytest.mark.parametrize(
        "cmp_a, cmp_b",
        [
            (
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="XYZ", station="GB", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="XYZ", station="GB", component="E", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="XYZ", station="GB", component="E", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="XYZ", station="GB", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
        ],
    )
    def test_same_component_same_station_same_network(self, cmp_a, cmp_b):
        assert is_intrastation_correlation(cmp_a=cmp_a, cmp_b=cmp_b)

    @pytest.mark.parametrize(
        "cmp_a, cmp_b",
        [
            (
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="TD", station="TD17", component="E", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="XX", station="TD03", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="XX", station="TD03", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
            (
                Component(
                    network="TD", station="TD03", component="E", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
                Component(
                    network="XZ", station="TD13", component="N", lat=32.5, lon=8.2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1),
                ),
            ),
        ],
    )
    def test_different_component_parameters(self, cmp_a, cmp_b):
        assert is_intrastation_correlation(cmp_a=cmp_a, cmp_b=cmp_b) is False


@pytest.mark.xfail
def test_calculate_distance_backazimuth():
    assert False


@pytest.mark.parametrize(
    "cmp_a, cmp_b, expected_res",
    (
        (Component(lat=1, lon=1, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)), Component(lat=2, lon=2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)), False),
        (Component(lat=1, lon=1, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)), Component(lat=-10, lon=-2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)), True),
        (
            Component(x=452484.15, y=5411718.72, zone=31, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)),
            Component(lat=-10, lon=-2, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)),
            True,
        ),
        (
            Component(x=452484.15, y=5411718.72, zone=31, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)),
            Component(x=407950.22, y=5380786.7, zone=32, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)),
            False,
        ),
        (
            Component(x=407950.22, y=5380786.7, zone=32, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)),
            Component(x=452484.15, y=5411718.72, zone=31, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2020, 1, 1)),
            True,
        ),

    ),
)
def test_is_east_to_west(cmp_a, cmp_b, expected_res):
    assert is_east_to_west(cmp_a=cmp_a, cmp_b=cmp_b) == expected_res

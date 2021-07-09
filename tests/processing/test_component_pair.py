import pytest

from noiz.models.component import Component
from noiz.models.component_pair import ComponentPair

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
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XYZ", station="GB", component="N", lat=32.5, lon=8.2
                ),
                Component(
                    network="XYZ", station="GB", component="N", lat=32.5, lon=8.2
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
                    network="TD", station="TD03", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="TD", station="TD03", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="N", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="TD", station="TD13", component="Z", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XX", station="TD03", component="N", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="N", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="TD", station="TD03", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="XZ", station="TD13", component="N", lat=32.5, lon=8.2
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
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="N", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XYZ", station="GB", component="N", lat=32.5, lon=8.2
                ),
                Component(
                    network="XYZ", station="GB", component="E", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XYZ", station="GB", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="XYZ", station="GB", component="Z", lat=32.5, lon=8.2
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
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="TD", station="TD17", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="N", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XX", station="TD03", component="N", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XX", station="TD03", component="N", lat=32.5, lon=8.2
                ),
                Component(
                    network="TD", station="TD03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="TD", station="TD03", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="XZ", station="TD13", component="N", lat=32.5, lon=8.2
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
        (Component(lat=1, lon=1), Component(lat=2, lon=2), False),
        (Component(lat=1, lon=1), Component(lat=-10, lon=-2), True),
        (
            Component(x=452484.15, y=5411718.72, zone=31),
            Component(lat=-10, lon=-2),
            True,
        ),
        (
            Component(x=452484.15, y=5411718.72, zone=31),
            Component(x=407950.22, y=5380786.7, zone=32),
            False,
        ),
        (
            Component(x=407950.22, y=5380786.7, zone=32),
            Component(x=452484.15, y=5411718.72, zone=31),
            True,
        ),
    ),
)
def test_is_east_to_west(cmp_a, cmp_b, expected_res):
    assert is_east_to_west(cmp_a=cmp_a, cmp_b=cmp_b) == expected_res

import pytest

from noiz.models import ComponentPair, Component

from noiz.processing.component_pair import is_autocorrelation


class TestIsAutocorrelation:
    @pytest.mark.parametrize(
        "cmp_a, cmp_b",
        [
            (
                Component(
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
            )
        ],
    )
    def test_same_component_same_station_same_network(self, cmp_a, cmp_b):
        assert is_autocorrelation(cmp_a=cmp_a, cmp_b=cmp_b)

    def test_different_components_same_station_same_network(self):
        assert False

    def test_different_components_different_station_same_network(self):
        assert False

    def test_different_components_different_station_different_network(self):
        assert False

    def test_same_components_different_station_different_network(self):
        assert False

    def test_same_components_same_station_different_network(self):
        assert False


class TestIsIntracorrelation:
    def test_true(self):
        assert False

    def test_different_stations(self):
        assert False


# class TestsCaculateDistanceBackazimuth:
#     def test_

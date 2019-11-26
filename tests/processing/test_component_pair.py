import pytest

from noiz.models import ComponentPair, Component

from noiz.processing.component_pair import (
    is_autocorrelation,
    is_intrastation_correlation,
)


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
                    network="SI", station="SI03", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="SI", station="SI03", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="N", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="SI", station="SI13", component="Z", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XX", station="SI03", component="N", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="N", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="SI", station="SI03", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="XZ", station="SI13", component="N", lat=32.5, lon=8.2
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
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="N", lat=32.5, lon=8.2
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
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="SI", station="SI17", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="N", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XX", station="SI03", component="N", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="XX", station="SI03", component="N", lat=32.5, lon=8.2
                ),
                Component(
                    network="SI", station="SI03", component="Z", lat=32.5, lon=8.2
                ),
            ),
            (
                Component(
                    network="SI", station="SI03", component="E", lat=32.5, lon=8.2
                ),
                Component(
                    network="XZ", station="SI13", component="N", lat=32.5, lon=8.2
                ),
            ),
        ],
    )
    def test_different_component_parameters(self, cmp_a, cmp_b):
        assert is_intrastation_correlation(cmp_a=cmp_a, cmp_b=cmp_b) is False


# class TestsCaculateDistanceBackazimuth:
#     def test_

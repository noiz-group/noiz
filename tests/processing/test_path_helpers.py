import pytest
from obspy.core.inventory import Network, Station, Longitude, Latitude

from noiz.processing.path_helpers import _assembly_stationxml_filename


class TestAssemblySationXMLFilename:
    @pytest.mark.parametrize(
        "network_name, station_name, component_name, counter",
        (
            ("SI", "SI13", "Z", 1),
            ("XY", "GH", "X", 3),
            ("ZZZZ", "BG", "U", 7),
            ("WT", "GRT", "N", 10),
        ),
    )
    def test_assembly_stationxml_filename(self, network_name, station_name, component_name, counter):
        network = Network(code=network_name)
        station = Station(
            code=station_name, longitude=Longitude(1), latitude=Latitude(2), elevation=3
        )
        component = component_name
        assembled_name = _assembly_stationxml_filename(
            network=network, station=station, component=component, counter=counter
        )
        assert (
            assembled_name
            == f"inventory_{network_name}.{station_name}.{component_name}.xml.{counter}"
        )

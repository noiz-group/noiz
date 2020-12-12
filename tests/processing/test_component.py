import pytest
from pathlib import Path
import obspy

from obspy.core.inventory.network import Network
from obspy.core.inventory.station import Station, Longitude, Latitude
from obspy.core.inventory.channel import Channel

from noiz.models.component import Component
from noiz.models.component_pair import ComponentPair

from noiz.processing.component import _assembly_single_component_inventory, divide_channels_by_component, \
    _assembly_stationxml_filename, read_inventory


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


class TestDivideChannelsByComponent:
    @pytest.mark.xfail
    def test_divide_channels_by_component(self):
        assert False


class TestAssemblySingleComponentInventory:
    @pytest.mark.xfail
    def test_assembly_single_component_inventory(self):
        assert False


def test_read_inventory():
    test_file_path = Path(__file__).parent.joinpath("data/inventory.xml")
    read_inv = obspy.read_inventory(str(test_file_path))
    assert read_inv == read_inventory(Path(test_file_path))

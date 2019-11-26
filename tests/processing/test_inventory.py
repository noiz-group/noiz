import pytest

from noiz.models import ComponentPair, Component

from noiz.processing.inventory import (
    _assembly_single_component_inventory,
    _assembly_stationxml_filename,
    read_inventory,
    divide_channels_by_component,
)


class TestAssemblySationxmlGilename:
    @pytest.mark.xfail
    def test_assembly_stationxml_filename(self):
        assert False


@pytest.mark.xfail
def test_read_inventory(self):

    assert False

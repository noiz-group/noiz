# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pytest
from pathlib import Path
import obspy

from noiz.processing.component import read_inventory


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

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import os
from pathlib import Path
import pytest
from obspy.core.inventory import Network, Station, Longitude, Latitude

from noiz.processing.path_helpers import _assembly_stationxml_filename, increment_filename_counter


class TestAssemblySationXMLFilename:
    @pytest.mark.parametrize(
        "network_name, station_name, component_name, counter",
        (
            ("TD", "TD13", "Z", 1),
            ("XY", "GH", "X", 3),
            ("ZZZZ", "BG", "U", 7),
            ("WT", "GRT", "N", 10),
        ),
    )
    def test_assembly_stationxml_filename(self, network_name, station_name, component_name, counter):
        network = Network(code=network_name)
        station = Station(code=station_name, longitude=Longitude(1), latitude=Latitude(2), elevation=3)
        component = component_name
        assembled_name = _assembly_stationxml_filename(
            network=network, station=station, component=component, counter=counter
        )
        assert assembled_name == f"inventory_{network_name}.{station_name}.{component_name}.xml.{counter}"


def test_increment_filename_counter(tmp_path):
    filepath_in = tmp_path.joinpath("this_filename_to_iterate_counter.0")
    filepath_in.touch()
    expected_filepath = tmp_path.joinpath("this_filename_to_iterate_counter.1")

    obtained_fpath = increment_filename_counter(filepath=filepath_in, extension=False)

    assert obtained_fpath == expected_filepath
    assert isinstance(obtained_fpath, Path)


def test_increment_filename_counter_higher_count(tmp_path):
    starting_fpath = tmp_path.joinpath("this_filename_to_iterate_counter.0")
    for i in range(21):
        filepath_in = starting_fpath.with_suffix(f"{os.extsep}{i}")
        filepath_in.touch()

    expected_filepath = tmp_path.joinpath("this_filename_to_iterate_counter.21")

    assert increment_filename_counter(filepath=starting_fpath, extension=False) == expected_filepath


def test_increment_filename_counter_with_extension(tmp_path):
    filepath_in = tmp_path.joinpath("this_filename_to_iterate_counter.0.npz")
    filepath_in.touch()
    expected_filepath = tmp_path.joinpath("this_filename_to_iterate_counter.1.npz")

    assert increment_filename_counter(filepath=filepath_in, extension=True) == expected_filepath


def test_increment_filename_counter_with_extension_raising(tmp_path):
    filepath_in = tmp_path.joinpath("this_filename_to_iterate_counter.0")
    filepath_in.touch()

    with pytest.raises(ValueError):
        increment_filename_counter(filepath=filepath_in, extension=True)

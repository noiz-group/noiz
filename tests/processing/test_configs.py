# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import pytest
import toml

from noiz.models.processing_params import DatachunkParamsHolder
from noiz.processing.configs import parse_single_config_toml


def test_parse_single_config_toml_datachunkparams_datachunk_params_type_defined_in_toml(tmp_path):
    params = {
        "DatachunkParams": {
            "sampling_rate": 24,
            "prefiltering_low": 0.01,
            "prefiltering_high": 11.0,
            "prefiltering_order": 4,
            "preprocessing_taper_type": "cosine",
            "preprocessing_taper_side": "both",
            "preprocessing_taper_max_length": 5,
            "preprocessing_taper_max_percentage": 0.1,
            "remove_response": True,
            "datachunk_sample_tolerance": 0.02,
            "zero_padding_method": "padding_with_tapering",
            "padding_taper_type": "cosine",
            "padding_taper_max_length": 5,
            "padding_taper_max_percentage": 0.0,
        },
    }

    test_file = tmp_path.joinpath("test_file.toml")
    with open(test_file, "w") as f:
        toml.dump(o=params, f=f)

    parsed_config = parse_single_config_toml(filepath=test_file)

    assert isinstance(parsed_config, DatachunkParamsHolder)
    for key in params["DatachunkParams"].keys():
        assert params["DatachunkParams"][key] == parsed_config.__getattribute__(key)


def test_parse_single_config_toml_datachunkparams_datachunk_params_provided(tmp_path):
    params = {
        "sampling_rate": 24,
        "prefiltering_low": 0.01,
        "prefiltering_high": 11.0,
        "prefiltering_order": 4,
        "preprocessing_taper_type": "cosine",
        "preprocessing_taper_side": "both",
        "preprocessing_taper_max_length": 5,
        "preprocessing_taper_max_percentage": 0.1,
        "remove_response": True,
        "datachunk_sample_tolerance": 0.02,
        "zero_padding_method": "padding_with_tapering",
        "padding_taper_type": "cosine",
        "padding_taper_max_length": 5,
        "padding_taper_max_percentage": 0.0,
    }

    test_file = tmp_path.joinpath("test_file.toml")
    with open(test_file, "w") as f:
        toml.dump(o=params, f=f)

    parsed_config = parse_single_config_toml(filepath=test_file, config_type="DatachunkParams")

    assert isinstance(parsed_config, DatachunkParamsHolder)
    for key in params.keys():
        assert params[key] == parsed_config.__getattribute__(key)


def test_parse_single_config_toml_datachunkparams_datachunk_params_both(tmp_path):
    params = {
        "DatachunkParams": {
            "sampling_rate": 24,
            "prefiltering_low": 0.01,
            "prefiltering_high": 11.0,
            "prefiltering_order": 4,
            "preprocessing_taper_type": "cosine",
            "preprocessing_taper_side": "both",
            "preprocessing_taper_max_length": 5,
            "preprocessing_taper_max_percentage": 0.1,
            "remove_response": True,
            "datachunk_sample_tolerance": 0.02,
            "zero_padding_method": "padding_with_tapering",
            "padding_taper_type": "cosine",
            "padding_taper_max_length": 5,
            "padding_taper_max_percentage": 0.0,
        },
    }

    test_file = tmp_path.joinpath("test_file.toml")
    with open(test_file, "w") as f:
        toml.dump(o=params, f=f)

    parsed_config = parse_single_config_toml(filepath=test_file, config_type="DatachunkParams")

    assert isinstance(parsed_config, DatachunkParamsHolder)
    for key in params["DatachunkParams"].keys():
        assert params["DatachunkParams"][key] == parsed_config.__getattribute__(key)


@pytest.mark.xfail
def test_parse_single_config_toml_datachunkparams_qc_one_defined_in_toml(tmp_path):
    assert False


@pytest.mark.xfail
def test_parse_single_config_toml_datachunkparams_qc_one_provided(tmp_path):
    assert False


@pytest.mark.xfail
def test_parse_single_config_toml_datachunkparams_qc_one_both(tmp_path):
    assert False


def test_parse_single_config_toml_raise_on_no_type_provided(tmp_path):
    params = {
        "value1": 24,
        "value2": 24,
    }
    test_file = tmp_path.joinpath("test_file.toml")
    with open(test_file, "w") as f:
        toml.dump(o=params, f=f)

    with pytest.raises(ValueError):
        parse_single_config_toml(filepath=test_file)


def test_parse_single_config_toml_raise_on_wrong_type_provided(tmp_path):
    params = {
        "value1": 24,
        "value2": 24,
    }
    test_file = tmp_path.joinpath("test_file.toml")
    with open(test_file, "w") as f:
        toml.dump(o=params, f=f)

    with pytest.raises(ValueError):
        parse_single_config_toml(filepath=test_file, config_type="wrong_type")


def test_parse_single_config_toml_raise_on_wrong_type_read(tmp_path):
    params = {
        "wrong_type": {
            "value1": 24,
            "value2": 24,
        },
    }
    test_file = tmp_path.joinpath("test_file.toml")
    with open(test_file, "w") as f:
        toml.dump(o=params, f=f)

    with pytest.raises(ValueError):
        parse_single_config_toml(filepath=test_file)


def test_parse_single_config_toml_raise_on_different_types_provided_and_read(tmp_path):
    from noiz.processing.configs import DefinedConfigs

    params = {
        f"{DefinedConfigs.DATACHUNKPARAMS.value}": {
            "value1": 24,
            "value2": 24,
        },
    }

    test_file = tmp_path.joinpath("test_file.toml")
    with open(test_file, "w") as f:
        toml.dump(o=params, f=f)

    with pytest.raises(ValueError):
        parse_single_config_toml(filepath=test_file, config_type=DefinedConfigs.QCONE.value)

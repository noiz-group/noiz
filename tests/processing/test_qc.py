import pytest

from noiz.processing.qc import parse_single_config_toml


@pytest.mark.xfail
def test_parse_single_config_toml_datachunkparams(tmp_path):
    assert False


def test_parse_single_config_toml_raise_on_no_type_provided(tmp_path):
    params = {
        "value1": 24,
        "value2": 24,
    }
    test_file = tmp_path.joinpath('test_file.toml')
    import toml
    with open(test_file, 'w') as f:
        toml.dump(o=params, f=f)

    with pytest.raises(ValueError):
        parse_single_config_toml(filepath=test_file)


def test_parse_single_config_toml_raise_on_wrong_type_provided(tmp_path):
    params = {
        "value1": 24,
        "value2": 24,
    }
    test_file = tmp_path.joinpath('test_file.toml')
    import toml
    with open(test_file, 'w') as f:
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
    test_file = tmp_path.joinpath('test_file.toml')
    import toml
    with open(test_file, 'w') as f:
        toml.dump(o=params, f=f)

    with pytest.raises(ValueError):
        parse_single_config_toml(filepath=test_file)


def test_parse_single_config_toml_raise_on_different_types_provided_and_read(tmp_path):
    from noiz.processing.qc import DefinedConfigs
    params = {
        f"{DefinedConfigs.DATACHUNKPARAMS.value}": {
            "value1": 24,
            "value2": 24,
        },
    }

    test_file = tmp_path.joinpath('test_file.toml')
    import toml
    with open(test_file, 'w') as f:
        toml.dump(o=params, f=f)

    with pytest.raises(ValueError):
        parse_single_config_toml(filepath=test_file, config_type=DefinedConfigs.QCONE.value)

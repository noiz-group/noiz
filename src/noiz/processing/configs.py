import toml

from typing import Dict, Optional, Union
from pathlib import Path

from noiz.globals import ExtendedEnum
from noiz.models.processing_params import DatachunkParamsHolder
from noiz.models.qc import QCOneRejectedTimeHolder, QCOneHolder


class DefinedConfigs(ExtendedEnum):
    # filldocs
    DATACHUNKPARAMS = "DatachunkParams"
    QCONE = "QCOne"


def select_validator_for_config_type(config_type: DefinedConfigs):
    if config_type is DefinedConfigs.DATACHUNKPARAMS:
        return validate_config_dict_as_datachunkparams
    elif config_type is DefinedConfigs.QCONE:
        return validate_dict_as_qcone_holder


def parse_single_config_toml(filepath: Path, config_type: Optional[Union[str, DefinedConfigs]] = None):
    config_type_read = None
    config_type_provided = None

    if not filepath.exists() or not filepath.is_file():
        raise ValueError("Provided filepath has to be a path to existing file")

    with open(file=filepath, mode='r') as f:
        loaded_dict: Dict = toml.load(f=f)  # type: ignore

    if len(loaded_dict.keys()) != 1 and config_type is None:
        raise ValueError(f"You have to provide either a config_type argument or indicate in your toml type of config."
                         f"Allowed config types are: {list(DefinedConfigs)}")

    if config_type is not None:
        if isinstance(config_type, DefinedConfigs):
            config_type_provided = config_type
        else:
            try:
                config_type_provided = DefinedConfigs(config_type)
            except ValueError:
                raise ValueError(f"Wrong config_type value provided. You provided `{config_type}`. "
                                 f"Only accepted ones are: {list(DefinedConfigs)}")

    if len(loaded_dict.keys()) == 1:
        read_value = list(loaded_dict.keys())[0]
        try:
            config_type_read = DefinedConfigs(read_value)
        except ValueError:
            raise ValueError(f"Your TOML file contained wrong section header name. Value read from file `{read_value}`."
                             f" Only accepted ones are: {list(DefinedConfigs)}")
        loaded_dict = loaded_dict[config_type_read.value]

    if len(loaded_dict.keys()) == 1 and config_type is not None:
        if config_type_provided != config_type_read:
            raise ValueError(f"Config type read from TOML file and provided by user are different."
                             f"Read: {config_type_read} "
                             f"Provided by user: {config_type_read} ")

    config_type_selected = [x for x in (config_type_read, config_type_provided) if x is not None][0]

    validator = select_validator_for_config_type(config_type=config_type_selected)

    return validator(loaded_dict)


def load_qc_one_config_toml(filepath: Path) -> QCOneHolder:
    """
    This method loads the TOML config file, validates it and returns a :class:`~noiz.models.QCOneHolder` that is
    compatible with constructor of :class:`~noiz.models.QCOneConfig`

    :param filepath: Path to existing QCOne config TOML file
    :type filepath: Path
    :return: QCOneHolder compatible with constructor of QCOne model
    :rtype: QCOneHolder
    """
    import warnings
    warnings.warn(DeprecationWarning("this method is deprecated, use more general `parse_single_config_toml`"))

    if not filepath.exists() or not filepath.is_file():
        raise ValueError("Provided filepath has to be a path to existing file")

    with open(file=filepath, mode='r') as f:
        loaded_config: Dict = toml.load(f=f)  # type: ignore

    return validate_dict_as_qcone_holder(loaded_config)


def validate_dict_as_qcone_holder(loaded_dict: Dict) -> QCOneHolder:
    """
    Takes a dict, or an output from TOML parser and tries to convert it into a :class:`~noiz.models.QCOneHolder` object

    :param loaded_dict: Dictionary to be parsed and validated as QCOneHolder
    :type loaded_dict: Dict
    :return: Valid QCOneHolder object
    :rtype: QCOneHolder
    """

    processed_dict = loaded_dict.copy()

    validated_forbidden_channels = []
    for forb_chn in loaded_dict['rejected_times']:
        validated_forbidden_channels.append(QCOneRejectedTimeHolder(**forb_chn))
    processed_dict['rejected_times'] = validated_forbidden_channels

    return QCOneHolder(**processed_dict)


def validate_config_dict_as_datachunkparams(loaded_dict: Dict) -> DatachunkParamsHolder:
    # filldocs
    return DatachunkParamsHolder(**loaded_dict)

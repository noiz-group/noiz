import toml

from typing import Any, MutableMapping, Dict, Union
from pathlib import Path

from noiz.models.qc import QCOneRejectedTimeHolder, QCOneHolder


def load_qc_one_config_toml(filepath: Path) -> QCOneHolder:
    """
    This method loads the TOML config file, validates it and returns a :class:`~noiz.models.QCOneHolder` that is
    compatible with constructor of :class:`~noiz.models.QCOneConfig`

    :param filepath: Path to existing QCOne config TOML file
    :type filepath: Path
    :return: QCOneHolder compatible with constructor of QCOne model
    :rtype: QCOneHolder
    """

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

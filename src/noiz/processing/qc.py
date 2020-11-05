import datetime
import toml

from dataclasses import dataclass
from typing import Optional, List, Any, MutableMapping, Dict, Union
from pathlib import Path

from noiz.models import QCOne, QCOneRejectedTime


@dataclass
class QCOneRejectedTimeHolder:
    """
        This simple dataclass is just helping to validate :class:`~noiz.models.QCOneRejectedTime` values loaded
        from the TOML file
    """
    network: str
    station: str
    component: str
    starttime: datetime.datetime
    endtime: datetime.datetime


@dataclass
class QCOneHolder:
    """
    This simple dataclass is just helping to validate :class:`~noiz.models.QCOne` values loaded from the TOML file
    """

    starttime: datetime.datetime
    endtime: datetime.datetime
    avg_gps_time_error_min: float
    avg_gps_time_error_max: float
    avg_gps_time_uncertainty_min: float
    avg_gps_time_uncertainty_max: float
    forbidden_channels: List[QCOneRejectedTimeHolder]


def load_qc_one_config_toml(filepath: Path) -> QCOneHolder:
    """
    This method loads the TOML config file, validates it and returns a QCOneHolder that is compatible with
    constructor of :class:`~noiz.models.QCOne`

    :param filepath: Path to existing QCOne config TOML file
    :type filepath: Path
    :return: QCOneHolder compatible with constructor of QCOne model
    :rtype: QCOneHolder
    """

    if not filepath.exists() or not filepath.is_file():
        raise ValueError("Provided filepath has to be a path to existing file")

    with open(file=filepath, mode='r') as f:
        loaded_config = toml.load(f=f)

    return validate_dict_as_qcone_holder(loaded_config)


def validate_dict_as_qcone_holder(loaded_config: Union[Dict, MutableMapping[str, Any]]) -> QCOneHolder:
    validated_forbidden_channels = []
    for forb_chn in loaded_config['forbidden_channels']:
        validated_forbidden_channels.append(QCOneRejectedTimeHolder(**forb_chn))
    loaded_config['forbidden_channels'] = validated_forbidden_channels
    return QCOneHolder(**loaded_config)

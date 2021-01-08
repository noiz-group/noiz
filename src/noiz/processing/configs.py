import toml

from typing import Dict, Optional, Union
from pathlib import Path

from noiz.globals import ExtendedEnum
from noiz.models import DatachunkParams
from noiz.models.processing_params import DatachunkParamsHolder, ProcessedDatachunkParamsHolder, \
    ProcessedDatachunkParams
from noiz.models.qc import QCOneConfigRejectedTimeHolder, QCOneConfigHolder


class DefinedConfigs(ExtendedEnum):
    # filldocs
    DATACHUNKPARAMS = "DatachunkParams"
    PROCESSEDDATACHUNKPARAMS = "ProcessedDatachunkParams"
    QCONE = "QCOne"


def _select_validator_for_config_type(config_type: DefinedConfigs):
    """
    filldocs
    """
    if config_type is DefinedConfigs.DATACHUNKPARAMS:
        return validate_config_dict_as_datachunkparams
    elif config_type is DefinedConfigs.PROCESSEDDATACHUNKPARAMS:
        return validate_config_dict_as_processeddatachunkparams
    elif config_type is DefinedConfigs.QCONE:
        return validate_dict_as_qcone_holder
    else:
        raise NotImplementedError(f"There is no validator specified for {config_type}")


def parse_single_config_toml(filepath: Path, config_type: Optional[Union[str, DefinedConfigs]] = None):
    """
    # filldocs
    """
    config_type_read = None
    config_type_provided = None

    if not filepath.exists() or not filepath.is_file():
        raise ValueError("Provided filepath has to be a path to existing file")

    with open(file=filepath, mode='r') as f:
        loaded_dict: Dict = toml.load(f=f)  # type: ignore

    single_keyed_dict = len(loaded_dict.keys()) == 1

    if not single_keyed_dict and config_type is None:
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

    if single_keyed_dict:
        read_value = list(loaded_dict.keys())[0]
        try:
            config_type_read = DefinedConfigs(read_value)
        except ValueError:
            raise ValueError(f"Your TOML file contained wrong section header name. Value read from file `{read_value}`."
                             f" Only accepted ones are: {list(DefinedConfigs)}")
        loaded_dict = loaded_dict[config_type_read.value]

    if single_keyed_dict and config_type is not None:
        if config_type_provided != config_type_read:
            raise ValueError(f"Config type read from TOML file and provided by user are different."
                             f"Read: {config_type_read} "
                             f"Provided by user: {config_type_read} ")

    config_type_selected = [x for x in (config_type_read, config_type_provided) if x is not None][0]

    validator = _select_validator_for_config_type(config_type=config_type_selected)

    return validator(loaded_dict)


def load_qc_one_config_toml(filepath: Path) -> QCOneConfigHolder:
    """
    This method loads the TOML config file, validates it and returns a :class:`~noiz.models.QCOneConfigHolder` that is
    compatible with constructor of :class:`~noiz.models.QCOneConfig`

    :param filepath: Path to existing QCOne config TOML file
    :type filepath: Path
    :return: QCOneConfigHolder compatible with constructor of QCOne model
    :rtype: QCOneConfigHolder
    """
    import warnings
    warnings.warn(DeprecationWarning("this method is deprecated, use more general `parse_single_config_toml`"))

    if not filepath.exists() or not filepath.is_file():
        raise ValueError("Provided filepath has to be a path to existing file")

    with open(file=filepath, mode='r') as f:
        loaded_config: Dict = toml.load(f=f)  # type: ignore

    return validate_dict_as_qcone_holder(loaded_config)


def validate_dict_as_qcone_holder(loaded_dict: Dict) -> QCOneConfigHolder:
    """
    Takes a dict, or an output from TOML parser and tries to convert it into a :class:`~noiz.models.QCOneConfigHolder` object

    :param loaded_dict: Dictionary to be parsed and validated as QCOneConfigHolder
    :type loaded_dict: Dict
    :return: Valid QCOneConfigHolder object
    :rtype: QCOneConfigHolder
    """

    processed_dict = loaded_dict.copy()

    validated_forbidden_channels = []
    for forb_chn in loaded_dict['rejected_times']:
        validated_forbidden_channels.append(QCOneConfigRejectedTimeHolder(**forb_chn))
    processed_dict['rejected_times'] = validated_forbidden_channels

    return QCOneConfigHolder(**processed_dict)


def validate_config_dict_as_datachunkparams(loaded_dict: Dict) -> DatachunkParamsHolder:
    # filldocs
    return DatachunkParamsHolder(**loaded_dict)


def validate_config_dict_as_processeddatachunkparams(loaded_dict: Dict) -> ProcessedDatachunkParamsHolder:
    # filldocs
    return ProcessedDatachunkParamsHolder(**loaded_dict)


def create_datachunkparams(
        params_holder: Optional[DatachunkParamsHolder] = None,
        **kwargs,
) -> DatachunkParams:
    """
    This method takes a :class:`~noiz.models.processing_params.DatachunkParamsHolder` instance and based on it creates
    an instance of database model :class:`~noiz.models.processing_params.DatachunkParams`.

    Optionally, it can create the instance of :class:`~noiz.models.processing_params.DatachunkParamsHolder` from
    provided kwargs, but why dont you do it on your own to ensure that it will get everything it needs?

    :param params_holder: Object containing all required elements to create a DatachunkParams instance
    :type params_holder: DatachunkParamsHolder
    :param kwargs: Optional kwargs to create DatachunkParamsHolder
    :return: Working DatachunkParams model that needs to be inserted into db
    :rtype: DatachunkParams
    """

    if params_holder is None:
        params_holder = validate_config_dict_as_datachunkparams(kwargs)

    params = DatachunkParams(
        sampling_rate=params_holder.sampling_rate,
        prefiltering_low=params_holder.prefiltering_low,
        prefiltering_high=params_holder.prefiltering_high,
        prefiltering_order=params_holder.prefiltering_order,
        preprocessing_taper_type=params_holder.preprocessing_taper_type,
        preprocessing_taper_side=params_holder.preprocessing_taper_side,
        preprocessing_taper_max_length=params_holder.preprocessing_taper_max_length,
        preprocessing_taper_max_percentage=params_holder.preprocessing_taper_max_percentage,
        remove_response=params_holder.remove_response,
        datachunk_sample_tolerance=params_holder.datachunk_sample_tolerance,
        zero_padding_method=params_holder.zero_padding_method,
        padding_taper_type=params_holder.padding_taper_type,
        padding_taper_max_length=params_holder.padding_taper_max_length,
        padding_taper_max_percentage=params_holder.padding_taper_max_percentage,
    )
    return params


def create_processed_datachunk_params(
        params_holder: Optional[ProcessedDatachunkParamsHolder] = None,
        **kwargs,
) -> ProcessedDatachunkParams:
    """
    This method takes a :py:class:`~noiz.models.processing_params.ProcessedDatachunkParamsHolder` instance and based on
    it creates an instance of database model :py:class:`~noiz.models.processing_params.ProcessedDatachunkParams`.

    Optionally, it can create the instance of :py:class:`~noiz.models.processing_params.ProcessedDatachunkParamsHolder`
    from provided kwargs, but why dont you do it on your own to ensure that it will get everything it needs?

    :param params_holder: Object containing all required elements to create a ProcessedDatachunkParams instance
    :type params_holder: ProcessedDatachunkParamsHolder
    :param kwargs: Optional kwargs to create ProcessedDatachunkParamsHolder
    :return: Working ProcessedDatachunkParams model that needs to be inserted into db
    :rtype: ProcessedDatachunkParams
    """

    if params_holder is None:
        params_holder = validate_config_dict_as_processeddatachunkparams(kwargs)

    params = ProcessedDatachunkParams(
        datachunk_params_id=params_holder.datachunk_params_id,
        qcone_config_id=params_holder.qcone_config_id,
        spectral_whitening=params_holder.spectral_whitening,
        one_bit=params_holder.one_bit,
    )
    return params

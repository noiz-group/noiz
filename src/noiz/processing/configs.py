import toml

from typing import Dict, Optional, Union
from pathlib import Path

from noiz.globals import ExtendedEnum
from noiz.models import DatachunkParams
from noiz.models.processing_params import DatachunkParamsHolder, ProcessedDatachunkParamsHolder, \
    ProcessedDatachunkParams, CrosscorrelationParamsHolder, CrosscorrelationParams, BeamformingParamsHolder, \
    BeamformingParams
from noiz.models.qc import QCOneConfigRejectedTimeHolder, QCOneConfigHolder, QCTwoConfigHolder, \
    QCTwoConfigRejectedTimeHolder
from noiz.models.stacking import StackingSchemaHolder, StackingSchema


class DefinedConfigs(ExtendedEnum):
    # filldocs
    DATACHUNKPARAMS = "DatachunkParams"
    BEAMFORMINGPARAMS = "BeamformingParams"
    PROCESSEDDATACHUNKPARAMS = "ProcessedDatachunkParams"
    CROSSCORRELATIONPARAMS = "CrosscorrelationParams"
    QCONE = "QCOne"
    QCTWO = "QCTwo"
    STACKINGSCHEMA = "StackingSchema"


def _select_validator_for_config_type(config_type: DefinedConfigs):
    """
    filldocs
    """
    if config_type is DefinedConfigs.DATACHUNKPARAMS:
        return validate_config_dict_as_datachunkparams
    elif config_type is DefinedConfigs.PROCESSEDDATACHUNKPARAMS:
        return validate_config_dict_as_processeddatachunkparams
    elif config_type is DefinedConfigs.BEAMFORMINGPARAMS:
        return validate_config_dict_as_beamformingparams
    elif config_type is DefinedConfigs.CROSSCORRELATIONPARAMS:
        return validate_config_dict_as_crosscorrelationparams
    elif config_type is DefinedConfigs.QCONE:
        return validate_dict_as_qcone_holder
    elif config_type is DefinedConfigs.QCTWO:
        return validate_dict_as_qctwo_holder
    elif config_type is DefinedConfigs.STACKINGSCHEMA:
        return validate_config_dict_as_stacking_schema
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


def validate_dict_as_qcone_holder(loaded_dict: Dict) -> QCOneConfigHolder:
    """
    Takes a dict, or an output from TOML parser and tries to convert it into a :class:`~noiz.models.QCOneConfigHolder` object

    :param loaded_dict: Dictionary to be parsed and validated as QCOneConfigHolder
    :type loaded_dict: Dict
    :return: Valid QCOneConfigHolder object
    :rtype: QCOneConfigHolder
    """

    processed_dict = loaded_dict.copy()

    if "rejected_times" in loaded_dict.keys():
        validated_forbidden_channels = []
        for forb_chn in loaded_dict["rejected_times"]:
            validated_forbidden_channels.append(QCOneConfigRejectedTimeHolder(**forb_chn))
        processed_dict["rejected_times"] = validated_forbidden_channels

    return QCOneConfigHolder(**processed_dict)


def validate_dict_as_qctwo_holder(loaded_dict: Dict) -> QCTwoConfigHolder:
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
        validated_forbidden_channels.append(QCTwoConfigRejectedTimeHolder(**forb_chn))
    processed_dict['rejected_times'] = validated_forbidden_channels

    return QCTwoConfigHolder(**processed_dict)


def validate_config_dict_as_datachunkparams(loaded_dict: Dict) -> DatachunkParamsHolder:
    # filldocs
    return DatachunkParamsHolder(**loaded_dict)


def validate_config_dict_as_processeddatachunkparams(loaded_dict: Dict) -> ProcessedDatachunkParamsHolder:
    # filldocs
    return ProcessedDatachunkParamsHolder(**loaded_dict)


def validate_config_dict_as_beamformingparams(loaded_dict: Dict) -> BeamformingParamsHolder:
    # filldocs
    return BeamformingParamsHolder(**loaded_dict)


def validate_config_dict_as_crosscorrelationparams(loaded_dict: Dict) -> CrosscorrelationParamsHolder:
    # filldocs
    return CrosscorrelationParamsHolder(**loaded_dict)


def validate_config_dict_as_stacking_schema(loaded_dict: Dict) -> StackingSchemaHolder:
    # filldocs
    return StackingSchemaHolder(**loaded_dict)


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


def create_beamforming_params(
        params_holder: Optional[BeamformingParamsHolder] = None,
        **kwargs,
) -> BeamformingParams:
    """
    This method takes a :py:class:`~noiz.models.processing_params.BeamformingParamsHolder` instance and based on
    it creates an instance of database model :py:class:`~noiz.models.processing_params.BeamformingParams`.

    Optionally, it can create the instance of :py:class:`~noiz.models.processing_params.BeamformingParamsHolder`
    from provided kwargs, but why dont you do it on your own to ensure that it will get everything it needs?

    :param params_holder: Object containing all required elements to create a ProcessedDatachunkParams instance
    :type params_holder: BeamformingParamsHolder
    :param kwargs: Optional kwargs to create BeamformingParamsHolder
    :return: Working BeamformingParams model that needs to be inserted into db
    :rtype: ProcessedDatachunkParams
    """

    if params_holder is None:
        params_holder = validate_config_dict_as_beamformingparams(kwargs)

    params = BeamformingParams(
        qcone_config_id=params_holder.qcone_config_id,
        min_freq=params_holder.min_freq,
        max_freq=params_holder.max_freq,
        slowness_x_min=params_holder.slowness_x_min,
        slowness_x_max=params_holder.slowness_x_max,
        slowness_y_min=params_holder.slowness_y_min,
        slowness_y_max=params_holder.slowness_y_max,
        slowness_step=params_holder.slowness_step,
        semplance_threshold=params_holder.semplance_threshold,
        velocity_threshold=params_holder.velocity_threshold,
        window_length=params_holder.window_length,
        window_step=params_holder.window_step,
        prewhiten=params_holder.prewhiten,
        method=params_holder.method,
    )
    return params


def create_crosscorrelation_params(
        params_holder: CrosscorrelationParamsHolder,
        processed_params: ProcessedDatachunkParams,
) -> CrosscorrelationParams:
    """
    This method takes a :py:class:`~noiz.models.processing_params.CrosscorrelationParamsHolder` instance and based on
    it creates an instance of database model :py:class:`~noiz.models.processing_params.CrosscorrelationParams`.

    :param params_holder: Object containing all required elements to create a CrosscorrelationParams instance
    :type params_holder: CrosscorrelationParamsHolder
    :param processed_params: ProcessedDatachunkParams to be associated with this set of params. \
    It has to include eager loaded DatachunkParams
    :type processed_params: ProcessedDatachunkParams
    :return: Working CrosscorrelationParams model that needs to be inserted into db
    :rtype: CrosscorrelationParams
    """

    params = CrosscorrelationParams(
        processed_datachunk_params_id=params_holder.processed_datachunk_params_id,
        correlation_max_lag=params_holder.correlation_max_lag,
        sampling_rate=processed_params.datachunk_params.sampling_rate,
    )
    return params


def create_stacking_params(
        params_holder: StackingSchemaHolder,
) -> StackingSchema:
    """
    This method takes a :py:class:`~noiz.models.processing_params.CrosscorrelationParamsHolder` instance and based on
    it creates an instance of database model :py:class:`~noiz.models.processing_params.CrosscorrelationParams`.

    :param params_holder: Object containing all required elements to create a CrosscorrelationParams instance
    :type params_holder: CrosscorrelationParamsHolder
    :param processed_params: ProcessedDatachunkParams to be associated with this set of params. \
    It has to include eager loaded DatachunkParams
    :type processed_params: ProcessedDatachunkParams
    :return: Working CrosscorrelationParams model that needs to be inserted into db
    :rtype: CrosscorrelationParams
    """

    params = StackingSchema(
        qctwo_config_id=params_holder.qctwo_config_id,
        minimum_ccf_count=params_holder.minimum_ccf_count,
        starttime=params_holder.starttime,
        endtime=params_holder.endtime,
        stacking_length=params_holder.stacking_length,
        stacking_step=params_holder.stacking_step,
        stacking_overlap=params_holder.stacking_overlap,
    )
    return params

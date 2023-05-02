# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import copy

import numpy as np
import toml
from loguru import logger

from typing import Dict, Optional, Union, List
from pathlib import Path

from noiz.globals import ExtendedEnum
from noiz.models.processing_params import DatachunkParams, DatachunkParamsHolder, ProcessedDatachunkParamsHolder, \
    ProcessedDatachunkParams, CrosscorrelationParamsHolder, CrosscorrelationParams, BeamformingParamsHolder, \
    BeamformingParams, PPSDParamsHolder, PPSDParams, EventDetectionParamsHolder, EventDetectionParams, EventConfirmationParamsHolder, \
    EventConfirmationParams
from noiz.models.qc import QCOneConfigRejectedTimeHolder, QCOneConfigHolder, QCTwoConfigHolder, \
    QCTwoConfigRejectedTimeHolder
from noiz.models.stacking import StackingSchemaHolder, StackingSchema


class DefinedConfigs(ExtendedEnum):
    # filldocs
    DATACHUNKPARAMS = "DatachunkParams"
    BEAMFORMINGPARAMS = "BeamformingParams"
    PPSDPARAMS = "PPSDParams"
    PROCESSEDDATACHUNKPARAMS = "ProcessedDatachunkParams"
    CROSSCORRELATIONPARAMS = "CrosscorrelationParams"
    EVENTDETECTIONPARAMS = "EventDetectionParams"
    EVENTCONFIRMATIONPARAMS = "EventConfirmationParams"
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
    elif config_type is DefinedConfigs.PPSDPARAMS:
        return validate_config_dict_as_ppsdparams
    elif config_type is DefinedConfigs.CROSSCORRELATIONPARAMS:
        return validate_config_dict_as_crosscorrelationparams
    elif config_type is DefinedConfigs.EVENTDETECTIONPARAMS:
        return validate_config_dict_as_eventdetectionparams
    elif config_type is DefinedConfigs.EVENTCONFIRMATIONPARAMS:
        return validate_config_dict_as_eventconfirmationparams
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


def validate_config_dict_as_ppsdparams(loaded_dict: Dict) -> PPSDParamsHolder:
    # filldocs
    return PPSDParamsHolder(**loaded_dict)


def validate_config_dict_as_crosscorrelationparams(loaded_dict: Dict) -> CrosscorrelationParamsHolder:
    # filldocs
    return CrosscorrelationParamsHolder(**loaded_dict)


def validate_config_dict_as_eventdetectionparams(loaded_dict: Dict) -> EventDetectionParamsHolder:
    # filldocs
    return EventDetectionParamsHolder(**loaded_dict)


def validate_config_dict_as_eventconfirmationparams(loaded_dict: Dict) -> EventConfirmationParamsHolder:
    # filldocs
    return EventConfirmationParamsHolder(**loaded_dict)


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
        response_constant_coefficient=params_holder.response_constant_coefficient,
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
        filtering_low=params_holder.filtering_low,
        filtering_high=params_holder.filtering_high,
        filtering_order=params_holder.filtering_order,
        waterlevel_ratio_to_max=params_holder.waterlevel_ratio_to_max,
        convolution_sliding_window_min_samples=params_holder.convolution_sliding_window_min_samples,
        convolution_sliding_window_max_ratio_to_fmin=params_holder.convolution_sliding_window_max_ratio_to_fmin,
        convolution_sliding_window_ratio_to_bandwidth=params_holder.convolution_sliding_window_ratio_to_bandwidth,
        quefrency_filter_lowpass_pct=params_holder.quefrency_filter_lowpass_pct,
        quefrency_filter_taper_min_samples=params_holder.quefrency_filter_taper_min_samples,
        quefrency_filter_taper_length_ratio_to_length_cepstrum=params_holder.quefrency_filter_taper_length_ratio_to_length_cepstrum,
        spectral_whitening=params_holder.spectral_whitening,
        one_bit=params_holder.one_bit,
        quefrency=params_holder.quefrency
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
        window_length_minimum_periods=params_holder.window_length_minimum_periods,
        window_length=params_holder.window_length,
        window_step_fraction=params_holder.window_step_fraction,
        window_step=params_holder.window_step,
        save_average_beamformer_abspower=params_holder.save_average_beamformer_abspower,
        save_all_beamformers_abspower=params_holder.save_all_beamformers_abspower,
        save_average_beamformer_relpower=params_holder.save_average_beamformer_relpower,
        save_all_beamformers_relpower=params_holder.save_all_beamformers_relpower,
        extract_peaks_average_beamformer_abspower=params_holder.extract_peaks_average_beamformer_abspower,
        extract_peaks_all_beamformers_abspower=params_holder.extract_peaks_all_beamformers_abspower,
        extract_peaks_average_beamformer_relpower=params_holder.extract_peaks_average_beamformer_relpower,
        extract_peaks_all_beamformers_relpower=params_holder.extract_peaks_all_beamformers_relpower,
        neighborhood_size=params_holder.neighborhood_size,
        neighborhood_size_xaxis_fraction=params_holder.neighborhood_size_xaxis_fraction,
        maxima_threshold=params_holder.maxima_threshold,
        best_point_count=params_holder.best_point_count,
        beam_portion_threshold=params_holder.beam_portion_threshold,
        semblance_threshold=params_holder.semblance_threshold,
        velocity_threshold=params_holder.velocity_threshold,
        prewhiten=params_holder.prewhiten,
        method=params_holder.method,
        used_component_codes=params_holder.used_component_codes,
        minimum_trace_count=params_holder.minimum_trace_count,
    )
    return params


def create_ppsd_params(
        params_holder: PPSDParamsHolder,
        datachunk_params: DatachunkParams,
) -> PPSDParams:
    """
    This method takes a :py:class:`~noiz.models.processing_params.PPSDParamsHolder` instance and based on
    it creates an instance of database model :py:class:`~noiz.models.processing_params.PPSDParams`.

    :param params_holder: Object containing all required elements to create a PPSDParams instance
    :type params_holder: PPSDParams
    :param datachunk_params: Datachunk params which this PPSD params are associated with
    :type datachunk_params: DatachunkParams
    :return: Working PPSDParams model that needs to be inserted into db
    :rtype: PPSDParams
    """

    if (params_holder.datachunk_params_id != datachunk_params.id) or not isinstance(datachunk_params, DatachunkParams):
        raise ValueError("Expected DatachunkParams that have the same id as passed within the PPSDParamsHolder. "
                         "Got something different.")

    params = PPSDParams(
        datachunk_params_id=params_holder.datachunk_params_id,
        segment_length=params_holder.segment_length,
        segment_step=params_holder.segment_step,
        freq_min=params_holder.freq_min,
        freq_max=params_holder.freq_max,
        resample=params_holder.resample,
        resampled_frequency_start=params_holder.resampled_frequency_start,
        resampled_frequency_stop=params_holder.resampled_frequency_stop,
        resampled_frequency_step=params_holder.resampled_frequency_step,
        rejected_windows_quantile=params_holder.rejected_windows_quantile,
        save_all_windows=params_holder.save_all_windows,
        save_compressed=params_holder.save_compressed,
        sampling_rate=datachunk_params.sampling_rate,
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


def create_event_detection_params(
        params_holder: EventDetectionParamsHolder,
) -> EventDetectionParams:
    """
    This method takes a :py:class:`~noiz.models.processing_params.EventDetectionParamsHolder` instance and based on
    it creates an instance of database model :py:class:`~noiz.models.processing_params.EventDetectionParams`.

    :param params_holder: Object containing all required elements to create a EventDetectionParams instance
    :type params_holder: EventDetectionParamsHolder
    :return: Working EventDetectionParams model that needs to be inserted into db
    :rtype: EventDetectionParams
    """

    params = EventDetectionParams(
        detection_type=params_holder.detection_type,
        n_short_time_average=params_holder.n_short_time_average,
        n_long_time_average=params_holder.n_long_time_average,
        datachunk_params_id=params_holder.datachunk_params_id,
        trigger_value=params_holder.trigger_value,
        detrigger_value=params_holder.detrigger_value,
        peak_ground_velocity_threshold=params_holder.peak_ground_velocity_threshold,
        minimum_frequency=params_holder.minimum_frequency,
        maximum_frequency=params_holder.maximum_frequency,
        output_margin_length_sec=params_holder.output_margin_length_sec,
        trace_trimming_sec=params_holder.trace_trimming_sec,

    )
    return params


def create_event_confirmation_params(
        params_holder: EventConfirmationParamsHolder,
) -> EventConfirmationParams:
    """
    This method takes a :py:class:`~noiz.models.processing_params.EventConfirmationParamsHolder` instance and based on
    it creates an instance of database model :py:class:`~noiz.models.processing_params.EventConfirmationParams`.

    :param params_holder: Object containing all required elements to create a EventConfirmationParams instance
    :type params_holder: EventConfirmationParamsHolder
    :return: Working EventConfirmationParams model that needs to be inserted into db
    :rtype: EventConfirmationParams
    """

    params = EventConfirmationParams(
        datachunk_params_id=params_holder.datachunk_params_id,
        event_detection_params_id=params_holder.event_detection_params_id,
        time_lag=params_holder.time_lag,
        sampling_step=params_holder.sampling_step,
        vote_threshold=params_holder.vote_threshold,
        vote_weight=params_holder.vote_weight,
    )
    return params


def generate_multiple_beamforming_configs_based_on_single_holder(
        params_holder: BeamformingParamsHolder,
        freq_min: Optional[float],
        freq_max: Optional[float],
        freq_step: Optional[float],
        freq_window_width: Optional[float],
        rounding_precision: int = 4
) -> List[BeamformingParamsHolder]:
    """
    Generates multiple :py:class:`~noiz.models.processing_params.BeamformingParamsHolder` based on single
    :py:class:`~noiz.models.processing_params.BeamformingParamsHolder`. The only difference of generated
    :py:class:`~noiz.models.processing_params.BeamformingParamsHolder` are values of
    :py:attr::`~noiz.models.processing_params.BeamformingParamsHolder.freq_min` and
    :py:attr::`~noiz.models.processing_params.BeamformingParamsHolder.freq_max` which are generated based on provided
    arguments to that function.
    To generate :py:attr::`~noiz.models.processing_params.BeamformingParamsHolder.freq_min` values,
    a :py:meth:`numpy.arange` is used and the
    :py:attr::`~noiz.models.processing_params.BeamformingParamsHolder.freq_max` is calculated as sum of
    :py:attr::`~noiz.models.processing_params.BeamformingParamsHolder.freq_min` and `freq_window_width` argument.

    :param params_holder:
    :type params_holder: BeamformingParamsHolder
    :param freq_min: Minimum frequency generated
    :type freq_min: Optional[float]
    :param freq_max: Maximum frequency generated
    :type freq_max: Optional[float]
    :param freq_step: Step based on which starts of frequency bands will be generated
    :type freq_step: Optional[float]
    :param freq_window_width: Width of generated frequency band
    :type freq_window_width: Optional[float]
    :return: List of generated BeamformingParamsHolder for different frequency bands
    :rtype: List[BeamformingParamsHolder]
    """

    if any([x is None for x in (freq_min, freq_max, freq_step, freq_window_width)]):
        raise ValueError("If you want to generate multiple params you have to provide freq_min, freq_max, "
                         "freq_step, freq_window_width. ")

    if freq_window_width is not None and freq_window_width < 0.:  # This None check is for mypy to be satisfied
        raise ValueError('The freq_window_width has to be a positive value.')

    window_starts = np.arange(start=freq_min, stop=freq_max, step=freq_step)
    if len(window_starts) < 1:
        raise ValueError("Based on provided freq_min, freq_max, freq_step method `np.arange` produced less than"
                         "one result. Provide proper values.")

    param_holders = []
    for start in window_starts:
        min_freq = np.round(start, rounding_precision)
        max_freq = np.round(start + freq_window_width, rounding_precision)
        logger.debug(f"Generating beamforming params for {min_freq}-{max_freq}Hz. ")

        new_param_holder = copy.deepcopy(params_holder)
        new_param_holder.min_freq = min_freq
        new_param_holder.max_freq = max_freq

        param_holders.append(new_param_holder)
    return param_holders

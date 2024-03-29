# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from loguru import logger
from pathlib import Path
from sqlalchemy.exc import IntegrityError
from typing import Optional, Tuple, Union, List

from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import QCOneConfig, QCOneConfigRejectedTimeHolder, QCOneRejectedTime, QCOneConfigHolder, \
    QCTwoConfigRejectedTimeHolder, QCTwoRejectedTime, QCTwoConfigHolder, QCTwoConfig, StackingSchemaHolder, \
    StackingSchema, DatachunkParams, DatachunkParamsHolder, ProcessedDatachunkParams, \
    ProcessedDatachunkParamsHolder, CrosscorrelationCartesianParams, CrosscorrelationCartesianParamsHolder, \
    CrosscorrelationCylindricalParams, CrosscorrelationCylindricalParamsHolder, \
    EventDetectionParams, EventDetectionParamsHolder, EventConfirmationParams, EventConfirmationParamsHolder
from noiz.models.processing_params import BeamformingParams, BeamformingParamsHolder, PPSDParams, PPSDParamsHolder

from noiz.processing.configs import parse_single_config_toml, DefinedConfigs, \
    create_datachunkparams, create_processed_datachunk_params, create_crosscorrelation_cartesian_params, create_crosscorrelation_cylindrical_params, \
    create_stacking_params, create_beamforming_params, create_ppsd_params, create_event_detection_params, create_event_confirmation_params, \
    generate_multiple_beamforming_configs_based_on_single_holder

from noiz.api.component import fetch_components
from noiz.api.component_pair import fetch_componentpairs_cartesian


def fetch_datachunkparams_by_id(id: int) -> DatachunkParams:
    """
    Fetches a DatachunkParams objects by its ID.

    :param id: ID of processing params to be fetched
    :type id: int
    :return: fetched DatachunkParams object
    :rtype: Optional[DatachunkParams]
    """
    fetched_params = DatachunkParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"DatachunkParams object of id {id} does not exist.")

    return fetched_params


def fetch_processed_datachunk_params_by_id(id: int) -> ProcessedDatachunkParams:
    """
    Fetches a ProcessedDatachunkParams objects by its ID.

    :param id: ID of processing params to be fetched
    :type id: int
    :return: fetched ProcessedDatachunkParams object
    :rtype: ProcessedDatachunkParams
    :raises ValueError
    """
    fetched_params = ProcessedDatachunkParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"ProcessedDatachunkParams object of id {id} does not exist.")
    return fetched_params


def fetch_crosscorrelation_cartesian_params_by_id(id: int) -> CrosscorrelationCartesianParams:
    """
    Fetches a CrosscorrelationCartesianParams objects by its ID.

    :param id: ID of processing params to be fetched
    :type id: int
    :return: fetched CrosscorrelationCartesianParams object
    :rtype: CrosscorrelationCartesianParams
    :raises ValueError
    """
    fetched_params = CrosscorrelationCartesianParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"CrosscorrelationCartesianParams object of id {id} does not exist.")
    return fetched_params


def fetch_crosscorrelation_cylindrical_params_by_id(id: int) -> CrosscorrelationCylindricalParams:
    """
    Fetches a CrosscorrelationCylindricalParams objects by its ID.

    :param id: ID of processing params to be fetched
    :type id: int
    :return: fetched CrosscorrelationCylindricalParams object
    :rtype: CrosscorrelationCylindricalParams
    :raises ValueError
    """
    fetched_params = CrosscorrelationCylindricalParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"CrosscorrelationCylindricalParams object of id {id} does not exist.")
    return fetched_params


def fetch_stacking_schema_by_id(id: int) -> StackingSchema:
    """
    Fetches a StackingSchema objects by its ID.

    :param id: ID of processing params to be fetched
    :type id: int
    :return: fetched StackingSchema object
    :rtype: StackingSchema
    :raises ValueError
    """
    fetched_params = StackingSchema.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"StackingSchema object of id {id} does not exist.")
    return fetched_params


AllParamsObjects = Union[
    DatachunkParams,
    BeamformingParams,
    PPSDParams,
    ProcessedDatachunkParams,
    CrosscorrelationCartesianParams,
    StackingSchema,
    QCOneConfig,
    QCTwoConfig,
]


def _insert_params_into_db(
        params: AllParamsObjects
) -> AllParamsObjects:
    """
    This is method simply adding an instance of
    :py:class:`~noiz.models.DatachunkParams`,
    :py:class:`~noiz.models.ProcessedDatachunkParams`,
    :py:class:`~noiz.models.CrosscorrelationCartesianParams`,
    :py:class:`~noiz.models.StackingSchema`
    to DB and committing changes.

    Has to be executed within `app_context`

    :param params: Instance of supported params object to be added to db
    :type params: AllParamsObjects
    :return: Instance that was just added to the database
    :rtype: AllParamsObjects
    """
    db.session.add(params)
    try:
        db.session.commit()
        logger.info(f"Inserted {type(params)} to db with id {params.id}")
    except IntegrityError as e:
        logger.error(f"There was an error during insertion of object {params}. Error: {e}")
        db.session.rollback()
        raise e
    return params


def create_and_add_datachunk_params_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[DatachunkParams, Tuple[DatachunkParamsHolder, DatachunkParams]]:
    """
    filldocs
    This method takes a filepath to a TOML file with valid parameters
    to create a :class:`~noiz.processing.qc.QCOneConfigHolder` and subsequently :class:`~noiz.models.QCOneConfig`.
    It can also add the created object to the database. By default it does not add it to db.
    If chosen not to add the result to db, a tuple containing both :class:`~noiz.processing.qc.QCOneConfigHolder`
    and :class:`~noiz.models.QCOneConfig` will be returned for manual check.

    :param filepath: Path to existing TOML file
    :type filepath: Path
    :param add_to_db: If the result of parsing of TOML should be added to DB
    :type add_to_db: bool
    :return: It can return QCOneConfigHolder object for manual validation
    :rtype: Optional[Tuple[DatachunkParamsHolder, DatachunkParams]]
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.DATACHUNKPARAMS)
    datachunk_params = create_datachunkparams(params_holder=params_holder)

    if add_to_db:
        return _insert_params_into_db(params=datachunk_params)
    else:
        return (params_holder, datachunk_params)


def create_and_add_processed_datachunk_params_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[ProcessedDatachunkParams, Tuple[ProcessedDatachunkParamsHolder, ProcessedDatachunkParams]]:
    """
    filldocs
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.PROCESSEDDATACHUNKPARAMS)
    try:
        _ = fetch_datachunkparams_by_id(
            id=params_holder.datachunk_params_id
        )
    except EmptyResultException:
        raise EmptyResultException(f"There are no processed_datachunk_params in the database with requested id: "
                                   f"{params_holder.datachunk_params_id}")

    params = create_processed_datachunk_params(params_holder=params_holder)

    if add_to_db:
        return _insert_params_into_db(params=params)
    else:
        return (params_holder, params)


def create_and_add_beamforming_params_from_toml(
        filepath: Path,
        add_to_db: bool = False,
        generate_multiple: bool = False,
        freq_min: Optional[float] = None,
        freq_max: Optional[float] = None,
        freq_step: Optional[float] = None,
        freq_window_width: Optional[float] = None,
        rounding_precision: int = 4,
) -> Union[
    Union[BeamformingParams, Tuple[BeamformingParamsHolder, BeamformingParams]],
    Union[List[BeamformingParams], List[Tuple[BeamformingParamsHolder, BeamformingParams]]],
]:
    """
    filldocs
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.BEAMFORMINGPARAMS)
    from noiz.api.qc import fetch_qcone_config_single
    try:
        _ = fetch_qcone_config_single(
            id=params_holder.qcone_config_id
        )
    except EmptyResultException:
        raise EmptyResultException(f"There is no QCOneConfig in the database with requested id: "
                                   f"{params_holder.qcone_config_id}")
    if not generate_multiple:
        params = create_beamforming_params(params_holder=params_holder)

        if add_to_db:
            return _insert_params_into_db(params=params)
        else:
            return (params_holder, params)
    else:
        logger.debug("Generating multiple beamforming param holders. ")
        param_holders = generate_multiple_beamforming_configs_based_on_single_holder(
            params_holder=params_holder,
            freq_min=freq_min,
            freq_max=freq_max,
            freq_step=freq_step,
            freq_window_width=freq_window_width,
            rounding_precision=rounding_precision,
        )
        logger.debug(f"Generated {len(param_holders)}.")
        logger.debug("Converting holders to BeamformingParams. ")
        generated_params = []
        for holder in param_holders:
            params = create_beamforming_params(params_holder=holder)
            generated_params.append((holder, params))
        logger.debug(f"Generated {len(generated_params)} BeamformingParams. ")

        if add_to_db:
            logger.debug("Starting insertion of BeamformingParams to DB. ")
            added_params = []
            for _, params in generated_params:
                added_params.append(_insert_params_into_db(params=params))
            logger.debug(f"Successfully added to DB {len(added_params)} BeamformingParams.")
            return added_params
        else:
            return generated_params


def create_and_add_ppsd_params_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[PPSDParams, Tuple[PPSDParamsHolder, PPSDParams]]:
    """
    filldocs
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.PPSDPARAMS)
    try:
        datachunk_params = fetch_datachunkparams_by_id(
            id=params_holder.datachunk_params_id
        )
    except EmptyResultException:
        raise EmptyResultException(f"There is no DatachunkParams in the database with requested id: "
                                   f"{params_holder.datachunk_params_id}")

    params = create_ppsd_params(params_holder=params_holder, datachunk_params=datachunk_params)

    if add_to_db:
        return _insert_params_into_db(params=params)
    else:
        return (params_holder, params)


def create_and_add_crosscorrelation_cartesian_params_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[CrosscorrelationCartesianParams, Tuple[CrosscorrelationCartesianParamsHolder, CrosscorrelationCartesianParams]]:
    """
    filldocs
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.CROSSCORRELATIONCARTESIANPARAMS)

    try:
        processed_datachunk_params = fetch_processed_datachunk_params_by_id(
            id=params_holder.processed_datachunk_params_id
        )
    except EmptyResultException:
        raise EmptyResultException(f"There are no processed_datachunk_params in the database with requested id: "
                                   f"{params_holder.processed_datachunk_params_id}")

    params = create_crosscorrelation_cartesian_params(params_holder=params_holder, processed_params=processed_datachunk_params)

    if add_to_db:
        return _insert_params_into_db(params=params)
    else:
        return (params_holder, params)


def create_and_add_crosscorrelation_cylindrical_params_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[CrosscorrelationCylindricalParams, Tuple[CrosscorrelationCylindricalParamsHolder, CrosscorrelationCylindricalParams]]:
    """_summary_

    :param filepath: path of configuration file for cylindrical crosscorrelation
    :type filepath: Path
    :param add_to_db: if parameters have to be added or not to the database, defaults to False
    :type add_to_db: bool, optional
    :return:
    :rtype: Union[CrosscorrelationCylindricalParams, Tuple[CrosscorrelationCylindricalParamsHolder, CrosscorrelationCylindricalParams]]
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.CROSSCORRELATIONCYLINDRICALPARAMS)
    try:
        crosscorrelation_cartesian_params = fetch_crosscorrelation_cartesian_params_by_id(
            id=params_holder.crosscorrelation_cartesian_params_id
        )
    except EmptyResultException:
        raise EmptyResultException(f"There are no crosscorrelation_cartesian_params in the database with requested id: "
                                   f"{crosscorrelation_cartesian_params}")

    params = create_crosscorrelation_cylindrical_params(params_holder=params_holder)
    if add_to_db:
        return _insert_params_into_db(params=params)
    else:
        return (params_holder, params)


def create_and_add_stacking_schema_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[StackingSchema, Tuple[StackingSchemaHolder, StackingSchema]]:
    """
    filldocs
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.STACKINGSCHEMA)
    params = create_stacking_params(params_holder=params_holder)

    from noiz.api.qc import fetch_qctwo_config_single

    params.crosscorrelation_cartesian_params_id = fetch_qctwo_config_single(id=params.qctwo_config_id).crosscorrelation_cartesian_params_id

    if add_to_db:
        return _insert_params_into_db(params=params)
    else:
        return (params_holder, params)


def create_and_add_qcone_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[QCOneConfig, Tuple[QCOneConfigHolder, QCOneConfig]]:
    """
    This method takes a filepath to a TOML file with valid parameters
    to create a :class:`~noiz.processing.qc.QCOneConfigHolder` and subsequently :class:`~noiz.models.QCOneConfig`.
    It can also add the created object to the database. By default it does not add it to db.
    If chosen not to add the result to db, a tuple containing both :class:`~noiz.processing.qc.QCOneConfigHolder`
    and :class:`~noiz.models.QCOneConfig` will be returned for manual check.

    :param filepath: Path to existing TOML file
    :type filepath: Path
    :param add_to_db: If the result of parsing of TOML should be added to DB
    :type add_to_db: bool
    :return: It can return QCOneConfigHolder object for manual validation
    :rtype: Union[QCOneConfig, Tuple[QCOneConfigHolder, QCOneConfig]]
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.QCONE)
    try:
        fetch_datachunkparams_by_id(
            id=params_holder.datachunk_params_id
        )
    except EmptyResultException:
        raise EmptyResultException(f"There are no datachunk_params in the database with requested id: "
                                   f"{params_holder.datachunk_params_id}")

    qcone = create_qcone_config(qcone_holder=params_holder)

    if add_to_db:
        return _insert_params_into_db(params=qcone)
    else:
        return (params_holder, qcone)


def create_and_add_qctwo_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[QCTwoConfig, Tuple[QCTwoConfigHolder, QCTwoConfig]]:
    """
    This method takes a filepath to a TOML file with valid parameters
    to create a :class:`~noiz.models.qc.QCTwoConfigHolder` and subsequently :class:`~noiz.models.qc.QCTwoConfig`.
    It can also add the created object to the database. By default it does not add it to db.
    If chosen not to add the result to db, a tuple containing both :class:`~noiz.models.qc.QCTwoConfigHolder`
    and :class:`~noiz.models.qc.QCTwoConfig` will be returned for manual check.

    :param filepath: Path to existing TOML file
    :type filepath: Path
    :param add_to_db: If the result of parsing of TOML should be added to DB
    :type add_to_db: bool
    :return: It can return QCTwoConfigHolder object for manual validation
    :rtype: Union[QCTwoConfig, Tuple[QCTwoConfigHolder, QCTwoConfig]]
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.QCTWO)
    qqtwo = create_qctwo_config(qctwo_holder=params_holder)

    if add_to_db:
        return _insert_params_into_db(params=qqtwo)
    else:
        return (params_holder, qqtwo)


def create_and_add_event_detection_params_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[EventDetectionParams, Tuple[EventDetectionParamsHolder, EventDetectionParams]]:
    """
    filldocs
    """
    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.EVENTDETECTIONPARAMS)
    params = create_event_detection_params(params_holder=params_holder)

    if add_to_db:
        return _insert_params_into_db(params=params)
    else:
        return (params_holder, params)


def create_and_add_event_confirmation_params_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Union[EventConfirmationParams, Tuple[EventConfirmationParamsHolder, EventConfirmationParams]]:
    """
    filldocs
    """
    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.EVENTCONFIRMATIONPARAMS)
    params = create_event_confirmation_params(params_holder=params_holder)

    if add_to_db:
        return _insert_params_into_db(params=params)
    else:
        return (params_holder, params)


def create_qcone_rejected_time(
        holder: QCOneConfigRejectedTimeHolder,
) -> List[QCOneRejectedTime]:
    """
    Based on provided :class:`~noiz.processing.qc.QCOneConfigRejectedTimeHolder` creates instances of the
    database models :class:`~noiz.models.QCOneRejectedTime`.

    Since the holder itself is focused on the single component inputs, it should never return more than a
    single element list but for safety, it will return a list instead of single object.

    Has to be executed within `app_context`

    :param holder: Holder to be processed
    :type holder: QCOneConfigRejectedTimeHolder
    :return: Instance of a model, ready to be added to to the database
    :rtype: QCOneRejectedTime
    """
    fetched_components = fetch_components(
        networks=holder.network,
        stations=holder.station,
        components=holder.component,
    )
    if len(fetched_components) == 0:
        raise EmptyResultException(f"There were no components found in db for that parameters. "
                                   f"{holder.network}.{holder.station}.{holder.component}")

    res = [
        QCOneRejectedTime(component_id=cmp.id, starttime=holder.starttime, endtime=holder.endtime)
        for cmp in fetched_components
    ]
    return res


def create_qcone_config(
        qcone_holder: QCOneConfigHolder,
) -> QCOneConfig:
    """
    This method takes a :class:`~noiz.processing.qc.QCOneConfigHolder` instance and based on it creates an instance
    of database model :class:`~noiz.models.QCOneConfig`.

    Optionally, it can create the instance of :class:`~noiz.processing.qc.QCOneConfigHolder` from provided kwargs, but
    why dont you do it on your own to ensure that it will get everything it needs?

    Has to be executed within `app_context`

    :param qcone_holder: Object containing all required elements to create a QCOne instance
    :type qcone_holder: QCOneConfigHolder
    :param kwargs: Optional kwargs to create QCOneConfigHolder
    :return: Working QCOne model that needs to be inserted into db
    :rtype: QCOneConfig
    """

    qc_one_rejected_times = []
    for rej_time in qcone_holder.rejected_times:
        qc_one_rejected_times.extend(create_qcone_rejected_time(holder=rej_time))

    qcone = QCOneConfig(
        datachunk_params_id=qcone_holder.datachunk_params_id,
        null_policy=qcone_holder.null_treatment_policy.value,
        strict_gps=qcone_holder.strict_gps,
        starttime=qcone_holder.starttime,
        endtime=qcone_holder.endtime,
        avg_gps_time_error_min=qcone_holder.avg_gps_time_error_min,
        avg_gps_time_error_max=qcone_holder.avg_gps_time_error_max,
        avg_gps_time_uncertainty_min=qcone_holder.avg_gps_time_uncertainty_min,
        avg_gps_time_uncertainty_max=qcone_holder.avg_gps_time_uncertainty_max,
        signal_energy_min=qcone_holder.signal_energy_min,
        signal_energy_max=qcone_holder.signal_energy_max,
        signal_min_value_min=qcone_holder.signal_min_value_min,
        signal_min_value_max=qcone_holder.signal_min_value_max,
        signal_max_value_min=qcone_holder.signal_max_value_min,
        signal_max_value_max=qcone_holder.signal_max_value_max,
        signal_mean_value_min=qcone_holder.signal_mean_value_min,
        signal_mean_value_max=qcone_holder.signal_mean_value_max,
        signal_variance_min=qcone_holder.signal_variance_min,
        signal_variance_max=qcone_holder.signal_variance_max,
        signal_skewness_min=qcone_holder.signal_skewness_min,
        signal_skewness_max=qcone_holder.signal_skewness_max,
        signal_kurtosis_min=qcone_holder.signal_kurtosis_min,
        signal_kurtosis_max=qcone_holder.signal_kurtosis_max,
    )
    qcone.time_periods_rejected = qc_one_rejected_times
    return qcone


def create_qctwo_rejected_time(
        holder: QCTwoConfigRejectedTimeHolder,
) -> List[QCTwoRejectedTime]:
    """
    Based on provided :class:`~noiz.models.qc.QCTwoConfigRejectedTimeHolder` creates instances of the
    database models :class:`~noiz.models.qc.QCTwoRejectedTime`.

    Since the holder itself is focused on the single component inputs, it should never return more than a
    single element list but for safety, it will return a list instead of single object.

    Has to be executed within `app_context`

    :param holder: Holder to be processed
    :type holder: QCTwoConfigRejectedTimeHolder
    :return: Instance of a model, ready to be added to to the database
    :rtype: QCTwoRejectedTime
    """
    fetched_components_pairs = fetch_componentpairs_cartesian(
        network_codes_a=holder.network_a,
        station_codes_a=holder.station_a,
        component_codes_a=holder.component_a,
        network_codes_b=holder.network_b,
        station_codes_b=holder.station_b,
        component_codes_b=holder.component_b,
        include_intracorrelation=True,
        include_autocorrelation=True,
    )
    if len(fetched_components_pairs) == 0:
        reversed_components_pair = fetch_componentpairs_cartesian(
            network_codes_b=holder.network_a,
            station_codes_b=holder.station_a,
            component_codes_b=holder.component_a,
            network_codes_a=holder.network_b,
            station_codes_a=holder.station_b,
            component_codes_a=holder.component_b,
            include_intracorrelation=True,
            include_autocorrelation=True,
        )
        if len(reversed_components_pair) == 1:
            raise EmptyResultException(f"There were no components found in db for that parameters. "
                                       f"However, there is a pair that fits parameters you provided but in reversed"
                                       f"order. Check if it satisfies you and change it in the file. "
                                       f"{reversed_components_pair}")
        else:
            raise EmptyResultException(f"There were no components found in db for that parameters. "
                                       f"Also, pair for reversed parameters do not exist. {holder}")

    if len(fetched_components_pairs) > 1:
        raise ValueError(f"There was more than one component_pair_cartesian fetched for that query. "
                         f"Something is wrong with the fetcher. {fetched_components_pairs}")

    res = [
        QCTwoRejectedTime(componentpair_id=cmp.id, starttime=holder.starttime, endtime=holder.endtime)
        for cmp in fetched_components_pairs
    ]
    return res


def create_qctwo_config(
        qctwo_holder: QCTwoConfigHolder,
) -> QCTwoConfig:
    """
    This method takes a :class:`~noiz.models.qc.QCTwoConfigHolder` instance and based on it creates an instance
    of database model :class:`~noiz.models.qc.QCTwoConfig`.

    Optionally, it can create the instance of :class:`~noiz.models.qc.QCOneConfigHolder` from provided kwargs, but
    why dont you do it on your own to ensure that it will get everything it needs?

    Has to be executed within `app_context`

    :param qctwo_holder: Object containing all required elements to create a QCTwoConfig instance
    :type qctwo_holder: QCTwoConfigHolder
    :param kwargs: Optional kwargs to create QCTwoConfigHolder
    :return: Working QCOne model that needs to be inserted into db
    :rtype: QCTwoConfig
    """

    qc_two_rejected_times = []
    for rej_time in qctwo_holder.rejected_times:
        qc_two_rejected_times.extend(create_qctwo_rejected_time(holder=rej_time))

    qctwo = QCTwoConfig(
        null_policy=qctwo_holder.null_treatment_policy.value,
        crosscorrelation_cartesian_params_id=qctwo_holder.crosscorrelation_cartesian_params_id,
        starttime=qctwo_holder.starttime,
        endtime=qctwo_holder.endtime,
    )
    qctwo.time_periods_rejected = qc_two_rejected_times
    return qctwo

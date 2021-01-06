import datetime
from loguru import logger
import operator as ope
from pathlib import Path
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from typing import List, Collection, Union, Optional, Tuple, Any, Callable

from noiz.api.component import fetch_components
from noiz.api.datachunk import _determine_filters_and_opts_for_datachunk
from noiz.api.helpers import validate_to_tuple
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.models.datachunk import Datachunk, DatachunkStats
from noiz.models.qc import QCOneConfig, QCOneRejectedTime, QCOneRejectedTimeHolder, QCOneHolder, QCOneResults
from noiz.models.soh import AveragedSohGps
from noiz.models.timespan import Timespan
from noiz.processing.configs import validate_dict_as_qcone_holder, load_qc_one_config_toml, parse_single_config_toml, \
    DefinedConfigs


def fetch_qc_ones(ids: Union[int, Collection[int]]) -> List[QCOneConfig]:
    """
    Fetches the QCOne from db based on id.

    :param ids: IDs to be fetched
    :type ids: Union[int, Collection[int]]
    :return: Fetched QCones
    :rtype: List[QCOneConfig]
    """

    ids = validate_to_tuple(val=ids, accepted_type=int)

    fetched = db.session.query(QCOneConfig).filter(
        QCOneConfig.id.in_(ids),
    ).all()

    return fetched


def fetch_qc_one_single(id: int) -> QCOneConfig:
    """
    Fetches the QCOne from db based on id.

    :param ids: IDs to be fetched
    :type ids: Union[int, Collection[int]]
    :return: Fetched QCones
    :rtype: List[QCOneConfig]
    """

    fetched = db.session.query(QCOneConfig).filter(
        QCOneConfig.id == id,
    ).first()

    return fetched


def create_qcone_rejected_time(
        holder: QCOneRejectedTimeHolder,
) -> List[QCOneRejectedTime]:
    """
    Based on provided :class:`~noiz.processing.qc.QCOneRejectedTimeHolder` creates instances of the
    database models :class:`~noiz.models.QCOneRejectedTime`.

    Since the holder itself is focused on the single component inputs, it should never return more than a
    single element list but for safety, it will return a list instead of single object.

    :param holder: Holder to be processed
    :type holder: QCOneRejectedTimeHolder
    :return: Instance of a model, ready to be added to to the database
    :rtype: QCOneRejectedTime
    """

    fetched_components = fetch_components(
        networks=holder.network,
        stations=holder.station,
        components=holder.component,
    )
    if len(fetched_components) == 0:
        raise ValueError(f"There were no components found in db for that parameters. "
                         f"{holder.network}.{holder.station}.{holder.component}")

    res = [
        QCOneRejectedTime(component_id=cmp.id, starttime=holder.starttime, endtime=holder.endtime)
        for cmp in fetched_components
    ]
    return res


def create_qcone_config(
        qcone_holder: Optional[QCOneHolder] = None,
        **kwargs,
) -> QCOneConfig:
    """
    This method takes a :class:`~noiz.processing.qc.QCOneHolder` instance and based on it creates an instance
    of database model :class:`~noiz.models.QCOneConfig`.

    Optionally, it can create the instance of :class:`~noiz.processing.qc.QCOneHolder` from provided kwargs, but
    why dont you do it on your own to ensure that it will get everything it needs?

    Has to be executed within `app_context`

    :param qcone_holder: Object containing all required elements to create a QCOne instance
    :type qcone_holder: QCOneHolder
    :param kwargs: Optional kwargs to create QCOneHolder
    :return: Working QCOne model that needs to be inserted into db
    :rtype: QCOneConfig
    """

    if qcone_holder is None:
        qcone_holder = validate_dict_as_qcone_holder(kwargs)

    qc_one_rejected_times = []
    for rej_time in qcone_holder.rejected_times:
        qc_one_rejected_times.extend(create_qcone_rejected_time(holder=rej_time))

    qcone = QCOneConfig(
        null_policy=qcone_holder.null_treatment_policy.value,
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


def insert_qc_one_config_into_db(qcone_config: QCOneConfig):
    """
    This is method simply adding an instance of :class:`~noiz.models.QCOneConfig` to DB and committing changes.

    Has to be executed within `app_context`

    :param qcone_config: Instance of QCOne to be added to db
    :type qcone_config: QCOneConfig
    :return: None
    :rtype: NoneType
    """
    db.session.add(qcone_config)
    db.session.commit()
    return


def create_and_add_qc_one_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Optional[Tuple[QCOneHolder, QCOneConfig]]:
    """
    This method takes a filepath to a TOML file with valid parameters
    to create a :class:`~noiz.processing.qc.QCOneHolder` and subsequently :class:`~noiz.models.QCOneConfig`.
    It can also add the created object to the database. By default it does not add it to db.
    If chosen not to add the result to db, a tuple containing both :class:`~noiz.processing.qc.QCOneHolder`
    and :class:`~noiz.models.QCOneConfig` will be returned for manual check.

    :param filepath: Path to existing TOML file
    :type filepath: Path
    :param add_to_db: If the result of parsing of TOML should be added to DB
    :type add_to_db: bool
    :return: It can return QCOneHolder object for manual validation
    :rtype: Optional[QCOneHolder]
    """

    qcone_holder = load_qc_one_config_toml(filepath=filepath)
    qcone_config = create_qcone_config(qcone_holder=qcone_holder)

    if add_to_db:
        insert_qc_one_config_into_db(qcone_config=qcone_config)
    else:
        return (qcone_holder, qcone_config)
    return None


def process_qcone(
        qcone_config_id: int,
        stations: Optional[Union[Collection[str], str]],
        components: Optional[Union[Collection[str], str]],
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        use_gps: bool = True,
        strict_gps: bool = False,
):
    qcone_config = fetch_qc_one_single(id=qcone_config_id)
    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_components = fetch_components(
        stations=stations,
        components=components
    )
    filters, opts = _determine_filters_and_opts_for_datachunk(
        components=fetched_components,
        timespans=timespans,
        load_timespan=True,
        load_stats=True
    )

    if qcone_config.uses_gps() and use_gps:
        query = (db.session
                 .query(Datachunk, DatachunkStats, AveragedSohGps)
                 .select_from(Datachunk)
                 .join(AveragedSohGps,
                       and_(
                           Datachunk.device_id == AveragedSohGps.device_id,
                           Datachunk.timespan_id == AveragedSohGps.timespan_id
                       ))
                 .join(DatachunkStats)
                 .filter(*filters).options(opts))
        fetched_results = query.all()

        logger.error(len(fetched_results))
        qcone_results = []
        used_ids = []
        for datachunk, stats, avg_soh_gps in fetched_results:
            used_ids.append(datachunk.id)
            qcone_res = calculate_qcone_for_gps_and_stats(avg_soh_gps, datachunk, qcone_config, stats)
            qcone_results.append(qcone_res)

        if not strict_gps:
            # Topping up by calculating QCOneResult for those Datachunks that do not have an AvgGpsSoh
            filters.append(~Datachunk.id.in_(used_ids))
            topup_query = (db.session
                           .query(Datachunk, DatachunkStats)
                           .select_from(Datachunk)
                           .join(DatachunkStats)
                           .filter(*filters).options(opts))
            topup_fetched_results = topup_query.all()

            for datachunk, stats in topup_fetched_results:
                qcone_res = calculate_qcone_for_stats_only(datachunk, qcone_config, stats)
                qcone_results.append(qcone_res)

    else:
        query = (db.session
                 .query(Datachunk, DatachunkStats)
                 .select_from(Datachunk)
                 .join(DatachunkStats)
                 .filter(*filters).options(opts))
        fetched_results = query.all()

        qcone_results = []

        for datachunk, stats in fetched_results:
            qcone_res = calculate_qcone_for_stats_only(datachunk, qcone_config, stats)
            qcone_results.append(qcone_res)

    add_or_upsert_qcone_results_in_db(qcone_results_collection=qcone_results)


def add_or_upsert_qcone_results_in_db(qcone_results_collection: Collection[QCOneResults]):
    """
    Adds or upserts provided iterable of QCOneResults to DB.
    Must be executed within AppContext.

    :param qcone_results_collection:
    :type qcone_results_collection: Iterable[Datachunk]
    :return:
    :rtype:
    """
    for results in qcone_results_collection:

        if not isinstance(results, QCOneResults):
            logger.warning(f'Provided object is not an instance of QCOneResults. '
                           f'Provided object was an {type(results)}. Skipping.')
            continue

        logger.info("Querrying db if the QCOneResults already exists.")
        existing_chunks = (
            db.session.query(QCOneResults)
            .filter(
                QCOneResults.datachunk_id == results.datachunk_id,
                QCOneResults.qcone_config_id == results.qcone_config_id,
            )
            .all()
        )

        if len(existing_chunks) == 0:
            logger.info("No existing chunks found. Adding QCOneResults to DB.")
            db.session.add(results)
        else:
            logger.info("The QCOneResults already exists in db. Updating.")
            insert_command = (
                insert(QCOneResults)
                .values(
                    starttime=results.starttime,
                    endtime=results.endtime,
                    avg_gps_time_error_min=results.avg_gps_time_error_min,
                    avg_gps_time_error_max=results.avg_gps_time_error_max,
                    avg_gps_time_uncertainty_min=results.avg_gps_time_uncertainty_min,
                    avg_gps_time_uncertainty_max=results.avg_gps_time_uncertainty_max,
                    signal_energy_min=results.signal_energy_min,
                    signal_energy_max=results.signal_energy_max,
                    signal_min_value_min=results.signal_min_value_min,
                    signal_min_value_max=results.signal_min_value_max,
                    signal_max_value_min=results.signal_max_value_min,
                    signal_max_value_max=results.signal_max_value_max,
                    signal_mean_value_min=results.signal_mean_value_min,
                    signal_mean_value_max=results.signal_mean_value_max,
                    signal_variance_min=results.signal_variance_min,
                    signal_variance_max=results.signal_variance_max,
                    signal_skewness_min=results.signal_skewness_min,
                    signal_skewness_max=results.signal_skewness_max,
                    signal_kurtosis_min=results.signal_kurtosis_min,
                    signal_kurtosis_max=results.signal_kurtosis_max,
                )
                .on_conflict_do_update(
                    constraint="unique_qcone_results_per_config_per_datachunk",
                    set_=dict(
                        starttime=results.starttime,
                        endtime=results.endtime,
                        avg_gps_time_error_min=results.avg_gps_time_error_min,
                        avg_gps_time_error_max=results.avg_gps_time_error_max,
                        avg_gps_time_uncertainty_min=results.avg_gps_time_uncertainty_min,
                        avg_gps_time_uncertainty_max=results.avg_gps_time_uncertainty_max,
                        signal_energy_min=results.signal_energy_min,
                        signal_energy_max=results.signal_energy_max,
                        signal_min_value_min=results.signal_min_value_min,
                        signal_min_value_max=results.signal_min_value_max,
                        signal_max_value_min=results.signal_max_value_min,
                        signal_max_value_max=results.signal_max_value_max,
                        signal_mean_value_min=results.signal_mean_value_min,
                        signal_mean_value_max=results.signal_mean_value_max,
                        signal_variance_min=results.signal_variance_min,
                        signal_variance_max=results.signal_variance_max,
                        signal_skewness_min=results.signal_skewness_min,
                        signal_skewness_max=results.signal_skewness_max,
                        signal_kurtosis_min=results.signal_kurtosis_min,
                        signal_kurtosis_max=results.signal_kurtosis_max,
                    ),
                )
            )
            db.session.execute(insert_command)

    logger.debug('Commiting session.')
    db.session.commit()
    return


def calculate_qcone_for_gps_and_stats(avg_soh_gps, datachunk, qcone_config, stats):
    qcone_res = QCOneResults(datachunk_id=datachunk.id, qcone_config_id=qcone_config.id)
    qcone_res = determine_qcone_time(
        results=qcone_res,
        datachunk=datachunk,
        config=qcone_config,
    )
    qcone_res = determine_qcone_gps(result=qcone_res, config=qcone_config, avg_soh_gps=avg_soh_gps)
    qcone_res = determine_qcone_stats(results=qcone_res, stats=stats, config=qcone_config)
    return qcone_res


def calculate_qcone_for_stats_only(datachunk, qcone_config, stats):
    qcone_res = QCOneResults(datachunk_id=datachunk.id, qcone_config_id=qcone_config.id)
    qcone_res = determine_qcone_time(
        results=qcone_res,
        datachunk=datachunk,
        config=qcone_config,
    )
    qcone_res = determine_qcone_gps(result=qcone_res, config=qcone_config, avg_soh_gps=None)
    qcone_res = determine_qcone_stats(results=qcone_res, stats=stats, config=qcone_config)
    return qcone_res


def determine_qcone_time(
        results: QCOneResults,
        datachunk: Datachunk,
        config: QCOneConfig,
) -> QCOneResults:

    if not isinstance(datachunk.timespan, Timespan):
        raise ValueError('You should load timespan together with the Datachunk.')

    results.starttime = compare_vals_null_safe(
        config.starttime, datachunk.timespan.starttime, ope.le, null_value=config.null_value)
    results.endtime = compare_vals_null_safe(
        config.endtime, datachunk.timespan.endtime, ope.ge, null_value=config.null_value)

    return results


def determine_qcone_stats(
        results: QCOneResults,
        stats: DatachunkStats,
        config: QCOneConfig,
) -> QCOneResults:

    results.signal_energy_max = compare_vals_null_safe(
        config.signal_energy_max, stats.energy, ope.le, config.null_value)
    results.signal_energy_min = compare_vals_null_safe(
        config.signal_energy_min, stats.energy, ope.ge, config.null_value)
    results.signal_min_value_max = compare_vals_null_safe(
        config.signal_min_value_max, stats.min, ope.le, config.null_value)
    results.signal_min_value_min = compare_vals_null_safe(
        config.signal_min_value_min, stats.min, ope.ge, config.null_value)
    results.signal_max_value_max = compare_vals_null_safe(
        config.signal_max_value_max, stats.max, ope.le, config.null_value)
    results.signal_max_value_min = compare_vals_null_safe(
        config.signal_max_value_min, stats.max, ope.ge, config.null_value)
    results.signal_mean_value_max = compare_vals_null_safe(
        config.signal_mean_value_max, stats.mean, ope.le, config.null_value)
    results.signal_mean_value_min = compare_vals_null_safe(
        config.signal_mean_value_min, stats.mean, ope.ge, config.null_value)
    results.signal_variance_max = compare_vals_null_safe(
        config.signal_variance_max, stats.variance, ope.le, config.null_value)
    results.signal_variance_min = compare_vals_null_safe(
        config.signal_variance_min, stats.variance, ope.ge, config.null_value)
    results.signal_skewness_max = compare_vals_null_safe(
        config.signal_skewness_max, stats.skewness, ope.le, config.null_value)
    results.signal_skewness_min = compare_vals_null_safe(
        config.signal_skewness_min, stats.skewness, ope.ge, config.null_value)
    results.signal_kurtosis_max = compare_vals_null_safe(
        config.signal_kurtosis_max, stats.kurtosis, ope.le, config.null_value)
    results.signal_kurtosis_min = compare_vals_null_safe(
        config.signal_kurtosis_min, stats.kurtosis, ope.ge, config.null_value)

    return results


def determine_qcone_gps(
        result: QCOneResults,
        config: QCOneConfig,
        avg_soh_gps: Optional[AveragedSohGps]
) -> QCOneResults:
    """
    Compares values of provided instance of :class:`noiz.models.soh.AveragedSohGps` with values defined in
    :class:`noiz.models.qc.QCOneConfig`. If as :paramref:`noiz.api.qc.determine_qcone_gps.avg_soh_gps` will me provided
    None, all values will be set to the :py:attr:`noiz.models.qc.QCOneConfig.null_value`.
    If any of the config or real data values will also be None, the result of comparison will be set to
    :py:attr:`noiz.models.qc.QCOneConfig.null_value`.

    :param result: Object to which the results of comparisons will be saved
    :type result: QCOneResults,
    :param config: Object to take the reference values to compare against
    :type config: QCOneConfig,
    :param avg_soh_gps: Real data values that will be used in comparison
    :type avg_soh_gps: Optional[AveragedSohGps]
    :return: Object that will include results of the comparison
    :rtype: QCOneResults
    """

    if avg_soh_gps is not None:
        result.avg_gps_time_error_max = compare_vals_null_safe(
            config.avg_gps_time_error_max, avg_soh_gps.time_error, ope.ge, config.null_value)
        result.avg_gps_time_error_min = compare_vals_null_safe(
            config.avg_gps_time_error_min, avg_soh_gps.time_error, ope.le, config.null_value)
        result.avg_gps_time_uncertainty_max = compare_vals_null_safe(
            config.avg_gps_time_uncertainty_max, avg_soh_gps.time_uncertainty, ope.ge, config.null_value)
        result.avg_gps_time_uncertainty_min = compare_vals_null_safe(
            config.avg_gps_time_uncertainty_min, avg_soh_gps.time_uncertainty, ope.le, config.null_value)
    else:
        result.avg_gps_time_error_max = config.null_value
        result.avg_gps_time_error_min = config.null_value
        result.avg_gps_time_uncertainty_max = config.null_value
        result.avg_gps_time_uncertainty_min = config.null_value
    return result


def compare_vals_null_safe(a: Any, b: Any, op: Callable[[Any, Any], bool], null_value: bool):
    """
    Compares two values with provided callable. Callable, should be coming from the :py:mod:`operator`.
    It first checks if any of provided values is None and if yes, returns a provided
    :paramref:`noiz.api.qc.compare_vals_null_safe.null_value`.

    :param a: First value to compare
    :type a: Any
    :param b: Second value to compare
    :type b: Any
    :param op: Callable to perform comparison with.
    :type op: Callable[[Any, Any], bool]
    :param null_value:
    :type null_value: bool
    :return: Returns result of a call or a value of :paramref:`noiz.api.qc.compare_vals_null_safe.null_value`
    :rtype: bool
    """

    if a is None or b is None:
        return null_value
    else:
        return op(a, b)


def insert_qconeconfig_into_db(params: QCOneConfig) -> None:
    """
    This is method simply adding an instance of :class:`~noiz.models.DatachunkParams` to DB and committing changes.

    Has to be executed within `app_context`

    :param params: Instance of DatachunkParams to be added to db
    :type params: DatachunkParams
    :return: None
    :rtype: NoneType
    """
    db.session.add(params)
    db.session.commit()
    logger.info(f"Succesfully added to db QCOneConfig object with id={params.id}")
    return


def create_and_add_qcone_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Optional[Tuple[QCOneHolder, QCOneConfig]]:
    """
    This method takes a filepath to a TOML file with valid parameters
    to create a :class:`~noiz.processing.qc.QCOneHolder` and subsequently :class:`~noiz.models.QCOneConfig`.
    It can also add the created object to the database. By default it does not add it to db.
    If chosen not to add the result to db, a tuple containing both :class:`~noiz.processing.qc.QCOneHolder`
    and :class:`~noiz.models.QCOneConfig` will be returned for manual check.

    :param filepath: Path to existing TOML file
    :type filepath: Path
    :param add_to_db: If the result of parsing of TOML should be added to DB
    :type add_to_db: bool
    :return: It can return QCOneHolder object for manual validation
    :rtype: Optional[QCOneHolder]
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.QCONE)
    qcone = create_qcone_config(qcone_holder=params_holder)

    if add_to_db:
        insert_qconeconfig_into_db(params=qcone)
    else:
        return (params_holder, qcone)
    return None

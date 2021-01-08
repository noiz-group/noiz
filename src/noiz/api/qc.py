import datetime
from loguru import logger
from pathlib import Path

from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query
from typing import List, Collection, Union, Optional, Tuple

from noiz.api.component import fetch_components
from noiz.api.datachunk import _determine_filters_and_opts_for_datachunk
from noiz.api.helpers import validate_to_tuple, extract_object_ids
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models.datachunk import Datachunk, DatachunkStats
from noiz.models.qc import QCOneConfig, QCOneRejectedTime, QCOneConfigRejectedTimeHolder, QCOneConfigHolder, QCOneResults
from noiz.models.soh import AveragedSohGps
from noiz.processing.configs import validate_dict_as_qcone_holder, load_qc_one_config_toml, parse_single_config_toml, \
    DefinedConfigs
from noiz.processing.qc import calculate_qcone_results


def fetch_qcone_config(ids: Union[int, Collection[int]]) -> List[QCOneConfig]:
    """
    Fetches the QCOneConfig from db based on id. Can be either a single id or some collection of ids.
    It always returns a list of instances, can also be an empty list.

    :param ids: IDs to be fetched
    :type ids: Union[int, Collection[int]]
    :return: Fetched QConeConfig objects
    :rtype: List[QCOneConfig]
    """

    ids = validate_to_tuple(val=ids, accepted_type=int)

    fetched = db.session.query(QCOneConfig).filter(
        QCOneConfig.id.in_(ids),
    ).all()

    return fetched


def fetch_qcone_config_single(id: int) -> QCOneConfig:
    """
    Fetches a single :class:`noiz.models.qc.QCOneConfig` from db based on id.

    :param id: ID to be fetched
    :type id: int
    :return: Fetched config
    :rtype: QCOneConfig
    :raises ValueError
    """

    fetched = db.session.query(QCOneConfig).filter(
        QCOneConfig.id == id,
    ).first()

    if fetched is None:
        raise EmptyResultException(f"There was no QCOneConfig with if={id} in the database.")

    return fetched


def fetch_qcone_results(
        qcone_config: Optional[QCOneConfig] = None,
        qcone_config_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
) -> List[QCOneResults]:

    query = _query_qcone_results(
        qcone_config=qcone_config,
        qcone_config_id=qcone_config_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
    )
    return query.all()


def count_qcone_results(
        qcone_config: Optional[QCOneConfig] = None,
        qcone_config_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
) -> List[QCOneResults]:

    query = _query_qcone_results(
        qcone_config=qcone_config,
        qcone_config_id=qcone_config_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
    )
    return query.count()


def _query_qcone_results(
        qcone_config: Optional[QCOneConfig] = None,
        qcone_config_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
) -> Query:

    if datachunks is not None and datachunk_ids is not None:
        raise ValueError("Both datachunks and datachunk_ids parameters were provided. "
                         "You have to provide maximum one of them.")
    if qcone_config is not None and qcone_config_id is not None:
        raise ValueError("Both qcone_config and qcone_config_id parameters were provided. "
                         "You have to provide maximum one of them.")

    filters = []
    if qcone_config is not None:
        filters.append(QCOneResults.qcone_config_id.in_((qcone_config.id,)))
    if qcone_config_id is not None:
        qcone_config_ids = validate_to_tuple(val=qcone_config_id, accepted_type=int)
        filters.append(QCOneResults.qcone_config_id.in_(qcone_config_ids))
    if datachunks is not None:
        extracted_datachunk_ids = extract_object_ids(datachunks)
        filters.append(QCOneResults.datachunk_id.in_(extracted_datachunk_ids))
    if datachunk_ids is not None:
        filters.append(QCOneResults.datachunk_id.in_(datachunk_ids))
    if len(filters) == 0:
        filters.append(True)

    query = QCOneResults.query.filter(*filters)

    return query


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
        qcone_holder: Optional[QCOneConfigHolder] = None,
        **kwargs,
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


def create_and_add_qc_one_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Optional[Tuple[QCOneConfigHolder, QCOneConfig]]:
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
    :rtype: Optional[QCOneConfigHolder]
    """

    qcone_holder = load_qc_one_config_toml(filepath=filepath)
    qcone_config = create_qcone_config(qcone_holder=qcone_holder)

    if add_to_db:
        insert_qcone_config_into_db(config=qcone_config)
    else:
        return (qcone_holder, qcone_config)
    return None


def process_qcone(
        qcone_config_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        use_gps: bool = True,
        strict_gps: bool = False,
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
):
    """
    A method that runs the whole process of calculation of results of QCOne.
    You need to provide id of :class:`noiz.models.qc.QCOneConfig` object that has to be present in the database.
    You have to add the config with different method prior to calling this one.

    You can limit the run of this method by standardized selection parameter based on the selection
    of :class:`noiz.models.component.Component` object and the :class:`noiz.models.timespan.Timespan` object.
    Arguments for selecting the former are voluntary, for the latter are obligatory. Component object query by default
    will return all components in the database.

    You can specify if you want to use GPS information for calculations by passing True as parameter
    :paramref:`noiz.api.qc.process_qcone.use_gps`.
    The default action here is to use gps. Additionally, by default, the Datachunks that do not have associated
    gps information with them, will have fields connected to GPS information filled with defined null value.
    This can be turned off by passing value False as param :paramref:`noiz.api.qc.process_qcone.strict_gps`.

    By default, results of that process will be upserted to the database.

    :param qcone_config_id: Id of a QCOneConfig from the database
    :type qcone_config_id: int
    :param starttime: Time after which to look for timespans
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Time before which to look for timespans
    :type endtime: Union[datetime.date, datetime.datetime],
    :param use_gps: If gps information should be used in the process
    :type use_gps: bool
    :param strict_gps: If the datachunks not containing gps information should be ommited.
    :type strict_gps: bool
    :param networks: Networks of components to be fetched
    :type networks: Optional[Union[Collection[str], str]]
    :param stations: Stations of components to be fetched
    :type stations: Optional[Union[Collection[str], str]]
    :param components: Component letters to be fetched
    :type components: Optional[Union[Collection[str], str]]
    :param components: Ids of components objects to be fetched
    :type components: Optional[Union[Collection[int], int]]
    :return:
    :rtype:
    """
    try:
        qcone_config: QCOneConfig = fetch_qcone_config_single(id=qcone_config_id)
    except EmptyResultException as e:
        logger.error(e)
        raise e

    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )
    filters, opts = _determine_filters_and_opts_for_datachunk(
        components=fetched_components,
        timespans=timespans,
        load_component=False,
        load_timespan=True,
        load_stats=False,
    )

    qcone_results = []
    used_ids = []
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

        logger.info(f"Starting QCOneResults calculations with GPS for {len(fetched_results)} elements")

        for datachunk, stats, avg_soh_gps in fetched_results:
            used_ids.append(datachunk.id)
            qcone_res = calculate_qcone_results(
                datachunk=datachunk,
                qcone_config=qcone_config,
                stats=stats,
                avg_soh_gps=avg_soh_gps,
            )
            qcone_results.append(qcone_res)

        logger.info("Calculations done for all elements with GPS info associated.")

        if not strict_gps:
            # Topping up by calculating QCOneResult for those Datachunks that do not have an AvgGpsSoh
            logger.info("Searching for datachunks that do not have GPS data associated to top up.")
            filters.append(~Datachunk.id.in_(used_ids))
            topup_query = (db.session
                           .query(Datachunk, DatachunkStats)
                           .select_from(Datachunk)
                           .join(DatachunkStats)
                           .filter(*filters).options(opts))
            topup_fetched_results = topup_query.all()

            logger.info(f"Starting topping up QCOneResults calculations without GPS for "
                        f"{len(topup_fetched_results)} elements")
            for datachunk, stats in topup_fetched_results:
                qcone_res = calculate_qcone_results(
                    datachunk=datachunk,
                    qcone_config=qcone_config,
                    stats=stats,
                    avg_soh_gps=None
                )
                qcone_results.append(qcone_res)
            logger.info("Topping up calculations finished.")

    else:
        query = (db.session
                 .query(Datachunk, DatachunkStats)
                 .select_from(Datachunk)
                 .join(DatachunkStats)
                 .filter(*filters).options(opts))
        fetched_results = query.all()

        logger.info(f"Starting QCOneResults calculations without GPS for {len(fetched_results)} elements")
        for datachunk, stats in fetched_results:
            qcone_res = calculate_qcone_results(
                datachunk=datachunk,
                qcone_config=qcone_config,
                stats=stats,
                avg_soh_gps=None
            )
            qcone_results.append(qcone_res)
        logger.info("Calculations done for all elements without GPS info associated.")

    logger.info("All processing finished. Trying to insert data into db.")
    add_or_upsert_qcone_results_in_db(qcone_results_collection=qcone_results)


def add_or_upsert_qcone_results_in_db(qcone_results_collection: Collection[QCOneResults]) -> None:
    """
    Adds or upserts provided iterable of QCOneResults to DB.
    Must be executed within AppContext.

    :param qcone_results_collection:
    :type qcone_results_collection: Iterable[Datachunk]
    :return: None
    :rtype: NoneType
    """
    # TODO OPTIMIZE the inserts. there could be extracted datachunk_ids to query for (the qcone_config_id does not
    #  change within this call). Then, the upsert could be executed on the existing only, insert on all the rest.
    #  Gitlab #143

    logger.info(f"Starting insertion procedure. There are {len(qcone_results_collection)} elements to be processed.")
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


def insert_qcone_config_into_db(config: QCOneConfig) -> None:
    """
    This is method simply adding an instance of :class:`~noiz.models.QCOneConfig` to DB and committing changes.

    Has to be executed within `app_context`

    :param qcone_config: Instance of QCOne to be added to db
    :type qcone_config: QCOneConfig
    :return: None
    :rtype: NoneType
    """
    db.session.add(config)
    db.session.commit()
    logger.info(f"Succesfully added to db {type(config)} object with id={config.id}")
    return


def create_and_add_qcone_config_from_toml(
        filepath: Path,
        add_to_db: bool = False
) -> Optional[Tuple[QCOneConfigHolder, QCOneConfig]]:
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
    :rtype: Optional[QCOneConfigHolder]
    """

    params_holder = parse_single_config_toml(filepath=filepath, config_type=DefinedConfigs.QCONE)
    qcone = create_qcone_config(qcone_holder=params_holder)

    if add_to_db:
        insert_qcone_config_into_db(config=qcone)
    else:
        return (params_holder, qcone)
    return None

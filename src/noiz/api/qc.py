import datetime
from loguru import logger
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert, Insert
from sqlalchemy.orm import Query
from typing import List, Collection, Union, Optional, Generator


from noiz.api.component import fetch_components
from noiz.api.crosscorrelations import fetch_crosscorrelation
from noiz.api.datachunk import _determine_filters_and_opts_for_datachunk
from noiz.api.helpers import extract_object_ids, bulk_add_or_upsert_objects, \
    _run_calculate_and_upsert_on_dask, _run_calculate_and_upsert_sequentially
from noiz.api.processing_config import fetch_datachunkparams_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import Datachunk, DatachunkStats, QCOneConfig, QCOneResults, QCTwoConfig, \
    QCTwoResults, AveragedSohGps, Component, Timespan, Crosscorrelation, DatachunkParams
from noiz.models.type_aliases import QCOneRunnerInputs
from noiz.processing.qc import calculate_qctwo_results, calculate_qcone_results_wrapper
from noiz.validation_helpers import validate_to_tuple


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
    """filldocs"""
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
) -> int:
    """filldocs"""

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


def fetch_qctwo_config(ids: Union[int, Collection[int]]) -> List[QCTwoConfig]:
    """
    Fetches the QCTwoConfig from db based on id. Can be either a single id or some collection of ids.
    It always returns a list of instances, can also be an empty list.

    :param ids: IDs to be fetched
    :type ids: Union[int, Collection[int]]
    :return: Fetched QCTwoConfig objects
    :rtype: List[QCTwoConfig]
    """

    ids = validate_to_tuple(val=ids, accepted_type=int)

    fetched = db.session.query(QCTwoConfig).filter(
        QCTwoConfig.id.in_(ids),
    ).all()

    return fetched


def fetch_qctwo_config_single(id: int) -> QCTwoConfig:
    """
    Fetches a single :class:`noiz.models.qc.QCTwoConfig` from db based on id.

    :param id: ID to be fetched
    :type id: int
    :return: Fetched config
    :rtype: QCTwoConfig
    :raises ValueError
    """

    fetched = db.session.query(QCTwoConfig).filter(
        QCTwoConfig.id == id,
    ).first()

    if fetched is None:
        raise EmptyResultException(f"There was no QCOneConfig with if={id} in the database.")

    return fetched


def fetch_qctwo_results(
        qctwo_config: Optional[QCTwoConfig] = None,
        qctwo_config_id: Optional[int] = None,
        crosscorrelations: Optional[Collection[Crosscorrelation]] = None,
        crosscorrelation_ids: Optional[Collection[int]] = None,
) -> List[QCTwoResults]:
    """filldocs"""
    query = _query_qctwo_results(
        qctwo_config=qctwo_config,
        qctwo_config_id=qctwo_config_id,
        crosscorrelations=crosscorrelations,
        crosscorrelation_ids=crosscorrelation_ids,
    )
    return query.all()


def count_qctwo_results(
        qctwo_config: Optional[QCTwoConfig] = None,
        qctwo_config_id: Optional[int] = None,
        crosscorrelations: Optional[Collection[Crosscorrelation]] = None,
        crosscorrelation_ids: Optional[Collection[int]] = None,
) -> int:
    """filldocs"""
    query = _query_qctwo_results(
        qctwo_config=qctwo_config,
        qctwo_config_id=qctwo_config_id,
        crosscorrelations=crosscorrelations,
        crosscorrelation_ids=crosscorrelation_ids,
    )
    return query.count()


def _query_qctwo_results(
        qctwo_config: Optional[QCOneConfig] = None,
        qctwo_config_id: Optional[int] = None,
        crosscorrelations: Optional[Collection[Crosscorrelation]] = None,
        crosscorrelation_ids: Optional[Collection[int]] = None,
) -> Query:
    """filldocs"""

    if crosscorrelations is not None and crosscorrelation_ids is not None:
        raise ValueError("Both crosscorrelations and crosscorrelation_ids parameters were provided. "
                         "You have to provide maximum one of them.")
    if qctwo_config is not None and qctwo_config_id is not None:
        raise ValueError("Both qcone_config and qcone_config_id parameters were provided. "
                         "You have to provide maximum one of them.")

    filters = []
    if qctwo_config is not None:
        filters.append(QCTwoResults.qctwo_config_id.in_((qctwo_config.id,)))
    if qctwo_config_id is not None:
        qcone_config_ids = validate_to_tuple(val=qctwo_config_id, accepted_type=int)
        filters.append(QCTwoResults.qctwo_config_id.in_(qcone_config_ids))
    if crosscorrelations is not None:
        extracted_datachunk_ids = extract_object_ids(crosscorrelations)
        filters.append(QCTwoResults.crosscorrelation_id.in_(extracted_datachunk_ids))
    if crosscorrelation_ids is not None:
        filters.append(QCTwoResults.crosscorrelation_id.in_(crosscorrelation_ids))
    if len(filters) == 0:
        filters.append(True)

    query = QCTwoResults.query.filter(*filters)

    return query


def process_qcone(
        qcone_config_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        batch_size: int = 5000,
        parallel: bool = True,
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
    calculation_inputs = _prepare_inputs_for_qcone_runner(
        qcone_config_id=qcone_config_id,
        starttime=starttime,
        endtime=endtime,
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_qcone_results_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_qcone,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_qcone_results_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_qcone,
        )
    return


def _prepare_inputs_for_qcone_runner(
        qcone_config_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
) -> Generator[QCOneRunnerInputs, None, None]:
    """filldocs"""
    try:
        qcone_config: QCOneConfig = fetch_qcone_config_single(id=qcone_config_id)
    except EmptyResultException as e:
        logger.error(e)
        raise e
    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    datachunk_params = fetch_datachunkparams_by_id(id=qcone_config.datachunk_params_id)
    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )
    calculation_inputs = _generate_inputs_for_qcone_runner(
        qcone_config=qcone_config,
        datachunk_params=datachunk_params,
        components=fetched_components,
        timespans=timespans,
        fetch_gps=qcone_config.uses_gps(),
        fetch_stats=qcone_config.uses_stats,
        top_up_gps=not qcone_config.strict_gps
    )
    return calculation_inputs


def _generate_inputs_for_qcone_runner(
        qcone_config: QCOneConfig,
        datachunk_params: DatachunkParams,
        components: Collection[Component],
        timespans: Collection[Timespan],
        fetch_gps: bool,
        fetch_stats: bool,
        top_up_gps: Optional[bool] = None,
) -> Generator[QCOneRunnerInputs, None, None]:
    """
    Fetches a proper combination of :py:class:`~noiz.models.datachunk.Datachunk`,
    :py:class:`~noiz.models.datachunk.DatachunkStats` and :py:class:`~noiz.models.soh.AveragedSohGps` that is necessary
    for proper calculation of :py:class:`~noiz.models.qc.QCOneResults`.

    :param qcone_config: QCOneConfig for which the query should be done
    :type qcone_config: ~noiz.models.qc.QCOneConfig
    :param components: Components for which the query should be done
    :type components: Collection[~noiz.models.component.Component]
    :param timespans: Timespans for which the query should be done
    :type timespans: Collection[~noiz.models.timespan.Timespan]
    :param fetch_gps: If AveragedSohGps should be joined and fetched with Datachunks
    :type fetch_gps: bool
    :param fetch_stats: If DatachunkStats should be joined and fetched with Datachunks
    :type fetch_stats: bool
    :param top_up_gps: If the Calculations should be topped up if some of the elements of original query are missing.
    :type top_up_gps: Optional[bool]
    :return:
    :rtype:
    """

    filters, opts = _determine_filters_and_opts_for_datachunk(
        components=components,
        timespans=timespans,
        datachunk_params=datachunk_params,
        load_component=False,
        load_timespan=True,
        load_stats=False,
    )

    if top_up_gps is None and fetch_gps is True:
        raise ValueError("If you are passing True for fetch_gps, you have to provide also value for top_up_gps.")

    if fetch_stats and fetch_gps:
        logger.info("Fetching Datachunk, DatachunkStats and AveragedSohGPS for the QCOne.")
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
        fetched_data = query.all()

        logger.info(f"Fetching done. There are {len(fetched_data)} items to process. Starting results generation.")
        used_datachunk_ids = []
        for datachunk, stats, avggps in fetched_data:
            used_datachunk_ids.append(datachunk.id)
            db.session.expunge_all()
            yield QCOneRunnerInputs(
                datachunk=datachunk,
                qcone_config=qcone_config,
                stats=stats,
                avg_soh_gps=avggps,
            )

        if top_up_gps:
            logger.info("Querrying for datachunks that do not have GPS but fit the query.")
            filters.append(~Datachunk.id.in_(used_datachunk_ids))
            topup_query = (db.session
                           .query(Datachunk, DatachunkStats)
                           .select_from(Datachunk)
                           .join(DatachunkStats)
                           .filter(*filters).options(opts))
            fetched_topup_data = topup_query.all()

            logger.info(f"Fetching done. There are {len(fetched_topup_data)} items to process. "
                        f"Starting results generation.")
            for datachunk, stats in fetched_topup_data:
                db.session.expunge_all()
                yield QCOneRunnerInputs(
                    datachunk=datachunk,
                    qcone_config=qcone_config,
                    stats=stats,
                    avg_soh_gps=None
                )

    elif not fetch_stats and fetch_gps:
        logger.info("Fetching Datachunk and AveragedSohGPS for the QCOne.")
        query = (db.session
                 .query(Datachunk, AveragedSohGps)
                 .select_from(Datachunk)
                 .join(AveragedSohGps,
                       and_(
                           Datachunk.device_id == AveragedSohGps.device_id,
                           Datachunk.timespan_id == AveragedSohGps.timespan_id
                       ))
                 .filter(*filters).options(opts))
        fetched_data = query.all()

        logger.info(f"Fetching done. There are {len(fetched_data)} items to process. Starting results generation.")
        used_datachunk_ids = []
        for datachunk, avggps in fetched_data:
            used_datachunk_ids.append(datachunk.id)
            db.session.expunge_all()
            yield QCOneRunnerInputs(
                datachunk=datachunk,
                qcone_config=qcone_config,
                stats=None,
                avg_soh_gps=avggps
            )

        if top_up_gps:
            logger.info("Querrying for datachunks that do not have GPS but fit the query.")
            filters.append(~Datachunk.id.in_(used_datachunk_ids))
            topup_query = (db.session
                           .query(Datachunk)
                           .select_from(Datachunk)
                           .filter(*filters).options(opts))
            fetched_topup_data = topup_query.all()

            logger.info(f"Fetching done. There are {len(fetched_topup_data)} items to process. "
                        f"Starting results generation.")
            for datachunk in fetched_topup_data:
                db.session.expunge_all()
                yield QCOneRunnerInputs(
                    datachunk=datachunk,
                    qcone_config=qcone_config,
                    stats=None,
                    avg_soh_gps=None
                )

    elif fetch_stats and not fetch_gps:
        logger.info("Fetching Datachunk and DatachunkStats for the QCOne.")

        query = (db.session
                 .query(Datachunk, DatachunkStats)
                 .select_from(Datachunk)
                 .join(DatachunkStats)
                 .filter(*filters).options(opts))
        fetched_data = query.all()

        logger.info(f"Fetching done. There are {len(fetched_data)} items to process. Starting results generation.")
        for datachunk, stats in fetched_data:
            db.session.expunge_all()
            yield QCOneRunnerInputs(
                datachunk=datachunk,
                qcone_config=qcone_config,
                stats=stats,
                avg_soh_gps=None
            )
    elif not fetch_stats and not fetch_gps:
        logger.info("Fetching Datachunk for the QCOne.")

        query = (db.session
                 .query(Datachunk)
                 .filter(*filters).options(opts))
        fetched_data = query.all()

        logger.info(f"Fetching done. There are {len(fetched_data)} items to process. Starting results generation.")
        for datachunk in fetched_data:
            db.session.expunge_all()
            yield QCOneRunnerInputs(
                datachunk=datachunk,
                qcone_config=qcone_config,
                stats=None,
                avg_soh_gps=None
            )
    else:
        raise ValueError(f"Despite of having workflow for all combinations of fetch_stats and fetch_gps params "
                         f"you managed to reach here. Congratulations. Go and see whats wrong."
                         f"fetch_stats: {fetch_stats}; fetch_gps: {fetch_gps}, top_up_gps: {top_up_gps}")

    return


def _prepare_upsert_command_qcone(results: QCOneResults) -> Insert:
    """
    Private method that generates an :py:class:`~sqlalchemy.dialects.postgresql.Insert` for
    :py:class:`~noiz.models.qc.QCOneResults` to be upserted to db.
    Postgres specific because it's upsert.

    :param results: Instance which is to be upserted
    :type results: noiz.models.qc.QCOneResults
    :return: Postgres-specific upsert command
    :rtype: sqlalchemy.dialects.postgresql.Insert
    """
    insert_command = (
        insert(QCOneResults)
        .values(
            starttime=results.starttime,
            endtime=results.endtime,
            accepted_time=results.accepted_time,
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
                accepted_time=results.accepted_time,
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
    return insert_command


def process_qctwo(
        qctwo_config_id: int,
):
    try:
        qctwo_config: QCTwoConfig = fetch_qctwo_config_single(id=qctwo_config_id)
    except EmptyResultException as e:
        logger.error(e)
        raise e

    ccfs = fetch_crosscorrelation(
        crosscorrelation_params_id=qctwo_config.crosscorrelation_params_id,
        load_timespan=True,
    )
    qctwo_results = []
    logger.info(f"Starting QCTwoResults calculations {len(ccfs)} elements.")
    for ccf in ccfs:
        qctwo_res = calculate_qctwo_results(
            qctwo_config=qctwo_config,
            crosscorrelation=ccf,
        )
        qctwo_results.append(qctwo_res)
    logger.info("Calculations of QCTwoResults done.")

    logger.info("All processing finished. Trying to insert data into db.")
    bulk_add_or_upsert_objects(
        objects_to_add=qctwo_results,
        upserter_callable=_prepare_upsert_command_qctwo,
        bulk_insert=True,
    )
    return


def _prepare_upsert_command_qctwo(results: QCTwoResults) -> Insert:
    """
    Private method that generates an :py:class:`~sqlalchemy.dialects.postgresql.dml.Insert` for
    :py:class:`~noiz.models.qc.QCTwoResults` to be upserted to db.
    Postgres specific because it's upsert.

    :param results: Instance which is to be upserted
    :type results: noiz.models.qc.QCTwoResults
    :return: Postgres-specific upsert command
    :rtype: sqlalchemy.dialects.postgresql.dml.Insert
    """
    insert_command = (
        insert(QCTwoResults)
        .values(
            starttime=results.starttime,
            endtime=results.endtime,
            accepted_time=results.accepted_time,
            qctwo_config_id=results.qctwo_config_id,
            crosscorrelation_id=results.crosscorrelation_id,
        )
        .on_conflict_do_update(
            constraint="unique_qctwo_results_per_config_per_ccf",
            set_=dict(
                starttime=results.starttime,
                endtime=results.endtime,
                accepted_time=results.accepted_time,
            ),
        )
    )
    return insert_command

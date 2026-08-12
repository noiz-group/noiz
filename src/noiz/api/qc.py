# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
from dataclasses import dataclass
from loguru import logger
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert, Insert
from sqlalchemy.orm import Query, subqueryload
from typing import List, Collection, Union, Optional, Generator, Callable, Tuple, Dict


from noiz.api.component import fetch_components
from noiz.api.crosscorrelations import (
    fetch_crosscorrelation_cartesian,
    count_crosscorrelation_cartesian,
    _query_crosscorrelation_cartesian,
)
from noiz.api.datachunk import _determine_filters_and_opts_for_datachunk
from noiz.api.helpers import (
    extract_object_ids,
    bulk_add_or_upsert_objects,
    _run_calculate_and_upsert_on_dask,
    _run_calculate_and_upsert_sequentially,
    create_process_dask_client,
    recreate_process_dask_client,
)
from noiz.api.processing_config import fetch_datachunkparams_by_id
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import (
    Datachunk,
    DatachunkStats,
    QCOneConfig,
    QCOneResults,
    QCTwoConfig,
    QCTwoResults,
    AveragedSohGps,
    Component,
    Timespan,
    CrosscorrelationCartesian,
    DatachunkParams,
)
from noiz.models.type_aliases import QCOneRunnerInputs, QCTwoRunnerInputs, QCTwoParallelInputs
from noiz.processing.qc import (
    calculate_qctwo_results,
    calculate_qcone_results_wrapper,
    calculate_qctwo_results_wrapper,
)
from noiz.validation_helpers import validate_to_tuple


_QCTWO_WORKER_APP = None


@dataclass
class QCTwoMemoryEstimation:
    """Result of memory estimation for QCTwo processing."""

    ccfs_per_batch: int
    total_ccfs: int
    mem_per_ccf: float  # Memory per CCF object in bytes
    usable_ram: float
    num_batches: int


def _estimate_ccfs_per_batch(
    total_ccfs: int,
    load_timespan: bool = True,
    ram_safety_factor: float = 0.5,
    max_ccfs_per_batch: int = 50000,
) -> QCTwoMemoryEstimation:
    """
    Estimate optimal number of CCFs to load at once based on available RAM.

    Memory estimation factors:
    - CrosscorrelationCartesian SQLAlchemy object: ~2KB base overhead
    - Timespan relationship (if loaded): ~500 bytes additional
    - QCTwoResults output object: ~1KB per CCF
    - Python list overhead: ~8 bytes per element

    :param total_ccfs: Total number of CCFs to process
    :param load_timespan: Whether timespan relationship is being loaded
    :param ram_safety_factor: Fraction of available RAM to use (0.0-1.0), default 0.5 (50%)
    :param max_ccfs_per_batch: Maximum CCFs per batch regardless of RAM
    :return: QCTwoMemoryEstimation with recommended batch size
    """
    import psutil

    # Get available system RAM
    mem = psutil.virtual_memory()
    available_ram_bytes = mem.available
    total_ram_bytes = mem.total

    logger.info(
        f"QCTwo RAM estimation: Total={total_ram_bytes / 1024**3:.2f}GB, "
        f"Available={available_ram_bytes / 1024**3:.2f}GB"
    )

    # Memory per CCF object estimation (in bytes)
    # Base SQLAlchemy CrosscorrelationCartesian object
    ccf_object_overhead = 2048  # ~2KB per SQLAlchemy object with relationships

    # Timespan relationship if loaded
    timespan_overhead = 512 if load_timespan else 0  # ~500 bytes

    # QCTwoResults output object (we accumulate these in a list)
    qctwo_result_overhead = 1024  # ~1KB per result object

    # Python list and reference overhead
    list_overhead = 64  # ~64 bytes per list element (reference + overhead)

    # Total memory per CCF (input + output)
    mem_per_ccf = ccf_object_overhead + timespan_overhead + qctwo_result_overhead + list_overhead

    # Add base Python/process overhead (~300MB)
    base_overhead = 300 * 1024**2

    # Calculate usable RAM
    usable_ram = (available_ram_bytes - base_overhead) * ram_safety_factor

    if usable_ram <= 0:
        logger.warning(f"Very low available RAM ({available_ram_bytes / 1024**3:.2f}GB).")
        usable_ram = 100 * 1024**2  # Minimum 100MB

    # Calculate optimal CCFs per batch
    optimal_ccfs_raw = usable_ram / mem_per_ccf
    optimal_ccfs = int(optimal_ccfs_raw)
    optimal_ccfs = max(100, min(optimal_ccfs, max_ccfs_per_batch, total_ccfs))

    # Calculate number of batches needed
    num_batches = (total_ccfs + optimal_ccfs - 1) // optimal_ccfs  # Ceiling division

    logger.info(
        f"QCTwo RAM estimation breakdown:\n"
        f"  - Total CCFs to process: {total_ccfs:,}\n"
        f"  - Memory per CCF: {mem_per_ccf / 1024:.2f}KB\n"
        f"    - CCF object: {ccf_object_overhead / 1024:.2f}KB\n"
        f"    - Timespan overhead: {timespan_overhead / 1024:.2f}KB\n"
        f"    - QCTwoResult object: {qctwo_result_overhead / 1024:.2f}KB\n"
        f"    - List overhead: {list_overhead} bytes\n"
        f"  - Usable RAM: {usable_ram / 1024**3:.2f}GB (safety factor: {ram_safety_factor})\n"
        f"  - Recommended CCFs per batch: {optimal_ccfs:,}\n"
        f"  - Number of batches: {num_batches}"
    )

    return QCTwoMemoryEstimation(
        ccfs_per_batch=optimal_ccfs,
        total_ccfs=total_ccfs,
        mem_per_ccf=mem_per_ccf,
        usable_ram=usable_ram,
        num_batches=num_batches,
    )


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

    fetched = (
        db.session.query(QCOneConfig)
        .filter(
            QCOneConfig.id.in_(ids),
        )
        .all()
    )

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

    fetched = (
        db.session.query(QCOneConfig)
        .filter(
            QCOneConfig.id == id,
        )
        .first()
    )

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
        raise ValueError(
            "Both datachunks and datachunk_ids parameters were provided. You have to provide maximum one of them."
        )
    if qcone_config is not None and qcone_config_id is not None:
        raise ValueError(
            "Both qcone_config and qcone_config_id parameters were provided. You have to provide maximum one of them."
        )

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

    fetched = (
        db.session.query(QCTwoConfig)
        .filter(
            QCTwoConfig.id.in_(ids),
        )
        .all()
    )

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

    fetched = (
        db.session.query(QCTwoConfig)
        .filter(
            QCTwoConfig.id == id,
        )
        .first()
    )

    if fetched is None:
        raise EmptyResultException(f"There was no QC Config with id={id} in the database.")

    return fetched


def fetch_qctwo_results(
    qctwo_config: Optional[QCTwoConfig] = None,
    qctwo_config_id: Optional[int] = None,
    crosscorrelations_cartesian: Optional[Collection[CrosscorrelationCartesian]] = None,
    crosscorrelation_cartesian_ids: Optional[Collection[int]] = None,
) -> List[QCTwoResults]:
    """filldocs"""
    query = _query_qctwo_results(
        qctwo_config=qctwo_config,
        qctwo_config_id=qctwo_config_id,
        crosscorrelations_cartesian=crosscorrelations_cartesian,
        crosscorrelation_cartesian_ids=crosscorrelation_cartesian_ids,
    )
    return query.all()


def count_qctwo_results(
    qctwo_config: Optional[QCTwoConfig] = None,
    qctwo_config_id: Optional[int] = None,
    crosscorrelations_cartesian: Optional[Collection[CrosscorrelationCartesian]] = None,
    crosscorrelation_cartesian_ids: Optional[Collection[int]] = None,
) -> int:
    """filldocs"""
    query = _query_qctwo_results(
        qctwo_config=qctwo_config,
        qctwo_config_id=qctwo_config_id,
        crosscorrelations_cartesian=crosscorrelations_cartesian,
        crosscorrelation_cartesian_ids=crosscorrelation_cartesian_ids,
    )
    return query.count()


def _query_qctwo_results(
    qctwo_config: Optional[QCOneConfig] = None,
    qctwo_config_id: Optional[int] = None,
    crosscorrelations_cartesian: Optional[Collection[CrosscorrelationCartesian]] = None,
    crosscorrelation_cartesian_ids: Optional[Collection[int]] = None,
) -> Query:
    """filldocs"""

    if crosscorrelations_cartesian is not None and crosscorrelation_cartesian_ids is not None:
        raise ValueError(
            "Both crosscorrelations_cartesian and crosscorrelation_cartesian_ids parameters were provided. "
            "You have to provide maximum one of them."
        )
    if qctwo_config is not None and qctwo_config_id is not None:
        raise ValueError(
            "Both qcone_config and qcone_config_id parameters were provided. You have to provide maximum one of them."
        )

    filters = []
    if qctwo_config is not None:
        filters.append(QCTwoResults.qctwo_config_id.in_((qctwo_config.id,)))
    if qctwo_config_id is not None:
        qcone_config_ids = validate_to_tuple(val=qctwo_config_id, accepted_type=int)
        filters.append(QCTwoResults.qctwo_config_id.in_(qcone_config_ids))
    if crosscorrelations_cartesian is not None:
        extracted_datachunk_ids = extract_object_ids(crosscorrelations_cartesian)
        filters.append(QCTwoResults.crosscorrelation_cartesian_id.in_(extracted_datachunk_ids))
    if crosscorrelation_cartesian_ids is not None:
        filters.append(QCTwoResults.crosscorrelation_cartesian_id.in_(crosscorrelation_cartesian_ids))
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
        component_ids=component_ids,
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
        top_up_gps=not qcone_config.strict_gps,
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
        query = (
            db.session.query(Datachunk, DatachunkStats, AveragedSohGps)
            .select_from(Datachunk)
            .join(
                AveragedSohGps,
                and_(
                    Datachunk.device_id == AveragedSohGps.device_id,
                    Datachunk.timespan_id == AveragedSohGps.timespan_id,
                ),
            )
            .join(DatachunkStats)
            .filter(*filters)
            .options(opts)
        )
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
            topup_query = (
                db.session.query(Datachunk, DatachunkStats)
                .select_from(Datachunk)
                .join(DatachunkStats)
                .filter(*filters)
                .options(opts)
            )
            fetched_topup_data = topup_query.all()

            logger.info(
                f"Fetching done. There are {len(fetched_topup_data)} items to process. Starting results generation."
            )
            for datachunk, stats in fetched_topup_data:
                db.session.expunge_all()
                yield QCOneRunnerInputs(datachunk=datachunk, qcone_config=qcone_config, stats=stats, avg_soh_gps=None)

    elif not fetch_stats and fetch_gps:
        logger.info("Fetching Datachunk and AveragedSohGPS for the QCOne.")
        query = (
            db.session.query(Datachunk, AveragedSohGps)
            .select_from(Datachunk)
            .join(
                AveragedSohGps,
                and_(
                    Datachunk.device_id == AveragedSohGps.device_id,
                    Datachunk.timespan_id == AveragedSohGps.timespan_id,
                ),
            )
            .filter(*filters)
            .options(opts)
        )
        fetched_data = query.all()

        logger.info(f"Fetching done. There are {len(fetched_data)} items to process. Starting results generation.")
        used_datachunk_ids = []
        for datachunk, avggps in fetched_data:
            used_datachunk_ids.append(datachunk.id)
            db.session.expunge_all()
            yield QCOneRunnerInputs(datachunk=datachunk, qcone_config=qcone_config, stats=None, avg_soh_gps=avggps)

        if top_up_gps:
            logger.info("Querrying for datachunks that do not have GPS but fit the query.")
            filters.append(~Datachunk.id.in_(used_datachunk_ids))
            topup_query = db.session.query(Datachunk).select_from(Datachunk).filter(*filters).options(opts)
            fetched_topup_data = topup_query.all()

            logger.info(
                f"Fetching done. There are {len(fetched_topup_data)} items to process. Starting results generation."
            )
            for datachunk in fetched_topup_data:
                db.session.expunge_all()
                yield QCOneRunnerInputs(datachunk=datachunk, qcone_config=qcone_config, stats=None, avg_soh_gps=None)

    elif fetch_stats and not fetch_gps:
        logger.info("Fetching Datachunk and DatachunkStats for the QCOne.")

        query = (
            db.session.query(Datachunk, DatachunkStats)
            .select_from(Datachunk)
            .join(DatachunkStats)
            .filter(*filters)
            .options(opts)
        )
        fetched_data = query.all()

        logger.info(f"Fetching done. There are {len(fetched_data)} items to process. Starting results generation.")
        for datachunk, stats in fetched_data:
            db.session.expunge_all()
            yield QCOneRunnerInputs(datachunk=datachunk, qcone_config=qcone_config, stats=stats, avg_soh_gps=None)
    elif not fetch_stats and not fetch_gps:
        logger.info("Fetching Datachunk for the QCOne.")

        query = db.session.query(Datachunk).filter(*filters).options(opts)
        fetched_data = query.all()

        logger.info(f"Fetching done. There are {len(fetched_data)} items to process. Starting results generation.")
        for datachunk in fetched_data:
            db.session.expunge_all()
            yield QCOneRunnerInputs(datachunk=datachunk, qcone_config=qcone_config, stats=None, avg_soh_gps=None)
    else:
        raise ValueError(
            f"Despite of having workflow for all combinations of fetch_stats and fetch_gps params "
            f"you managed to reach here. Congratulations. Go and see whats wrong."
            f"fetch_stats: {fetch_stats}; fetch_gps: {fetch_gps}, top_up_gps: {top_up_gps}"
        )

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
            set_={
                "starttime": results.starttime,
                "endtime": results.endtime,
                "accepted_time": results.accepted_time,
                "avg_gps_time_error_min": results.avg_gps_time_error_min,
                "avg_gps_time_error_max": results.avg_gps_time_error_max,
                "avg_gps_time_uncertainty_min": results.avg_gps_time_uncertainty_min,
                "avg_gps_time_uncertainty_max": results.avg_gps_time_uncertainty_max,
                "signal_energy_min": results.signal_energy_min,
                "signal_energy_max": results.signal_energy_max,
                "signal_min_value_min": results.signal_min_value_min,
                "signal_min_value_max": results.signal_min_value_max,
                "signal_max_value_min": results.signal_max_value_min,
                "signal_max_value_max": results.signal_max_value_max,
                "signal_mean_value_min": results.signal_mean_value_min,
                "signal_mean_value_max": results.signal_mean_value_max,
                "signal_variance_min": results.signal_variance_min,
                "signal_variance_max": results.signal_variance_max,
                "signal_skewness_min": results.signal_skewness_min,
                "signal_skewness_max": results.signal_skewness_max,
                "signal_kurtosis_min": results.signal_kurtosis_min,
                "signal_kurtosis_max": results.signal_kurtosis_max,
            },
        )
    )
    return insert_command


def process_qctwo(
    qctwo_config_id: int,
    batch_size: int = 5000,
    parallel: bool = True,
    ram_safety_factor: float = 0.5,
    max_ccfs_per_batch: int = 50000,
):
    """
    Process QCTwo for all crosscorrelation cartesian results associated with a QCTwoConfig.

    This function loads CCFs in batches to avoid excessive RAM consumption.
    The batch size is automatically determined based on available RAM.
    Supports both parallel (Dask) and sequential processing modes.

    :param qctwo_config_id: ID of QCTwoConfig from the database
    :param batch_size: Number of CCFs to process per Dask batch (for parallel mode)
    :param parallel: If True (default), use Dask for parallel processing; if False, process sequentially
    :param ram_safety_factor: Fraction of available RAM to use (0.0-1.0), default 0.5 (50%)
    :param max_ccfs_per_batch: Maximum CCFs to load per batch regardless of RAM (for sequential mode)
    """
    import more_itertools

    try:
        qctwo_config: QCTwoConfig = fetch_qctwo_config_single(id=qctwo_config_id)
    except EmptyResultException as e:
        logger.error(e)
        raise e

    # Count total CCFs first (lightweight query)
    total_ccfs = count_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id=qctwo_config.crosscorrelation_cartesian_params_id,
    )

    if total_ccfs == 0:
        logger.warning("No crosscorrelation cartesian results found for the given config. Nothing to process.")
        return

    logger.info(f"Found {total_ccfs:,} CCFs to process for QCTwo.")

    if parallel:
        logger.info("Processing QCTwo in parallel mode using Dask.")
        _run_qctwo_on_dask(
            qctwo_config_id=qctwo_config.id,
            batch_size=batch_size,
            inputs=_generate_inputs_for_qctwo_runner_parallel(
                qctwo_config=qctwo_config,
                total_ccfs=total_ccfs,
                ram_safety_factor=ram_safety_factor,
                max_ccfs_per_batch=max_ccfs_per_batch,
            ),
            upserter_callable=_prepare_upsert_command_qctwo,
        )
    else:
        logger.info("Processing QCTwo in sequential mode.")
        _run_qctwo_sequentially(
            upsert_batch_size=max_ccfs_per_batch,
            inputs=_generate_inputs_for_qctwo_runner(
                qctwo_config=qctwo_config,
                total_ccfs=total_ccfs,
                ram_safety_factor=ram_safety_factor,
                max_ccfs_per_batch=max_ccfs_per_batch,
            ),
            upserter_callable=_prepare_upsert_command_qctwo,
        )

    logger.info("QCTwo processing completed.")
    return


def _run_qctwo_sequentially(
    inputs: Generator[QCTwoRunnerInputs, None, None],
    upserter_callable: Callable[[QCTwoResults], Insert],
    upsert_batch_size: int = 50000,
):
    """
    Run QCTwo processing sequentially with efficient batch upserts.

    This function processes items from the generator and upserts results in batches.
    Since the generator already handles memory-efficient loading from DB, we simply
    accumulate results and upsert every upsert_batch_size items.

    :param inputs: Generator of QCTwoRunnerInputs
    :param upserter_callable: Function to prepare upsert command
    :param upsert_batch_size: Number of results to accumulate before upserting
    """
    results: List[QCTwoResults] = []
    total_processed = 0
    batch_num = 0

    for inp in inputs:
        res = calculate_qctwo_results_wrapper(inp)
        if res is not None:
            results.extend(res)

        # Upsert when we've accumulated enough results
        if len(results) >= upsert_batch_size:
            logger.info(f"Batch {batch_num}: Upserting {len(results):,} results...")
            bulk_add_or_upsert_objects(
                objects_to_add=results,
                upserter_callable=upserter_callable,
                bulk_insert=True,
            )
            total_processed += len(results)
            logger.info(f"Batch {batch_num}: Upsert completed. Total processed: {total_processed:,}")
            results = []
            batch_num += 1

    # Upsert any remaining results
    if results:
        logger.info(f"Final batch {batch_num}: Upserting {len(results):,} remaining results...")
        bulk_add_or_upsert_objects(
            objects_to_add=results,
            upserter_callable=upserter_callable,
            bulk_insert=True,
        )
        total_processed += len(results)
        logger.info(f"Final batch {batch_num}: Upsert completed.")

    logger.info(f"All processing is done. Total processed: {total_processed:,}")
    return


def _run_qctwo_on_dask(
    inputs: Generator[Tuple[int, ...], None, None],
    upserter_callable: Callable[[QCTwoResults], Insert],
    qctwo_config_id: int,
    batch_size: int = 5000,
):
    """
    Run QCTwo processing on Dask with efficient batch upserts.

    Unlike the generic _run_calculate_and_upsert_on_dask, this function:
    - Waits for ALL results in a batch before upserting (more efficient for lightweight calculations)
    - Performs a single bulk upsert per batch instead of many small ones

    :param inputs: Generator of QCTwoRunnerInputs
    :param upserter_callable: Function to prepare upsert command
    :param batch_size: Number of inputs per batch
    """
    import more_itertools
    from dask.distributed import wait

    client, cluster, n_workers = create_process_dask_client()
    logger.info(f"Dask client started. Dashboard: {client.dashboard_link}")

    total_processed = 0
    batch_num = 0

    for batch_num, ccf_id_batch in enumerate(inputs):
        if not ccf_id_batch:
            continue

        scheduler_worker_count = len(client.scheduler_info().get("workers", {}))
        effective_worker_count = max(1, scheduler_worker_count, len(getattr(cluster, "workers", {})), n_workers)
        requested_task_batch_size = max(1, batch_size)
        worker_target_chunk_size = max(1, len(ccf_id_batch) // effective_worker_count)
        per_task_chunk_size = max(requested_task_batch_size, worker_target_chunk_size)
        chunked_inputs = _prepare_qctwo_parallel_task_inputs(
            ccf_id_batch=ccf_id_batch,
            qctwo_config_id=qctwo_config_id,
            per_task_chunk_size=per_task_chunk_size,
        )

        logger.info(
            f"Batch {batch_num}: submitting {len(ccf_id_batch):,} QCTwo items as {len(chunked_inputs)} Dask tasks "
            f"(~{per_task_chunk_size} CCFs per task, effective_workers={effective_worker_count})."
        )

        futures = [
            client.submit(_calculate_qctwo_results_batch_by_id_wrapper, inp_chunk) for inp_chunk in chunked_inputs
        ]

        wait(futures)

        finished_futures = [future for future in futures if future.status == "finished"]
        non_finished_futures = [future for future in futures if future.status != "finished"]

        if non_finished_futures:
            statuses = [future.status for future in non_finished_futures]
            for future in non_finished_futures:
                try:
                    logger.error(f"QCTwo task {future.key} ended with status {future.status}: {future.exception()}")
                except Exception as exception_error:
                    logger.error(
                        f"QCTwo task {future.key} ended with status {future.status} and exception could not be read: {exception_error}"
                    )
            raise RuntimeError(
                f"Dask returned cancelled or failed QCTwo tasks. Aborting to avoid partial database writes. Statuses: {statuses}"
            )

        results_nested = client.gather(finished_futures)

        results = []
        for res in results_nested:
            if res is not None:
                results.extend(res)

        logger.info(f"Batch {batch_num}: Upserting {len(results):,} results...")

        # Single bulk upsert for the entire batch
        if results:
            bulk_add_or_upsert_objects(
                objects_to_add=results,
                upserter_callable=upserter_callable,
                bulk_insert=True,
            )

        total_processed += len(results)
        logger.info(f"Batch {batch_num}: Upsert completed. Total processed: {total_processed:,}")

    client.close()
    cluster.close()
    logger.info(f"All processing is done. Total processed: {total_processed:,}")
    return


def _calculate_qctwo_results_batch_wrapper(
    inputs_batch: Tuple[QCTwoRunnerInputs, ...],
) -> Tuple[QCTwoResults, ...]:
    results: List[QCTwoResults] = []
    for inputs in inputs_batch:
        results.extend(calculate_qctwo_results_wrapper(inputs))
    return tuple(results)


def _get_qctwo_worker_app():
    global _QCTWO_WORKER_APP
    if _QCTWO_WORKER_APP is None:
        from noiz.app import create_app

        _QCTWO_WORKER_APP = create_app()
    return _QCTWO_WORKER_APP


def _fetch_crosscorrelation_cartesian_for_qctwo_ids(
    crosscorrelation_cartesian_ids: Collection[int],
) -> List[CrosscorrelationCartesian]:
    return (
        db.session.query(CrosscorrelationCartesian)
        .filter(CrosscorrelationCartesian.id.in_(crosscorrelation_cartesian_ids))
        .options(subqueryload(CrosscorrelationCartesian.timespan))
        .order_by(CrosscorrelationCartesian.id)
        .all()
    )


def _prepare_qctwo_parallel_task_inputs(
    ccf_id_batch: Tuple[int, ...],
    qctwo_config_id: int,
    per_task_chunk_size: int,
) -> List[QCTwoParallelInputs]:
    import more_itertools

    return [
        QCTwoParallelInputs(
            qctwo_config_id=qctwo_config_id,
            crosscorrelation_cartesian_ids=tuple(chunk),
        )
        for chunk in more_itertools.chunked(ccf_id_batch, per_task_chunk_size)
    ]


def _calculate_qctwo_results_batch_by_id_wrapper(
    inputs_batch: QCTwoParallelInputs,
) -> Tuple[QCTwoResults, ...]:
    app = _get_qctwo_worker_app()
    with app.app_context():
        try:
            qctwo_config = fetch_qctwo_config_single(id=inputs_batch["qctwo_config_id"])
            _ = qctwo_config.time_periods_rejected
            _ = qctwo_config.componentpair_ids_rejected_times
            _ = qctwo_config.null_value

            ccfs = _fetch_crosscorrelation_cartesian_for_qctwo_ids(inputs_batch["crosscorrelation_cartesian_ids"])
            results: List[QCTwoResults] = []
            for ccf in ccfs:
                results.extend(
                    calculate_qctwo_results_wrapper(
                        QCTwoRunnerInputs(
                            crosscorrelation_cartesian=ccf,
                            qctwo_config=qctwo_config,
                        )
                    )
                )
            return tuple(results)
        finally:
            db.session.remove()


def _generate_inputs_for_qctwo_runner_parallel(
    qctwo_config: QCTwoConfig,
    total_ccfs: int,
    ram_safety_factor: float = 0.5,
    max_ccfs_per_batch: int = 50000,
) -> Generator[Tuple[int, ...], None, None]:
    mem_estimation = _estimate_ccfs_per_batch(
        total_ccfs=total_ccfs,
        load_timespan=False,
        ram_safety_factor=ram_safety_factor,
        max_ccfs_per_batch=max_ccfs_per_batch,
    )

    ccfs_per_batch = mem_estimation.ccfs_per_batch
    num_batches = mem_estimation.num_batches

    logger.info(f"Loading CCF ids in {num_batches} batch(es) of up to {ccfs_per_batch:,} CCFs each.")

    base_query = _query_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id=qctwo_config.crosscorrelation_cartesian_params_id,
        load_timespan=False,
    )

    for batch_idx in range(num_batches):
        offset = batch_idx * ccfs_per_batch
        ccf_id_batch = tuple(
            row.id
            for row in base_query.with_entities(CrosscorrelationCartesian.id)
            .order_by(CrosscorrelationCartesian.id)
            .offset(offset)
            .limit(ccfs_per_batch)
            .all()
        )

        if not ccf_id_batch:
            logger.info(f"Batch {batch_idx + 1}/{num_batches}: No more CCF ids to load.")
            break

        logger.info(f"Batch {batch_idx + 1}/{num_batches}: Loaded {len(ccf_id_batch):,} CCF ids (offset={offset:,})")
        yield ccf_id_batch


def _generate_inputs_for_qctwo_runner(
    qctwo_config: QCTwoConfig,
    total_ccfs: int,
    ram_safety_factor: float = 0.5,
    max_ccfs_per_batch: int = 50000,
) -> Generator[QCTwoRunnerInputs, None, None]:
    """
    Generator that yields QCTwoRunnerInputs for each CCF to be processed.

    Loads CCFs in memory-efficient batches and yields them one by one for processing.
    This allows Dask or sequential processing to handle memory management.

    Objects are expunged from the session after yielding to allow proper serialization
    to Dask workers.

    :param qctwo_config: QCTwoConfig for the processing
    :param total_ccfs: Total number of CCFs to process
    :param ram_safety_factor: Fraction of available RAM to use (0.0-1.0)
    :param max_ccfs_per_batch: Maximum CCFs to load per batch
    :yield: QCTwoRunnerInputs for each CCF
    """
    # Pre-load all lazy relationships on qctwo_config that will be needed during processing
    # This ensures they're available after session expunge
    _ = qctwo_config.time_periods_rejected  # Force load relationship
    _ = qctwo_config.componentpair_ids_rejected_times  # Force compute cached property
    _ = qctwo_config.null_value  # Force compute cached property

    # Estimate optimal batch size based on RAM
    mem_estimation = _estimate_ccfs_per_batch(
        total_ccfs=total_ccfs,
        load_timespan=True,
        ram_safety_factor=ram_safety_factor,
        max_ccfs_per_batch=max_ccfs_per_batch,
    )

    ccfs_per_batch = mem_estimation.ccfs_per_batch
    num_batches = mem_estimation.num_batches

    logger.info(f"Loading CCFs in {num_batches} batch(es) of up to {ccfs_per_batch:,} CCFs each.")

    # Get the base query for pagination
    base_query = _query_crosscorrelation_cartesian(
        crosscorrelation_cartesian_params_id=qctwo_config.crosscorrelation_cartesian_params_id,
        load_timespan=True,
    )

    # Load in batches and yield individual items
    for batch_idx in range(num_batches):
        offset = batch_idx * ccfs_per_batch

        # Fetch batch using LIMIT/OFFSET
        ccfs_batch = base_query.order_by(CrosscorrelationCartesian.id).offset(offset).limit(ccfs_per_batch).all()

        if not ccfs_batch:
            logger.info(f"Batch {batch_idx + 1}/{num_batches}: No more CCFs to load.")
            break

        logger.info(f"Batch {batch_idx + 1}/{num_batches}: Loaded {len(ccfs_batch):,} CCFs (offset={offset:,})")

        for ccf in ccfs_batch:
            # Expunge all objects from session to allow proper serialization to Dask workers
            # This is the same pattern used in _generate_inputs_for_qcone_runner
            db.session.expunge_all()
            yield QCTwoRunnerInputs(
                crosscorrelation_cartesian=ccf,
                qctwo_config=qctwo_config,
            )


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
            crosscorrelation_cartesian_id=results.crosscorrelation_cartesian_id,
        )
        .on_conflict_do_update(
            constraint="unique_qctwo_results_per_config_per_ccf",
            set_={
                "starttime": results.starttime,
                "endtime": results.endtime,
                "accepted_time": results.accepted_time,
            },
        )
    )
    return insert_command

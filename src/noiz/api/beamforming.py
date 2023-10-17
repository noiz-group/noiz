# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from collections import defaultdict
import datetime
import more_itertools
from loguru import logger
import pandas as pd
from sqlalchemy.dialects.postgresql import insert, Insert
from sqlalchemy.orm import subqueryload, Query
from sqlalchemy.sql.elements import BinaryExpression
from typing import Union, Collection, Optional, List, Tuple, Generator, Dict

from noiz.api.component import fetch_components
from noiz.api.helpers import extract_object_ids, _run_calculate_and_upsert_sequentially, \
    _run_calculate_and_upsert_on_dask, _parse_query_as_dataframe
from noiz.api.qc import fetch_qcone_config_single
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.models.type_aliases import BeamformingRunnerInputs
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import Timespan, Datachunk, QCOneResults, BeamformingParams
from noiz.models.beamforming import BeamformingResult, BeamformingPeakAverageAbspower, \
    association_table_beamforming_result_avg_abspower, BeamformingResultType, \
    BeamformingPeakAllAbspower, association_table_beamforming_result_all_abspower, \
    BeamformingPeakAllRelpower, association_table_beamforming_result_all_relpower, \
    BeamformingPeakAverageRelpower, association_table_beamforming_result_avg_relpower, \
    BeamformingFile
from noiz.processing.beamforming import calculate_beamforming_results_wrapper, \
    validate_if_all_beamforming_params_use_same_component_codes, validate_if_all_beamforming_params_use_same_qcone
from noiz.validation_helpers import validate_to_tuple, validate_maximum_one_argument_provided


def fetch_beamforming_params_single(params_id: int) -> BeamformingParams:
    """
    Fetches a BeamformingParams objects by its ID.

    :param params_id: ID of beamforming params to be fetched
    :type params_id: int
    :return: fetched BeamformingParams object
    :rtype: BeamformingParams
    """
    fetched_params = BeamformingParams.query.filter_by(id=params_id).first()
    if fetched_params is None:
        raise EmptyResultException(f"BeamformingParams object of id {params_id} does not exist.")

    return fetched_params


def fetch_beamforming_params(
        ids: Optional[Collection[int]] = None,
        load_qcone_config: bool = True,
) -> List[BeamformingParams]:
    """
    Fetches a BeamformingParams objects by its ID.

    :param ids: ID of beamforming params to be fetched
    :type ids: Collection[int]
    :param load_qcone_config: If qcone_config should be subquery loaded
    :type load_qcone_config: bool
    :return: List of fetched BeamformingParams objects
    :rtype: List[BeamformingParams]
    """
    filters = []
    if ids is not None:
        filters.append(BeamformingParams.id.in_(ids))
    if len(filters) == 0:
        filters.append(True)
    opts = []
    if load_qcone_config:
        opts.append(subqueryload(BeamformingParams.qcone_config))

    fetched_params = (BeamformingParams.query
                      .filter(*filters)
                      .options(opts)
                      .all())
    if len(fetched_params) == 0:
        raise EmptyResultException(f"BeamformingParams with ids of {ids} do not exist.")

    return fetched_params


def run_beamforming(
        beamforming_params_ids: Union[int, Tuple[int, ...]],
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        skip_existing: bool = True,
        batch_size: int = 500,
        parallel: bool = True,
        raise_errors: bool = True,
):

    calculation_inputs = _prepare_inputs_for_beamforming_runner(
        beamforming_params_ids=validate_to_tuple(beamforming_params_ids, int),
        starttime=starttime,
        endtime=endtime,
        networks=networks,
        stations=stations,
        component_ids=component_ids,
        skip_existing=skip_existing,
        batch_size=batch_size
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_beamforming_results_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_beamforming,
            with_file=True,
            is_beamforming=True,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_beamforming_results_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_beamforming,
            raise_errors=raise_errors,
            with_file=True,
            is_beamforming=True,
        )
    return


def _prepare_inputs_for_beamforming_runner(
        beamforming_params_ids: Collection[int],
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        skip_existing: bool = True,
        batch_size: int = 500,
) -> Generator[BeamformingRunnerInputs, None, None]:

    logger.debug(f"Fetching BeamformingParams with ids {beamforming_params_ids}")
    params = fetch_beamforming_params(ids=beamforming_params_ids)
    fetched_params_ids = extract_object_ids(params)
    fetched_params_ids.sort()
    logger.debug(f"Fetching BeamformingParams successful. {params}")

    single_qcone_config_id = validate_if_all_beamforming_params_use_same_qcone(params)
    single_used_component_codes = validate_if_all_beamforming_params_use_same_component_codes(params)
    global_minimum_trace_count = min([x.minimum_trace_count for x in params])

    logger.debug(f"Fetching QCOneConfig with id {single_qcone_config_id}")
    qcone_config = fetch_qcone_config_single(single_qcone_config_id)
    logger.debug(f"Fetching QCOneConfig successful. {qcone_config}")

    logger.debug(f"Fetching timespans for {starttime} - {endtime}")
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    logger.debug(f"Fetched {len(fetched_timespans)} timespans")

    logger.debug("Fetching components")
    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=single_used_component_codes,
        component_ids=component_ids,
    )
    logger.debug(f"Fetched {len(fetched_components)} components")

    for timespan_batch in more_itertools.chunked(iterable=fetched_timespans, n=batch_size):

        selection: List[Tuple[Timespan, Datachunk, QCOneResults]] = (
            db.session.query(Timespan, Datachunk, QCOneResults)
                      .select_from(Timespan)
                      .join(Datachunk, Datachunk.timespan_id == Timespan.id)
                      .join(QCOneResults, Datachunk.id == QCOneResults.datachunk_id)
                      .filter(Timespan.id.in_(extract_object_ids(timespan_batch)))  # type: ignore
                      .filter(Datachunk.component_id.in_(extract_object_ids(fetched_components)))
                      .filter(QCOneResults.qcone_config_id == qcone_config.id)
                      .options(subqueryload(Datachunk.component))
                      .order_by(Timespan.id)
                      .all()
        )

        grouped_by_tid: Dict[Timespan, List[Tuple[Datachunk, QCOneResults]]] = defaultdict(list)

        logger.debug("Grouping potential inputs by timespan")
        for timespan, datachunk, qconeresult in selection:
            grouped_by_tid[timespan].append((datachunk, qconeresult))
        logger.debug(f"Grouping done. There are {len(grouped_by_tid)} timespans with data.")

        grouped_existing_beam_param_ids: Dict[int, List[int]] = defaultdict(list)
        if skip_existing:
            logger.debug("Querying for existing beamforming results")
            existing_res = (
                db.session.query(Timespan.id, BeamformingResult.beamforming_params_id)
                .select_from(Timespan)
                .join(BeamformingResult, BeamformingResult.timespan_id == Timespan.id)
                .filter(Timespan.id.in_(extract_object_ids(timespan_batch)))  # type: ignore
                .filter(BeamformingResult.beamforming_params_id.in_(fetched_params_ids))
                .all()
            )

            for tid, params_id in existing_res:
                grouped_existing_beam_param_ids[tid].append(params_id)

        for ts, group in grouped_by_tid.items():
            if skip_existing:
                existing_beamforming_for_timespan = grouped_existing_beam_param_ids[ts.id]
                existing_beamforming_for_timespan.sort()

                if existing_beamforming_for_timespan == fetched_params_ids:
                    logger.debug(f"All beamforming operations for timespan {ts} are finished")
                    continue
                else:
                    used_params = [x for x in params if x.id not in grouped_existing_beam_param_ids[ts.id]]
            else:
                used_params = params
            logger.debug(f"There are {len(used_params)} beamformings to be done for timespan {ts}. ")

            passing_chunks = [chunk for chunk, qconeresult in group if qconeresult.is_passing()]
            logger.debug(f"There are {len(passing_chunks)} passing QCOne out of {len(group)} datachunks in total "
                         f"for timespan {ts}")

            if len(passing_chunks) < global_minimum_trace_count:
                logger.warning(f"There was not enough traces passing QCOne for the beamforming. "
                               f"Skipping this timespan. Timespan: {ts} "
                               f"Global minimum trace count: {global_minimum_trace_count}. "
                               f"Passing traces: {len(passing_chunks)}")
                continue

            db.session.expunge_all()
            yield BeamformingRunnerInputs(
                beamforming_params=used_params,
                timespan=ts,
                datachunks=tuple(passing_chunks),
            )


def _prepare_upsert_command_beamforming(results: BeamformingResult) -> Insert:
    """
    Private method that generates an :py:class:`~sqlalchemy.dialects.postgresql.Insert` for
    :py:class:`~noiz.models.beamforming.BeamformingResult` to be upserted to db.
    Postgres specific because it's upsert.

    :param results: Instance which is to be upserted
    :type results: noiz.models.beamforming.BeamformingResult
    :return: Postgres-specific upsert command
    :rtype: sqlalchemy.dialects.postgresql.Insert
    """
    insert_command = (
        insert(BeamformingResult)
        .values(
            beamforming_params_id=results.beamforming_params_id,
            timespan_id=results.timespan_id,
            beamforming_file_id=results.beamforming_file_id,
            used_component_count=results.used_component_count,
        )
        .on_conflict_do_update(
            constraint="unique_beam_per_config_per_timespan",
            set_=dict(
                beamforming_file_id=results.beamforming_file_id,
                used_component_count=results.used_component_count,
            ),
        )
    )
    return insert_command


def fetch_beamforming_peaks_avg_abspower_results_in_freq_slowness(
        beamforming_params_collection: Optional[Collection[BeamformingParams]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        minimum_trace_used_count: Optional[int] = None,
        beamforming_result_type: Union[str, BeamformingResultType] = BeamformingResultType.AVGABSPOWER,
) -> pd.DataFrame:
    """filldocs"""
    filters = _determine_filters_for_fetching_beamforming_peaks(
        beamforming_params_collection=beamforming_params_collection,
        timespans=timespans,
        minimum_trace_used_count=minimum_trace_used_count,
    )

    try:
        beamforming_result_type = BeamformingResultType(beamforming_result_type)
    except ValueError:
        raise ValueError(f"Invalid value of beamforming_result_type. "
                         f"Accepted values: {list(BeamformingResultType)}; "
                         f"Got value: {beamforming_result_type} ")

    query = _query_one_of_the_beamforming_result_types_for_freq_slowness(beamforming_result_type, filters)

    df = _parse_query_as_dataframe(query)
    return df


def _query_one_of_the_beamforming_result_types_for_freq_slowness(
        beamforming_result_type: BeamformingResultType,
        filters: Union[List[BinaryExpression], List[bool]],
) -> Query:
    if beamforming_result_type is BeamformingResultType.AVGABSPOWER:
        query = _query_beamforming_peaks_avg_abspower(filters)
    elif beamforming_result_type is BeamformingResultType.AVGRELPOWER:
        query = _query_beamforming_peaks_avg_relpower(filters)
    elif beamforming_result_type is BeamformingResultType.ALLABSPOWER:
        query = _query_beamforming_peaks_all_abspower(filters)
    elif beamforming_result_type is BeamformingResultType.ALLRELPOWER:
        query = _query_beamforming_peaks_all_relpower(filters)
    else:
        raise NotImplementedError(f'This value is not supported at all. Supported values {list(BeamformingResultType)}')
    return query


def _query_beamforming_peaks_avg_abspower(filters: Union[List[BinaryExpression], List[bool]]) -> Query:
    """filldocs"""
    query = (
        db.session
        .query(BeamformingParams.central_freq, BeamformingPeakAverageAbspower.slowness,
               BeamformingPeakAverageAbspower.slowness_x, BeamformingPeakAverageAbspower.slowness_y,
               BeamformingPeakAverageAbspower.amplitude, BeamformingPeakAverageAbspower.backazimuth, BeamformingPeakAverageAbspower.azimuth)
        .select_from(BeamformingResult)
        .join(BeamformingParams, BeamformingParams.id == BeamformingResult.beamforming_params_id)
        .join(association_table_beamforming_result_avg_abspower)
        .join(BeamformingPeakAverageAbspower)
        .filter(*filters)
    )
    return query


def _query_beamforming_peaks_all_abspower(filters: Union[List[BinaryExpression], List[bool]]) -> Query:
    """filldocs"""
    query = (
        db.session
        .query(BeamformingParams.central_freq, BeamformingPeakAllAbspower.slowness,
               BeamformingPeakAllAbspower.slowness_x, BeamformingPeakAllAbspower.slowness_y,
               BeamformingPeakAllAbspower.amplitude, BeamformingPeakAllAbspower.backazimuth, BeamformingPeakAllAbspower.azimuth)
        .select_from(BeamformingResult)
        .join(BeamformingParams, BeamformingParams.id == BeamformingResult.beamforming_params_id)
        .join(association_table_beamforming_result_all_abspower)
        .join(BeamformingPeakAllAbspower)
        .filter(*filters)
    )
    return query


def _query_beamforming_peaks_all_relpower(filters: Union[List[BinaryExpression], List[bool]]) -> Query:
    """filldocs"""
    query = (
        db.session
        .query(BeamformingParams.central_freq, BeamformingPeakAllRelpower.slowness,
               BeamformingPeakAllRelpower.slowness_x, BeamformingPeakAllRelpower.slowness_y,
               BeamformingPeakAllRelpower.amplitude, BeamformingPeakAllRelpower.backazimuth, BeamformingPeakAllRelpower.azimuth)
        .select_from(BeamformingResult)
        .join(BeamformingParams, BeamformingParams.id == BeamformingResult.beamforming_params_id)
        .join(association_table_beamforming_result_all_relpower)
        .join(BeamformingPeakAllRelpower)
        .filter(*filters)
    )
    return query


def _query_beamforming_peaks_avg_relpower(filters: Union[List[BinaryExpression], List[bool]]) -> Query:
    """filldocs"""
    query = (
        db.session
        .query(BeamformingParams.central_freq, BeamformingPeakAverageRelpower.slowness,
               BeamformingPeakAverageRelpower.slowness_x, BeamformingPeakAverageRelpower.slowness_y,
               BeamformingPeakAverageRelpower.amplitude, BeamformingPeakAverageRelpower.backazimuth, BeamformingPeakAverageRelpower.azimuth)
        .select_from(BeamformingResult)
        .join(BeamformingParams, BeamformingParams.id == BeamformingResult.beamforming_params_id)
        .join(association_table_beamforming_result_avg_relpower)
        .join(BeamformingPeakAverageRelpower)
        .filter(*filters)
    )
    return query


def _determine_filters_for_fetching_beamforming_peaks(
        beamforming_params_collection: Optional[Collection[BeamformingParams]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        minimum_trace_used_count: Optional[int] = None,
) -> Union[List[BinaryExpression], List[bool]]:
    """filldocs"""
    filters = []
    if beamforming_params_collection is not None:
        params_ids = extract_object_ids(beamforming_params_collection)
        filters.append(BeamformingResult.beamforming_params_id.in_(params_ids))
    if timespans is not None:
        timespan_ids = extract_object_ids(timespans)
        filters.append(BeamformingResult.timespan_id.in_(timespan_ids))
    if minimum_trace_used_count is not None:
        filters.append(BeamformingResult.used_component_count >= minimum_trace_used_count)
    if len(filters) == 0:
        filters.append(True)
    return filters


def fetch_beamforming_results(
        beamforming_params: Optional[BeamformingParams] = None,
        beamforming_params_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_beamforming_params: bool = False,
) -> List[BeamformingParams]:
    """filldocs"""
    query = _query_beamforming_results(
        beamforming_params=beamforming_params,
        beamforming_params_id=beamforming_params_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
        load_timespan=load_timespan,
        load_datachunk=load_datachunk,
        load_beamforming_params=load_beamforming_params,
    )

    return query.all()


def _query_beamforming_results(
        beamforming_params: Optional[BeamformingParams] = None,
        beamforming_params_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_beamforming_params: bool = False,
) -> Query:
    """filldocs"""
    try:
        validate_maximum_one_argument_provided(beamforming_params, beamforming_params_id)
    except ValueError:
        raise ValueError('Maximum one of beamforming_params or beamforming_params_id can be provided')
    try:
        validate_maximum_one_argument_provided(datachunks, datachunk_ids)
    except ValueError:
        raise ValueError('Maximum one of datachunks or datachunk_ids can be provided')

    filters, opts = _determine_filters_and_opts_for_beamforming_results(
        beamforming_params=beamforming_params,
        beamforming_params_id=beamforming_params_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
        load_timespan=load_timespan,
        load_datachunk=load_datachunk,
        load_beamforming_params=load_beamforming_params,
    )

    query = BeamformingResult.query.filter(*filters).options(opts)

    return query


def _determine_filters_and_opts_for_beamforming_results(
        beamforming_params: Optional[BeamformingParams] = None,
        beamforming_params_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_beamforming_params: bool = False,
) -> Tuple[List, List]:

    filters = []
    if beamforming_params is not None:
        filters.append(BeamformingResult.beamforming_params_id.in_((beamforming_params.id,)))
    if beamforming_params_id is not None:
        beamforming_params_ids = validate_to_tuple(val=beamforming_params_id, accepted_type=int)
        filters.append(BeamformingResult.beamforming_params_id.in_(beamforming_params_ids))
    if datachunks is not None:
        extracted_datachunk_ids = extract_object_ids(datachunks)
        filters.append(BeamformingResult.datachunk_ids.in_(extracted_datachunk_ids))
    if datachunk_ids is not None:
        filters.append(BeamformingResult.datachunk_ids.in_(datachunk_ids))
    if len(filters) == 0:
        filters.append(True)
    opts = []
    if load_timespan:
        opts.append(subqueryload(BeamformingResult.timespan))
    if load_datachunk:
        opts.append(subqueryload(BeamformingResult.datachunks))
    if load_beamforming_params:
        opts.append(subqueryload(BeamformingResult.beamforming_params))
    return filters, opts


def fetch_beamforming_peaks_all_abspower_results_in_freq_slowness(
        beamforming_params_collection: Optional[Collection[BeamformingParams]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        minimum_trace_used_count: Optional[int] = None,
        beamforming_result_type: Union[str, BeamformingResultType] = BeamformingResultType.ALLABSPOWER,
) -> pd.DataFrame:
    """filldocs"""
    filters = _determine_filters_for_fetching_beamforming_peaks(
        beamforming_params_collection=beamforming_params_collection,
        timespans=timespans,
        minimum_trace_used_count=minimum_trace_used_count,
    )

    try:
        beamforming_result_type = BeamformingResultType(beamforming_result_type)
    except ValueError:
        raise ValueError(f"Invalid value of beamforming_result_type. "
                         f"Accepted values: {list(BeamformingResultType)}; "
                         f"Got value: {beamforming_result_type} ")

    query = _query_one_of_the_beamforming_result_types_for_freq_slowness(beamforming_result_type, filters)

    df = _parse_query_as_dataframe(query)
    return df


def fetch_beamforming_peaks_all_relpower_results_in_freq_slowness(
        beamforming_params_collection: Optional[Collection[BeamformingParams]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        minimum_trace_used_count: Optional[int] = None,
        beamforming_result_type: Union[str, BeamformingResultType] = BeamformingResultType.ALLRELPOWER,
) -> pd.DataFrame:
    """filldocs"""
    filters = _determine_filters_for_fetching_beamforming_peaks(
        beamforming_params_collection=beamforming_params_collection,
        timespans=timespans,
        minimum_trace_used_count=minimum_trace_used_count,
    )

    try:
        beamforming_result_type = BeamformingResultType(beamforming_result_type)
    except ValueError:
        raise ValueError(f"Invalid value of beamforming_result_type. "
                         f"Accepted values: {list(BeamformingResultType)}; "
                         f"Got value: {beamforming_result_type} ")

    query = _query_one_of_the_beamforming_result_types_for_freq_slowness(beamforming_result_type, filters)

    df = _parse_query_as_dataframe(query)
    return df


def fetch_beamforming_peaks_avg_relpower_results_in_freq_slowness(
        beamforming_params_collection: Optional[Collection[BeamformingParams]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        minimum_trace_used_count: Optional[int] = None,
        beamforming_result_type: Union[str, BeamformingResultType] = BeamformingResultType.AVGRELPOWER,
) -> pd.DataFrame:
    """filldocs"""
    filters = _determine_filters_for_fetching_beamforming_peaks(
        beamforming_params_collection=beamforming_params_collection,
        timespans=timespans,
        minimum_trace_used_count=minimum_trace_used_count,
    )

    try:
        beamforming_result_type = BeamformingResultType(beamforming_result_type)
    except ValueError:
        raise ValueError(f"Invalid value of beamforming_result_type. "
                         f"Accepted values: {list(BeamformingResultType)}; "
                         f"Got value: {beamforming_result_type} ")

    query = _query_one_of_the_beamforming_result_types_for_freq_slowness(beamforming_result_type, filters)
    df = _parse_query_as_dataframe(query)
    return df


def fetch_beamforming_file_by_timespan_and_beamforming_param(
        beamforming_params: Optional[BeamformingParams] = None,
        beamforming_params_id: Optional[int] = None,
        timespans: Optional[Timespan] = None,
        timespan_ids: Optional[int] = None,
) -> pd.DataFrame:
    """Fetching the beamforming filepath filtered by time period (timespans) and by beamforming parameters.
    Allow to win a lot of computing time compared to fetching_beamforming_results as just filepaths are loaded

    :param beamforming_params: beamforming parameters, defaults to None
    :type beamforming_params: Optional[BeamformingParams], optional
    :param beamforming_params_id: id of beamforming parameters, defaults to None
    :type beamforming_params_id: Optional[int], optional
    :param timespans: timespans, defaults to None
    :type timespans: Optional[Timespan], optional
    :param timespan_ids: ids of timespans, defaults to None
    :type timespan_ids: Optional[int], optional
    :return: a dataframe containing beaforming filepaths
    :rtype: pd.DataFrame
    """

    query = _query_beamforming_file_id_by_timespan_by_beamforming_param(
        beamforming_params=beamforming_params,
        beamforming_params_id=beamforming_params_id,
        timespans=timespans,
        timespan_ids=timespan_ids,
    )

    df_beam_file_id = _parse_query_as_dataframe(query)
    df_beam_file_id = df_beam_file_id.rename(columns={"beamforming_file_id": "id"})
    df_beam_file_id = df_beam_file_id.drop("anon_1", axis=1)

    query_beam_file_all = query = (
            db.session
            .query(BeamformingFile)
            .select_from(BeamformingFile)
    )

    df_beam_file_all = _parse_query_as_dataframe(query_beam_file_all)

    df = df_beam_file_id.merge(df_beam_file_all, on='id')

    return df


def _query_beamforming_file_id_by_timespan_by_beamforming_param(
        beamforming_params: Optional[BeamformingParams] = None,
        beamforming_params_id: Optional[int] = None,
        timespans: Optional[Timespan] = None,
        timespan_ids: Optional[int] = None,
        ) -> Query:
    """Creating the filters for selecting beamforming and then doing the query

    :param beamforming_params: beamforming parameters, defaults to None
    :type beamforming_params: Optional[BeamformingParams], optional
    :param beamforming_params_id: id of beamforming parameters, defaults to None
    :type beamforming_params_id: Optional[int], optional
    :param timespans: timespans, defaults to None
    :type timespans: Optional[Timespan], optional
    :param timespan_ids: ids of timespans, defaults to None
    :type timespan_ids: Optional[int], optional
    :return:
    :rtype: Query
    """

    filters = []
    if beamforming_params is not None:
        params_ids = extract_object_ids(beamforming_params)
        filters.append(BeamformingResult.beamforming_params_id.in_(params_ids))
    if beamforming_params_id is not None:
        filters.append(BeamformingResult.beamforming_params_id.in_(beamforming_params_id))
    if timespans is not None:
        timespan_idss = extract_object_ids(timespans)
        filters.append(BeamformingResult.timespan_id.in_(timespan_idss))
    if timespan_ids is not None:
        filters.append(BeamformingResult.timespan_id.in_(timespan_ids))

    query = (
            db.session
            .query(BeamformingResult.beamforming_file_id, BeamformingResult.file)
            .select_from(BeamformingResult)
            .join(BeamformingParams, BeamformingParams.id == BeamformingResult.beamforming_params_id)
            .join(BeamformingFile, BeamformingFile.id == BeamformingResult.beamforming_file_id)
            .filter(*filters)
    )

    return query

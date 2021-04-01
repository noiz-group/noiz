from collections import defaultdict
import datetime
from loguru import logger
import pandas as pd
from sqlalchemy.dialects.postgresql import insert, Insert
from sqlalchemy.orm import subqueryload, Query
from sqlalchemy.sql.elements import BinaryExpression
from typing import Union, Collection, Optional, List, Tuple, Generator


from noiz.api.component import fetch_components
from noiz.api.helpers import extract_object_ids, _run_calculate_and_upsert_sequentially, \
    _run_calculate_and_upsert_on_dask
from noiz.api.qc import fetch_qcone_config_single
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.models.type_aliases import BeamformingRunnerInputs
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import Timespan, Datachunk, QCOneResults
from noiz.models.beamforming import BeamformingResult, BeamformingPeakAverageAbspower, \
    association_table_beamforming_result_avg_abspower
from noiz.models.processing_params import BeamformingParams
from noiz.processing.beamforming import calculate_beamforming_results_wrapper, \
    _validate_if_all_beamforming_params_use_same_component_codes, _validate_if_all_beamforming_params_use_same_qcone
from noiz.validation_helpers import validate_to_tuple


def fetch_beamforming_params_single(id: int) -> BeamformingParams:
    """
    Fetches a BeamformingParams objects by its ID.

    :param id: ID of beamforming params to be fetched
    :type id: int
    :return: fetched BeamformingParams object
    :rtype: BeamformingParams
    """
    fetched_params = BeamformingParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"BeamformingParams object of id {id} does not exist.")

    return fetched_params


def fetch_beamforming_params(
        ids: Collection[int],
        load_qcone_config: bool = True,
) -> List[BeamformingParams]:
    """
    Fetches a BeamformingParams objects by its ID.

    :param id: ID of beamforming params to be fetched
    :type id: Collection[int]
    :return: List of fetched BeamformingParams objects
    :rtype: List[BeamformingParams]
    """
    opts = []
    if load_qcone_config:
        opts.append(subqueryload(BeamformingParams.qcone_config))

    fetched_params = (BeamformingParams.query
                      .filter(BeamformingParams.id.in_(ids))
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
        batch_size: int = 2500,
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
        skip_existing=skip_existing
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
) -> Generator[BeamformingRunnerInputs, None, None]:

    logger.debug(f"Fetching BeamformingParams with ids {beamforming_params_ids}")
    params = fetch_beamforming_params(ids=beamforming_params_ids)
    logger.debug(f"Fetching BeamformingParams successful. {params}")

    single_qcone_config_id = _validate_if_all_beamforming_params_use_same_qcone(params)
    single_used_component_codes = _validate_if_all_beamforming_params_use_same_component_codes(params)
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

    selection = (
        db.session.query(Timespan, Datachunk, QCOneResults)
                  .select_from(Timespan)
                  .join(Datachunk, Datachunk.timespan_id == Timespan.id)
                  .join(QCOneResults, Datachunk.id == QCOneResults.datachunk_id)
                  .filter(Timespan.id.in_(extract_object_ids(fetched_timespans)))  # type: ignore
                  .filter(Datachunk.component_id.in_(extract_object_ids(fetched_components)))
                  .filter(QCOneResults.qcone_config_id == qcone_config.id)
                  .options(subqueryload(Datachunk.component))
                  .all()
    )

    grouped_by_tid = defaultdict(list)

    for sel in selection:
        grouped_by_tid[sel[0]].append(sel[1:])

    if skip_existing:
        raise NotImplementedError("Not yet implemented")

    for ts, group in grouped_by_tid.items():
        group: List[Tuple[Datachunk, QCOneResults]]  # type: ignore
        passing_chunks = [chunk for chunk, qcres in group if qcres.is_passing()]

        if len(passing_chunks) < global_minimum_trace_count:
            logger.warning(f"There was not enough traces passing QCOne for the beamforming. Skipping this timespan. "
                           f"Timespan: {ts} "
                           f"Global minimum trace count: {global_minimum_trace_count}. "
                           f"Passing traces: {len(passing_chunks)}")
            continue

        db.session.expunge_all()
        yield BeamformingRunnerInputs(
            beamforming_params=params,
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


def fetch_beamforming_peak_average_results_in_freq_slowness(
        beamforming_params_collection: Optional[Collection[BeamformingParams]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        minimum_trace_used_count: Optional[int] = None,
) -> pd.DataFrame:
    """filldocs"""
    filters = _determine_filters_for_fetching_beamforming_peaks(
        beamforming_params_collection=beamforming_params_collection,
        timespans=timespans,
        minimum_trace_used_count=minimum_trace_used_count,
    )
    query = _query_beamforming_peaks_avg_abspower(filters)
    df = _parse_query_as_dataframe(query)
    return df


def _parse_query_as_dataframe(query: Query) -> pd.DataFrame:
    """filldocs"""
    c = query.statement.compile(query.session.bind)
    df = pd.read_sql(c.string, query.session.bind, params=c.params)
    return df


def _query_beamforming_peaks_avg_abspower(filters: Union[List[BinaryExpression], List[bool]]) -> Query:
    """filldocs"""
    query = (
        db.session
        .query(BeamformingParams.central_freq, BeamformingPeakAverageAbspower.slowness)
        .select_from(BeamformingResult)
        .join(BeamformingParams, BeamformingParams.id == BeamformingResult.beamforming_params_id)
        .join(association_table_beamforming_result_avg_abspower)
        .join(BeamformingPeakAverageAbspower)
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

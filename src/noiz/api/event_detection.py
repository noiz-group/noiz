import datetime
import re
from loguru import logger
from sqlalchemy import func, Table, Column
from sqlalchemy.orm import Query, subqueryload
from sqlalchemy.dialects.postgresql import Insert, insert
from typing import Union, Optional, Collection, Generator, List, Tuple

from noiz.database import db
from noiz.api.component import fetch_components
from noiz.api.datachunk import _query_datachunks
from noiz.api.helpers import extract_object_ids, _run_calculate_and_upsert_on_dask, \
    _run_calculate_and_upsert_sequentially
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.api.database_helpers import _get_maximum_value_of_column_incremented
from noiz.models.component import Component
from noiz.models.timespan import Timespan
from noiz.models import Datachunk, EventDetectionParams, EventConfirmationParams, EventDetectionResult, EventConfirmationResult, \
    EventConfirmationRun
from noiz.models.type_aliases import EventDetectionRunnerInputs, EventConfirmationRunnerInputs
from noiz.validation_helpers import validate_maximum_one_argument_provided, validate_to_tuple, \
    validate_timestamp_as_pydatetime
from noiz.exceptions import EmptyResultException
from noiz.processing.event_detection import calculate_event_detection_wrapper, calculate_event_confirmation_wrapper, \
    _parse_str_collection_as_dict


def fetch_event_detection_params_by_id(params_id: int) -> EventDetectionParams:
    """
    Fetches a single EventDetectionParams objects by its ID.

    :param params_id: ID of EventDetectionParams to be fetched
    :type params_id: int
    :return: fetched EventDetectionParams object
    :rtype: EventDetectionParams
    """
    fetched_params = EventDetectionParams.query.filter_by(id=params_id).first()
    if fetched_params is None:
        raise EmptyResultException(f"EventDetectionParams object of id {params_id} does not exist.")
    return fetched_params


def fetch_event_detection_params(
        ids: Optional[Collection[int]] = None,
) -> List[EventDetectionParams]:
    """
    Fetches a EventDetectionParams objects by its ID.

    :param ids: ID of EventDetectionParams params to be fetched
    :type ids: Collection[int]
    :return: List of fetched EventDetectionParams objects
    :rtype: List[EventDetectionParams]
    """
    filters = []
    if ids is not None:
        filters.append(EventDetectionParams.id.in_(ids))
    if len(filters) == 0:
        filters.append(True)

    fetched_params = (EventDetectionParams.query
                      .filter(*filters)
                      .all())
    if len(fetched_params) == 0:
        raise EmptyResultException(f"EventDetectionParams with ids of {ids} do not exist.")

    return fetched_params


def fetch_event_confirmation_params_by_id(params_id: int) -> EventConfirmationParams:
    """
    Fetches a single EventConfirmationParams objects by its ID.

    :param params_id: ID of EventConfirmationParams to be fetched
    :type params_id: int
    :return: fetched EventConfirmationParams object
    :rtype: EventConfirmationParams
    """
    fetched_params = EventConfirmationParams.query.filter_by(id=params_id).first()
    if fetched_params is None:
        raise EmptyResultException(f"EventConfirmationParams object of id {params_id} does not exist.")
    return fetched_params


def fetch_event_detection_results(
        event_detection_params: Optional[EventDetectionParams] = None,
        event_detection_params_id: Optional[int] = None,
        event_detection_run_id : Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_event_detection_params: bool = False,
) -> List[EventDetectionResult]:
    """
    Fetches EventDetectionResult objects using varying optional filters. Associated Timespan, \
    Datachunk and EventDetectionParams can also be loaded for direct usage without context.

    :param event_detection_params: a EventDetectionParams
    :type event_detection_params: EventDetectionParams
    :param event_detection_params_id: ID of a EventDetectionParams
    :type params_id: int
    :param event_detection_run_id: a collection of event_detection_run_id
    :type ids: Collection[int]
    :param timespan_id: a collection of timespan_id
    :type timespan_id: Collection[int]
    :param datachunks: a collection of Datachunk object
    :type datachunk: Collection[Datachunk]
    :param datachunk_ids: a collection of datachunk_id
    :type ids: Collection[int]
    :param load_timespan: Loads also the associated Timespan object so it is available for usage \
        without context
    :type load_timespan: bool
    :param load_datachunk: Loads also the associated Datachunk object so it is available for usage \
        without context
    :type load_datachunk: bool
    :param load_event_detection_params: Loads also the associated EventDetectionParams object so it is available for usage \
        without context
    :type load_event_detection_params: bool
    :return: fetched EventDetectionResult objects
    :rtype: EventDetectionResult
    """
    query = _query_event_detection_results(
        event_detection_params=event_detection_params,
        event_detection_params_id=event_detection_params_id,
        event_detection_run_id=event_detection_run_id,
        timespan_id=timespan_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
        load_timespan=load_timespan,
        load_datachunk=load_datachunk,
        load_event_detection_params=load_event_detection_params,
    )

    return query.all()


def count_event_detection_results(
        event_detection_params: Optional[EventDetectionParams] = None,
        event_detection_params_id: Optional[int] = None,
        event_detection_run_id : Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
) -> int:
    """filldocs"""
    query = _query_event_detection_results(
        event_detection_params=event_detection_params,
        event_detection_params_id=event_detection_params_id,
        event_detection_run_id=event_detection_run_id,
        timespan_id=timespan_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
    )

    return query.count()


def _query_event_detection_results(
        event_detection_params: Optional[EventDetectionParams] = None,
        event_detection_params_id: Optional[int] = None,
        event_detection_run_id : Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_event_detection_params: bool = False,
) -> Query:
    """filldocs"""
    try:
        validate_maximum_one_argument_provided(event_detection_params, event_detection_params_id)
    except ValueError:
        raise ValueError('Maximum one of event_detection_params or event_detection_params_id can be provided')
    try:
        validate_maximum_one_argument_provided(datachunks, datachunk_ids)
    except ValueError:
        raise ValueError('Maximum one of datachunks or datachunk_ids can be provided')

    filters, opts = _determine_filters_and_opts_for_event_detection(
        event_detection_params=event_detection_params,
        event_detection_params_id=event_detection_params_id,
        event_detection_run_id=event_detection_run_id,
        timespan_id=timespan_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
        load_timespan=load_timespan,
        load_datachunk=load_datachunk,
        load_event_detection_params=load_event_detection_params,
    )

    query = EventDetectionResult.query.filter(*filters).options(opts)

    return query


def _determine_filters_and_opts_for_event_detection(
        event_detection_params: Optional[EventDetectionParams] = None,
        event_detection_params_id: Optional[int] = None,
        event_detection_run_id: Optional[Collection[int]] = None,
        timespan_id: Optional[Collection[int]] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_event_detection_params: bool = False,
) -> Tuple[List, List]:

    filters = []
    if event_detection_params is not None:
        filters.append(EventDetectionResult.event_detection_params_id.in_((event_detection_params.id,)))
    if event_detection_params_id is not None:
        event_detection_params_ids = validate_to_tuple(val=event_detection_params_id, accepted_type=int)
        filters.append(EventDetectionResult.event_detection_params_id.in_(event_detection_params_ids))
    if event_detection_run_id is not None:
        event_detection_run_ids = validate_to_tuple(val=event_detection_run_id, accepted_type=int)
        filters.append(EventDetectionResult.event_detection_run_id.in_((event_detection_run_ids,)))
    if timespan_id is not None:
        filters.append(EventDetectionResult.timespan_id.in_(timespan_id))
    if datachunks is not None:
        extracted_datachunk_ids = extract_object_ids(datachunks)
        filters.append(EventDetectionResult.datachunk_id.in_(extracted_datachunk_ids))
    if datachunk_ids is not None:
        filters.append(EventDetectionResult.datachunk_id.in_(datachunk_ids))
    if len(filters) == 0:
        filters.append(True)
    opts = []
    if load_timespan:
        opts.append(subqueryload(EventDetectionResult.timespan))
    if load_datachunk:
        opts.append(subqueryload(EventDetectionResult.datachunk))
    if load_event_detection_params:
        opts.append(subqueryload(EventDetectionResult.event_detection_params))
    return filters, opts


def fetch_event_confirmation_results(
        event_confirmation_params: Optional[EventConfirmationParams] = None,
        event_confirmation_params_id: Optional[int] = None,
        event_confirmation_run_id : Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_event_confirmation_params: bool = False,
        batch_size: Optional[int] = None,
) -> List[EventConfirmationResult]:
    """
    Fetches EventConfirmationResult objects using varying optional filters. Associated Timespan, \
    and EventConfirmationParams can also be loaded for direct usage without context.

    :param event_confirmation_params: an EventConfirmationParams
    :type event_confirmation_params: EventConfirmationParams
    :param event_confirmation_params_id: ID of an EventConfirmationParams
    :type params_id: int
    :param event_confirmation_run_id: a collection of event_confirmation_run_id
    :type ids: Collection[int]
    :param load_timespan: Loads also the associated Timespan object so it is available for usage \
        without context
    :type load_timespan: bool
    :param load_event_confirmation_params: Loads also the associated EventConfirmationParams object \
        so it is available for usage without context
    :type load_event_confirmation_params: bool
    :param batch_size: Returns the list[EventConfirmationResult] by batch instead of all at once.
    :type batch_size: int
    :return: fetched EventConfirmationResult objects
    :rtype: EventConfirmationResult
    """
    query = _query_event_confirmation_results(
        event_confirmation_params=event_confirmation_params,
        event_confirmation_params_id=event_confirmation_params_id,
        event_confirmation_run_id=event_confirmation_run_id,
        load_timespan=load_timespan,
        load_event_confirmation_params=load_event_confirmation_params,
    )

    if batch_size is not None:
        return query.yield_per(batch_size).enable_eagerloads(False)  # type: ignore

    return query.all()


def count_event_confirmation_results(
        event_confirmation_params: Optional[EventConfirmationParams] = None,
        event_confirmation_params_id: Optional[int] = None,
        event_confirmation_run_id : Optional[Collection[int]] = None,
) -> int:
    """filldocs"""
    query = _query_event_confirmation_results(
        event_confirmation_params=event_confirmation_params,
        event_confirmation_params_id=event_confirmation_params_id,
        event_confirmation_run_id=event_confirmation_run_id,
    )

    return query.count()


def _query_event_confirmation_results(
        event_confirmation_params: Optional[EventConfirmationParams] = None,
        event_confirmation_params_id: Optional[int] = None,
        event_confirmation_run_id : Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_event_confirmation_params: bool = False,
) -> Query:
    """filldocs"""
    try:
        validate_maximum_one_argument_provided(event_confirmation_params, event_confirmation_params_id)
    except ValueError:
        raise ValueError('Maximum one of event_confirmation_params or event_confirmation_params_id can be provided')

    filters, opts = _determine_filters_and_opts_for_event_confirmation(
        event_confirmation_params=event_confirmation_params,
        event_confirmation_params_id=event_confirmation_params_id,
        event_confirmation_run_id=event_confirmation_run_id,
        load_timespan=load_timespan,
        load_event_confirmation_params=load_event_confirmation_params,
    )

    query = EventConfirmationResult.query.filter(*filters).options(opts)

    return query


def _determine_filters_and_opts_for_event_confirmation(
        event_confirmation_params: Optional[EventConfirmationParams] = None,
        event_confirmation_params_id: Optional[int] = None,
        event_confirmation_run_id : Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_event_confirmation_params: bool = False,
) -> Tuple[List, List]:

    filters = []
    if event_confirmation_params is not None:
        filters.append(EventConfirmationResult.event_confirmation_params_id.in_((event_confirmation_params.id,)))
    if event_confirmation_params_id is not None:
        event_confirmation_params_ids = validate_to_tuple(val=event_confirmation_params_id, accepted_type=int)
        filters.append(EventConfirmationResult.event_confirmation_params_id.in_(event_confirmation_params_ids))
    if event_confirmation_run_id is not None:
        event_confirmation_run_ids = validate_to_tuple(val=event_confirmation_run_id, accepted_type=int)
        filters.append(EventConfirmationResult.event_confirmation_run_id.in_((event_confirmation_run_ids,)))
    if len(filters) == 0:
        filters.append(True)
    opts = []
    if load_timespan:
        opts.append(subqueryload(EventConfirmationResult.timespan))
    if load_event_confirmation_params:
        opts.append(subqueryload(EventConfirmationResult.event_confirmation_params))
    return filters, opts


def fetch_latest_event_confirmation_run() -> EventConfirmationRun:
    """
    Fetches the last EventConfirmationRun added to the db. This function
    uses EventConfirmationRun.id to return the latest.

    :return: fetched EventConfirmationRun object
    :rtype: EventConfirmationRun
    """
    latest_event_confirmation_run = EventConfirmationRun.query.order_by(EventConfirmationRun.id.desc()).first()

    return latest_event_confirmation_run


def perform_event_detection(
        event_detection_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        plot_figures: bool = True,
        batch_size: int = 2000,
        parallel: bool = True,
        skip_existing: bool = True,
        raise_errors: bool = False,
):
    """
    Performs event detection according to provided set of selectors.
    Uses Dask.distributed for parallelism.
    All the calculations are divided into batches in order to speed up queries that gather all the inputs.

    :param event_detection_params_id: ID of EventDetectionParams object to be used as config
    :type event_detection_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param networks: Selector for Networks
    :type networks: Optional[Collection[str]]
    :param stations: Selector stations
    :type stations: Optional[Collection[str]]
    :param components: Selector components
    :type components: Optional[Collection[str]]
    :param component_ids: IDs of Selector components
    :type component_ids: Optional[Collection[int]]
    :param plot_figures: If figures should be saved alonside the miniseed trace.
    :type plot_figures: bool
    :param raise_errors: If errors should be raised or just logged
    :type raise_errors: bool
    :param skip_existing: If pre-existing EventDetectionResult should be skipped.
    :type skip_existing: bool
    :param batch_size: How big should be the batch of calculations
    :type batch_size: int
    :param parallel: If the calculations should be done in parallel
    :type parallel: bool
    :return: None
    :rtype: NoneType
    """

    calculation_inputs = _prepare_inputs_for_event_detection(
        event_detection_params_id=event_detection_params_id,
        starttime=starttime,
        endtime=endtime,
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
        plot_figures=plot_figures,
        skip_existing=skip_existing,
        batch_size=batch_size,
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_event_detection_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_event_detection,
            with_file=True,
            raise_errors=raise_errors,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_event_detection_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_event_detection,
            with_file=True,
            raise_errors=raise_errors,
        )

    return


def _prepare_inputs_for_event_detection(
        event_detection_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        plot_figures: bool = True,
        batch_size: int = 2000,
        skip_existing: bool = True,
) -> Generator[EventDetectionRunnerInputs, None, None]:
    """
    Parses the given selectors and retrieves the corresponding datachunks.
    For each of these datachunks, a EventDetectionRunnerInputs is generated and
    yielded.

    :param event_detection_params_id: ID of EventDetectionParams object to be used as config
    :type event_detection_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param networks: Selector for Networks
    :type networks: Optional[Collection[str]]
    :param stations: Selector stations
    :type stations: Optional[Collection[str]]
    :param components: Selector components
    :type components: Optional[Collection[str]]
    :param component_ids: IDs of Selector components
    :type component_ids: Optional[Collection[int]]
    :param plot_figures: If figures should be saved alonside the miniseed trace.
    :type plot_figures: bool
    :param skip_existing: If datachunks should be skipped, if results already exist with the same parameters.
    :type skip_existing: bool
    :param batch_size: How big should be the batch of calculations
    :type batch_size: int
    :return: EventDetectionRunnerInputs
    :rtype: Generator[EventDetectionRunnerInputs, None, None]
    """

    params = fetch_event_detection_params_by_id(params_id=event_detection_params_id)

    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)

    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )

    event_detection_run_id = _get_maximum_value_of_column_incremented(EventDetectionResult.event_detection_run_id)

    i = 0

    while True:
        logger.info(f"Querying batch no. {i} of datachunks")
        query = _query_datachunks(
            datachunk_params_id=params.datachunk_params_id,
            timespans=timespans,
            components=fetched_components,
            load_timespan=True,
            load_component=True,
        )

        query = query.limit(batch_size).offset(i * batch_size)

        fetched_datachunks = query.all()

        if len(fetched_datachunks) == 0:
            logger.info("This batch did not contain any elements")
            break

        fetched_datachunk_ids = extract_object_ids(fetched_datachunks)
        logger.debug(f"There were {len(fetched_datachunk_ids)} in the batch")

        if skip_existing:
            logger.debug("Fetching existing EventDetectionResult")
            existing_results = fetch_event_detection_results(event_detection_params=params, datachunk_ids=fetched_datachunk_ids)
            existing_results_datachunk_ids = [x.datachunk_id for x in existing_results]
        else:
            existing_results_datachunk_ids = list()

        for datachunk in fetched_datachunks:
            if datachunk.id in existing_results_datachunk_ids:
                logger.debug(f"There already exists EventDetectionResult for datachunk {datachunk}")
                continue

            db.session.expunge_all()
            yield EventDetectionRunnerInputs(
                event_detection_params=params,
                timespan=datachunk.timespan,
                datachunk=datachunk,
                component=datachunk.component,
                event_detection_run_id=event_detection_run_id,
                plot_figures=plot_figures,
            )

        i += 1


def _prepare_upsert_command_event_detection(event_detection_result: EventDetectionResult) -> Insert:
    """
    Private method that generates an :py:class:`~sqlalchemy.dialects.postgresql.Insert` for
    :py:class:`~noiz.models.event_detection.EventDetectionResult` to be upserted to db.
    Postgres specific because it's upsert.

    :param results: Instance which is to be upserted
    :type results: noiz.models.event_detection.EventDetectionResult
    :return: Postgres-specific upsert command
    :rtype: sqlalchemy.dialects.postgresql.Insert
    """

    insert_command = (
        insert(EventDetectionResult)
        .values(
            event_detection_params_id=event_detection_result.event_detection_params_id,
            datachunk_id=event_detection_result.datachunk_id,
            timespan_id=event_detection_result.timespan_id,
            event_detection_run_id=event_detection_result.event_detection_run_id,
            time_start=event_detection_result.time_start,
            time_stop=event_detection_result.time_stop,
            peak_ground_velocity=event_detection_result.peak_ground_velocity,
            minimum_frequency=event_detection_result.minimum_frequency,
            maximum_frequency=event_detection_result.maximum_frequency,
            event_detection_file_id=event_detection_result.event_detection_file_id,
        )
        .on_conflict_do_update(
            constraint="unique_detection_per_timespan_per_datachunk_per_param_per_time",
            set_=dict(event_detection_file_id=event_detection_result.event_detection_file_id),
        )
    )
    return insert_command


def perform_event_confirmation(
        event_confirmation_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        exclude_stations: Optional[Union[Collection[str], str]] = None,
        specific_stations_params: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        batch_size: int = 50,
        parallel: bool = True,
        skip_existing: bool = True,
        raise_errors: bool = False,
):
    """
    Performs event confirmation according to provided set of selectors.
    Uses Dask.distributed for parallelism.
    All the calculations are divided into batches in order to speed up queries that gather all the inputs.

    :param event_confirmation_params_id: ID of EventConfirmationParams object to be used as config
    :type event_confirmation_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param networks: Selector for Networks
    :type networks: Optional[Collection[str]]
    :param stations: Selector stations
    :type stations: Optional[Collection[str]]
    :param exclude_stations: Stations to be excluded from selector
    :type exclude_stations: Optional[Collection[str]]
    :param specific_stations_params: Stations using specific EventConfirmationParams
    :type specific_stations_params: Optional[Collection[str]]
    :param components: Selector components
    :type components: Optional[Collection[str]]
    :param component_ids: IDs of Selector components
    :type component_ids: Optional[Collection[int]]
    :param raise_errors: If errors should be raised or just logged
    :type raise_errors: bool
    :param skip_existing: If pre-existing EventConfirmationResult should be skipped.
    :type skip_existing: bool
    :param batch_size: How big should be the batch of calculations
    :type batch_size: int
    :param parallel: If the calculations should be done in parallel
    :type parallel: bool
    :return: None
    :rtype: NoneType
    """

    calculation_inputs = _prepare_inputs_for_event_confirmation(
        event_confirmation_params_id=event_confirmation_params_id,
        starttime=starttime,
        endtime=endtime,
        networks=networks,
        stations=stations,
        exclude_stations=exclude_stations,
        specific_stations_params=specific_stations_params,
        components=components,
        component_ids=component_ids,
        skip_existing=skip_existing,
        batch_size=batch_size,
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_event_confirmation_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_event_confirmation,
            with_file=True,
            raise_errors=raise_errors,
            is_event_confirmation=True,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_event_confirmation_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_event_confirmation,
            with_file=True,
            raise_errors=raise_errors,
            is_event_confirmation=True,
        )

    return


def _prepare_inputs_for_event_confirmation(
        event_confirmation_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        exclude_stations: Optional[Union[Collection[str], str]] = None,
        specific_stations_params: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        batch_size: int = 10,
        skip_existing: bool = True,
) -> Generator[EventConfirmationRunnerInputs, None, None]:
    """
    Parses the given selectors and retrieves the corresponding EventDetectionResult.
    Creates and upsert an EventConfirmationRun object.
    Finally, EventConfirmationRunnerInputs are generated and yielded for each timespans
    found between the given starttime and endtime.

    :param event_confirmation_params_id: ID of EventConfirmationParams object to be used as config
    :type event_confirmation_params_id: int
    :param starttime: Date from where to start the query
    :type starttime: Union[datetime.date, datetime.datetime]
    :param endtime: Date on which finish the query
    :type endtime: Union[datetime.date, datetime.datetime]
    :param networks: Selector for Networks
    :type networks: Optional[Collection[str]]
    :param stations: Selector stations
    :type stations: Optional[Collection[str]]
    :param exclude_stations: Stations to be excluded from selector
    :type exclude_stations: Optional[Collection[str]]
    :param specific_stations_params: Stations using specific EventConfirmationParams
    :type specific_stations_params: Optional[Collection[str]]
    :param components: Selector components
    :type components: Optional[Collection[str]]
    :param component_ids: IDs of Selector components
    :type component_ids: Optional[Collection[int]]
    :param skip_existing: If pre-existing EventConfirmationResult should be skipped.
    :type skip_existing: bool
    :param batch_size: How big should be the batch of calculations
    :type batch_size: int
    :return: EventConfirmationRunnerInputs
    :rtype: Generator[EventConfirmationRunnerInputs, None, None]
    """

    logger.debug(f"Fetching EventConfirmationParams with ids {event_confirmation_params_id}")
    params = fetch_event_confirmation_params_by_id(params_id=event_confirmation_params_id)

    logger.debug("Fetching and components")
    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )

    # checks for specific station:event_detection_params combos and parse them in a dict
    couple_params_stations = None
    if specific_stations_params is not None:
        couple_params_stations = _parse_str_collection_as_dict(specific_stations_params)
        if exclude_stations is None:
            exclude_stations = tuple(couple_params_stations)
        else:
            exclude_stations += tuple(couple_params_stations)  # type: ignore

    # Removes the components from excluded or specific stations.
    if exclude_stations is not None:
        excluded_components = fetch_components(
            networks=networks,
            stations=exclude_stations,
            components=components,
            component_ids=component_ids,
        )
        fetched_components = [c for c in fetched_components if c not in excluded_components]
    logger.debug(f"Fetched {len(fetched_components)} components")

    logger.debug("Creating EventConfirmationRun")
    _create_and_upsert_event_confirmation_run(
        event_confirmation_params=params,
        datachunk_params_id=params.datachunk_params_id,
        timespans=fetch_timespans_between_dates(starttime=starttime, endtime=endtime),
        fetched_components=fetched_components,
        couple_params_stations=couple_params_stations,
        networks=networks,
        components=components,
        component_ids=component_ids,
    )
    # Then retrieves it.
    event_confirmation_run = fetch_latest_event_confirmation_run()

    i = 0

    while True:
        logger.info(f"Querying batch no. {i} of timepans")
        timespans_query = Timespan.query.filter(
            Timespan.starttime >= starttime,
            Timespan.endtime <= endtime,
            )
        timespans = timespans_query.limit(batch_size).offset(i * batch_size)
        fetched_timespans = timespans.all()

        logger.info(f"Treating {len(fetched_timespans)} timepans during {i}th batch. ")
        logger.info(fetched_timespans)

        if len(fetched_timespans) == 0:
            logger.info("This batch does not contains any timespan")
            break  # end the while loop when all timespans in fetched_timespans are handled.

        for timespan in fetched_timespans:

            # Retrieves event_detection_results from default args
            fetched_datachunks = _query_datachunks(
                datachunk_params_id=params.datachunk_params_id,
                timespans=[timespan],
                components=fetched_components,
                load_timespan=False,
                load_component=False,
                ).all()
            fetched_datachunk_ids = extract_object_ids(fetched_datachunks)
            event_detection_results = fetch_event_detection_results(
                    event_detection_params_id=params.event_detection_params_id,
                    datachunk_ids=fetched_datachunk_ids
                )

            # Retrieves event_detection_results from specific args
            if specific_stations_params is not None:
                for station, event_detection_params_id in couple_params_stations.items():  # type: ignore
                    cmp = fetch_components(
                        networks=networks,
                        stations=station,
                        components=components,
                        component_ids=component_ids,
                    )
                    dks = _query_datachunks(
                        datachunk_params_id=params.datachunk_params_id,
                        timespans=[timespan],
                        components=cmp,
                        load_timespan=False,
                        load_component=False,
                    ).all()
                    specifics_event_detection_results = fetch_event_detection_results(
                        event_detection_params_id=int(event_detection_params_id),
                        datachunk_ids=extract_object_ids(dks)
                        )
                    event_detection_results.extend(specifics_event_detection_results)

            if len(event_detection_results) == 0:
                logger.info("This batch did not contain any elements")
                continue

            logger.debug(f"There were {len(event_detection_results)} in the timespan")

            db.session.expunge_all()
            yield EventConfirmationRunnerInputs(
                    event_confirmation_params=params,
                    timespan=timespan,
                    event_detection_results=event_detection_results,
                    event_confirmation_run=event_confirmation_run,
                )

        i += 1


def _create_and_upsert_event_confirmation_run(
        event_confirmation_params: EventConfirmationParams,
        datachunk_params_id: int,
        timespans: Collection[Timespan],
        fetched_components: Collection[Component],
        networks: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        couple_params_stations: Optional[dict] = None,
):
    """
    Creates and upsert an EventConfirmationRun object in the db to log the parameters
    of a $:> noiz processing run_event_confirmation command.
    It creation enables the logging of each datachunks considered for event confirmation.

    :param event_confirmation_params: an EventConfirmationParams
    :type event_confirmation_params: EventConfirmationParams
    :param datachunk_params_id: ID of a DatachunkParams
    :type datachunk_params_id: int
    :param timespans: a collection of Timespan
    :type timespans: Collection[Timespan]
    :param fetched_components: a collection of Component
    :type fetched_components: Collection[Component]
    :param networks: Selector for Networks
    :type networks: Optional[Collection[str]]
    :param components: Selector components
    :type components: Optional[Collection[str]]
    :param component_ids: IDs of Selector components
    :type component_ids: Optional[Collection[int]]
    :param couple_params_stations: a dict with station as key and EventDetectionParams.id as value.
    :type couple_params_stations:  Optional[dict]
    :return: None
    :rtype: NoneType
    """

    event_detection_params = fetch_event_detection_params_by_id(
        event_confirmation_params.event_detection_params_id
        )

    event_confirmation_run = EventConfirmationRun(
        event_confirmation_params=event_confirmation_params,
        event_detection_type=event_detection_params.detection_type,)

    fetched_datachunks = _query_datachunks(
            datachunk_params_id=datachunk_params_id,
            timespans=timespans,
            components=fetched_components,
            load_timespan=False,
            load_component=False,
        ).all()

    if couple_params_stations is not None:
        event_confirmation_run.specific_stations_params = str(couple_params_stations)
        for station, params_id in couple_params_stations.items():
            cmp = fetch_components(
                networks=networks,
                stations=station,
                components=components,
                component_ids=component_ids,
            )
            prm = fetch_event_detection_params_by_id(params_id)
            fetched_datachunks.extend(
                _query_datachunks(
                    datachunk_params_id=prm.datachunk_params_id,
                    timespans=timespans,
                    components=cmp,
                    load_timespan=False,
                    load_component=False,
                ).all()
            )

    event_confirmation_run.datachunks = fetched_datachunks

    db.session.add(event_confirmation_run)
    logger.info("Commiting event_confirmation_run to db")
    db.session.commit()


def _prepare_upsert_command_event_confirmation(event_confirmation_result: EventConfirmationResult) -> Insert:
    """
    Private method that generates an :py:class:`~sqlalchemy.dialects.postgresql.Insert` for
    :py:class:`~noiz.models.event_detection.EventConfirmationResult` to be upserted to db.
    Postgres specific because it's upsert.
    Only used if something goes wrong with bulk_add_objects().

    :param results: Instance which is to be upserted
    :type results: noiz.models.event_detection.EventConfirmationResult
    :return: Postgres-specific upsert command
    :rtype: sqlalchemy.dialects.postgresql.Insert
    """

    insert_command = (
        insert(EventConfirmationResult)
        .values(
            event_confirmation_params_id=event_confirmation_result.event_confirmation_params_id,
            timespan_id=event_confirmation_result.timespan_id,
            event_confirmation_run_id=event_confirmation_result.event_confirmation_run_id,
            time_start=event_confirmation_result.time_start,
            time_stop=event_confirmation_result.time_stop,
            peak_ground_velocity=event_confirmation_result.peak_ground_velocity,
            number_station_triggered=event_confirmation_result.number_station_triggered,
            event_confirmation_file_id=event_confirmation_result.file.id,
        )
        .on_conflict_do_update(
            constraint="unique_confirmation_per_timespan_per_param_per_time",
            set_=dict(event_confirmation_file_id=event_confirmation_result.event_confirmation_file_id),
        )
    )
    return insert_command

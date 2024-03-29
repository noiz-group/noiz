# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime

from loguru import logger
from sqlalchemy.orm import Query, subqueryload
from sqlalchemy.dialects.postgresql import Insert, insert
from typing import Union, Optional, Collection, Generator, List, Tuple

from noiz.api.component import fetch_components
from noiz.api.datachunk import _query_datachunks
from noiz.api.helpers import _run_calculate_and_upsert_on_dask, _run_calculate_and_upsert_sequentially, \
    extract_object_ids
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import Datachunk, PPSDResult, PPSDParams
from noiz.models.type_aliases import PPSDRunnerInputs
from noiz.processing.ppsd import calculate_ppsd_wrapper
from noiz.validation_helpers import validate_maximum_one_argument_provided, validate_to_tuple


def fetch_ppsd_params_by_id(params_id: int) -> PPSDParams:
    """
    Fetches a single PPSDParams objects by its ID.

    :param params_id: ID of PPSDParams to be fetched
    :type params_id: int
    :return: fetched PPSDParams object
    :rtype: PPSDParams
    """
    fetched_params = PPSDParams.query.filter_by(id=params_id).first()
    if fetched_params is None:
        raise EmptyResultException(f"PPSDParams object of id {params_id} does not exist.")
    return fetched_params


def fetch_ppsd_results(
        ppsd_params: Optional[PPSDParams] = None,
        ppsd_params_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_ppsd_params: bool = False,
) -> List[PPSDResult]:
    """filldocs"""
    query = _query_ppsd_results(
        ppsd_params=ppsd_params,
        ppsd_params_id=ppsd_params_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
        load_timespan=load_timespan,
        load_datachunk=load_datachunk,
        load_ppsd_params=load_ppsd_params,
    )

    return query.all()


def count_ppsd_results(
        ppsd_params: Optional[PPSDParams] = None,
        ppsd_params_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
) -> int:
    """filldocs"""
    query = _query_ppsd_results(
        ppsd_params=ppsd_params,
        ppsd_params_id=ppsd_params_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
    )

    return query.count()


def _query_ppsd_results(
        ppsd_params: Optional[PPSDParams] = None,
        ppsd_params_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_ppsd_params: bool = False,
) -> Query:
    """filldocs"""
    try:
        validate_maximum_one_argument_provided(ppsd_params, ppsd_params_id)
    except ValueError:
        raise ValueError('Maximum one of ppsd_params or ppsd_params_id can be provided')
    try:
        validate_maximum_one_argument_provided(datachunks, datachunk_ids)
    except ValueError:
        raise ValueError('Maximum one of datachunks or datachunk_ids can be provided')

    filters, opts = _determine_filters_and_opts_for_datachunk(
        ppsd_params=ppsd_params,
        ppsd_params_id=ppsd_params_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
        load_timespan=load_timespan,
        load_datachunk=load_datachunk,
        load_ppsd_params=load_ppsd_params,
    )

    query = PPSDResult.query.filter(*filters).options(opts)

    return query


def _determine_filters_and_opts_for_datachunk(
        ppsd_params: Optional[PPSDParams] = None,
        ppsd_params_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_timespan: bool = False,
        load_datachunk: bool = False,
        load_ppsd_params: bool = False,
) -> Tuple[List, List]:

    filters = []
    if ppsd_params is not None:
        filters.append(PPSDResult.ppsd_params_id.in_((ppsd_params.id,)))
    if ppsd_params_id is not None:
        qcone_config_ids = validate_to_tuple(val=ppsd_params_id, accepted_type=int)
        filters.append(PPSDResult.ppsd_params_id.in_(qcone_config_ids))
    if datachunks is not None:
        extracted_datachunk_ids = extract_object_ids(datachunks)
        filters.append(PPSDResult.datachunk_id.in_(extracted_datachunk_ids))
    if datachunk_ids is not None:
        filters.append(PPSDResult.datachunk_id.in_(datachunk_ids))
    if len(filters) == 0:
        filters.append(True)
    opts = []
    if load_timespan:
        opts.append(subqueryload(PPSDResult.timespan))
    if load_datachunk:
        opts.append(subqueryload(PPSDResult.datachunk))
    if load_ppsd_params:
        opts.append(subqueryload(PPSDResult.ppsd_params))
    return filters, opts


def run_psd_calculations(
        ppsd_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        batch_size: int = 2000,
        parallel: bool = True,
        skip_existing: bool = True,
        raise_errors: bool = False,
):
    # filldocs
    calculation_inputs = _prepare_inputs_for_psd_calculation(
        ppsd_params_id=ppsd_params_id,
        starttime=starttime,
        endtime=endtime,
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
        skip_existing=skip_existing,
        batch_size=batch_size,
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_ppsd_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_ppsd,
            with_file=True,
            raise_errors=raise_errors,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_ppsd_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_ppsd,
            with_file=True,
            raise_errors=raise_errors,
        )

    return


def _prepare_inputs_for_psd_calculation(
        ppsd_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        batch_size: int = 2000,
        skip_existing: bool = True,
) -> Generator[PPSDRunnerInputs, None, None]:

    params = fetch_ppsd_params_by_id(params_id=ppsd_params_id)

    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)

    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )

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
            logger.debug("Fetching existing ppsds")
            existing_results = fetch_ppsd_results(ppsd_params=params, datachunk_ids=fetched_datachunk_ids)
            existing_results_datachunk_ids = [x.datachunk_id for x in existing_results]
        else:
            existing_results_datachunk_ids = list()

        for datachunk in fetched_datachunks:
            if datachunk.id in existing_results_datachunk_ids:
                logger.debug(f"There already exists PPSDResult for datachunk {datachunk}")
                continue

            db.session.expunge_all()
            yield PPSDRunnerInputs(
                ppsd_params=params,
                timespan=datachunk.timespan,
                datachunk=datachunk,
                component=datachunk.component,
            )

        i += 1


def _prepare_upsert_command_ppsd(ppsd_result: PPSDResult) -> Insert:
    insert_command = (
        insert(PPSDResult)
        .values(
            ppsd_params_id=ppsd_result.ppsd_params_id,
            datachunk_id=ppsd_result.datachunk_id,
            timespan_id=ppsd_result.timespan_id,
            ppsd_file_id=ppsd_result.ppsd_file_id,
        )
        .on_conflict_do_update(
            constraint="unique_ppsd_per_config_per_datachunk",
            set_=dict(ppsd_file_id=ppsd_result.ppsd_file_id),
        )
    )
    return insert_command

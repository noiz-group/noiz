import datetime
from sqlalchemy.orm import Query

from noiz.api.datachunk import fetch_datachunks, _query_datachunks
from noiz.api.processing_config import fetch_datachunkparams_by_id
from noiz.api.type_aliases import PPSDRunnerInputs
from typing import Union, Optional, Collection, Generator, List

from noiz.api.component import fetch_components
from noiz.api.helpers import _run_calculate_and_upsert_on_dask, _run_calculate_and_upsert_sequentially, \
    extract_object_ids
from noiz.api.timespan import fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import EmptyResultException
from noiz.models import Datachunk
from noiz.models.ppsd import PPSDResult
from noiz.models.processing_params import PPSDParams
from noiz.processing.ppsd import calculate_ppsd_wrapper
from noiz.validation_helpers import validate_maximum_one_argument_provided, validate_to_tuple


def fetch_ppsd_params_by_id(id: int) -> PPSDParams:
    """
    Fetches a single PPSDParams objects by its ID.

    :param id: ID of PPSDParams to be fetched
    :type id: int
    :return: fetched PPSDParams object
    :rtype: PPSDParams
    """
    fetched_params = PPSDParams.query.filter_by(id=id).first()
    if fetched_params is None:
        raise EmptyResultException(f"PPSDParams object of id {id} does not exist.")
    return fetched_params


def fetch_ppsd_results(
        ppsd_params: Optional[PPSDParams] = None,
        ppsd_params_id: Optional[int] = None,
        datachunks: Optional[Collection[Datachunk]] = None,
        datachunk_ids: Optional[Collection[int]] = None,
) -> List[PPSDResult]:
    """filldocs"""
    query = _query_ppsd_results(
        ppsd_params=ppsd_params,
        ppsd_params_id=ppsd_params_id,
        datachunks=datachunks,
        datachunk_ids=datachunk_ids,
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

    query = PPSDResult.query.filter(*filters)

    return query


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
            upserter_callable=_prepare_upsert_command_processed_datachunk,
            with_file=True,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_ppsd_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_processed_datachunk,
            with_file=True,
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

    params = fetch_ppsd_params_by_id(id=ppsd_params_id)

    timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)

    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )

    i = 0
    while True:
        query = _query_datachunks(
            datachunk_params_id=params.datachunk_params_id,
            timespans=timespans,
            components=fetched_components,
            load_timespan=True,
            load_component=True,
        )

        query = query.limit(batch_size).offset(i*batch_size)

        fetched_datachunks = query.all()

        if len(fetched_datachunks) == 0 :
            break

        fetched_datachunk_ids = extract_object_ids(fetched_datachunks)

        if skip_existing:
            existing_results = fetch_ppsd_results(ppsd_params=params, datachunk_ids=fetched_datachunk_ids)
            existing_results_datachunk_ids = [x.datachunk_id for x in existing_results]
        else:
            existing_results_datachunk_ids = tuple()

        for datachunk in fetched_datachunks:
            if datachunk.id in existing_results_datachunk_ids:
                continue

            db.session.expunge_all()
            yield PPSDRunnerInputs(
                ppsd_params=params,
                timespan=datachunk.timespan,
                datachunk=datachunk,
                component=datachunk.component,
            )

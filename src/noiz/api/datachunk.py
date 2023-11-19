# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import datetime
import itertools
import more_itertools
from loguru import logger
import pendulum
import numpy as np
from sqlalchemy.dialects.postgresql import Insert, insert

from sqlalchemy.orm import subqueryload, Query
from typing import List, Tuple, Collection, Optional, Dict, Union, Generator

from noiz.api.component import fetch_components
from noiz.api.helpers import extract_object_ids, _run_calculate_and_upsert_on_dask, \
    _run_calculate_and_upsert_sequentially
from noiz.api.processing_config import fetch_datachunkparams_by_id, fetch_processed_datachunk_params_by_id
from noiz.api.timeseries import fetch_raw_timeseries
from noiz.api.timespan import fetch_timespans_for_doy, fetch_timespans_between_dates
from noiz.database import db
from noiz.exceptions import NoDataException
from noiz.models import AveragedSohGps, Component, Datachunk, DatachunkParams, DatachunkStats, ProcessedDatachunk, \
    QCOneConfig, QCOneResults, Timespan
from noiz.models.type_aliases import CalculateDatachunkStatsInputs, RunDatachunkPreparationInputs, \
    ProcessDatachunksInputs
from noiz.processing.datachunk import create_datachunks_for_component_wrapper, calculate_datachunk_stats_wrapper
from noiz.processing.datachunk_processing import process_datachunk_wrapper
from noiz.validation_helpers import validate_maximum_one_argument_provided


def count_datachunks(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_params: Optional[DatachunkParams] = None,
        datachunk_params_id: Optional[int] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        not_datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
) -> int:
    """
    Counts number of datachunks that fit passed parameter set.

    :param components: Components to be checked
    :type components: Optional[Collection[Component]]
    :param timespans: Timespans to be checked
    :type timespans: Optional[Collection[Timespan]]
    :param datachunk_params: DatachunkParams to be checked. This have to be a single object.
    :type datachunk_params: Optional[DatachunkParams]
    :param datachunk_params_id: ID of DatachunkParams to be checked. This have to be an id of a single object.
    :type datachunk_params_id: Optional[int]
    :param datachunk_ids: Ids of Datachunk objects to be fetched
    :type datachunk_ids: Optional[Collection[int]]
    :param not_datachunk_ids: Ids of Datachunk objects that are not to be fetched
    :type not_datachunk_ids: Optional[Collection[int]]
    :param load_component: Loads also the associated Component object so it is available for usage \
        without context
    :type load_component: bool
    :param load_stats: Loads also the associated DatachunkStats object so it is available for usage \
        without context
    :type load_stats: bool
    :param load_timespan: Loads also the associated Timespan object so it is available for usage \
        without context
    :type load_timespan: bool
    :param load_processing_params: Loads also the associated DatachunkParams object \
        so it is available for usage without context
    :type load_processing_params: bool
    :return: Count fo datachunks
    :rtype: int
    """
    query = _query_datachunks(
        components=components,
        timespans=timespans,
        datachunk_params=datachunk_params,
        datachunk_params_id=datachunk_params_id,
        datachunk_ids=datachunk_ids,
        not_datachunk_ids=not_datachunk_ids,
        load_component=load_component,
        load_stats=load_stats,
        load_timespan=load_timespan,
        load_processing_params=load_processing_params,
    )

    return query.count()


def fetch_datachunks(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_params: Optional[DatachunkParams] = None,
        datachunk_params_id: Optional[int] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        not_datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
        order_by_id: bool = True,
) -> List[Datachunk]:
    """
    Fetches datachunks based on provided filters.

    You can filter your call by providing Timespans and/or components and/or
    processing_params you want to select Datachunk based on.

    None of The filters is obligatory. If none of the filters will be provided,
    it will return you content of the whole table Datachunk.
    This will be unpleasant.

    :param components: Components to be checked
    :type components: Optional[Collection[Component]]
    :param timespans: Timespans to be checked
    :type timespans: Optional[Collection[Timespan]]
    :param datachunk_params: DatachunkParams to be checked. This have to be a single object.
    :type datachunk_params: Optional[DatachunkParams]
    :param datachunk_params_id: ID of DatachunkParams to be checked. This have to be an id of a single object.
    :type datachunk_params_id: Optional[int]
    :param datachunk_ids: Ids of Datachunk objects to be fetched
    :type datachunk_ids: Optional[Collection[int]]
    :param not_datachunk_ids: Ids of Datachunk objects that are not to be fetched
    :type not_datachunk_ids: Optional[Collection[int]]
    :param load_component: Loads also the associated Component object so it is available for usage \
        without context
    :type load_component: bool
    :param load_stats: Loads also the associated DatachunkStats object so it is available for usage \
        without context
    :type load_stats: bool
    :param load_timespan: Loads also the associated Timespan object so it is available for usage \
        without context
    :type load_timespan: bool
    :param load_processing_params: Loads also the associated DatachunkParams object \
        so it is available for usage without context
    :type load_processing_params: bool
    :param order_by_id: If results of the query should be ordered by id in ascending order
    :type order_by_id: bool
    :return: List of Datachunks loaded from DB
    :rtype: List[Datachunk]
    """

    query = _query_datachunks(
        components=components,
        timespans=timespans,
        datachunk_params=datachunk_params,
        datachunk_params_id=datachunk_params_id,
        datachunk_ids=datachunk_ids,
        not_datachunk_ids=not_datachunk_ids,
        load_component=load_component,
        load_stats=load_stats,
        load_timespan=load_timespan,
        load_processing_params=load_processing_params,
        order_by_id=order_by_id,
    )

    return query.all()


def fetch_datachunks_without_stats(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_params: Optional[DatachunkParams] = None,
        datachunk_params_id: Optional[int] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        not_datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
) -> List[Datachunk]:
    """filldocs"""
    query = _query_datachunks(
        components=components,
        timespans=timespans,
        datachunk_params=datachunk_params,
        datachunk_params_id=datachunk_params_id,
        datachunk_ids=datachunk_ids,
        not_datachunk_ids=not_datachunk_ids,
        load_component=load_component,
        load_stats=load_stats,
        load_timespan=load_timespan,
        load_processing_params=load_processing_params,
    )
    return query.filter(~Datachunk.stats.has()).all()


def fetch_datachunks_from_other_channels(
        datachunk: Optional[Datachunk] = None,
        datachunk_id: Optional[int] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
        order_by_id: bool = True,
) -> List[Datachunk]:
    """
    Fetches datachunks from the other channels with the same station, network,
    timespan and datachunk_params as the datachunk given in argument. The given
    datachunk will also be returned alongside the other two.

    Either a datachunk or datachunk_id is required.

    If no datachunks fitting the query are found,
    an empty list will be returned.

    :param datachunk: datachunk whose other channels need to be fetch
    :type datachunk: Optional[Datachunk]
    :param datachunk_ids: Ids of Datachunk objects to be fetched
    :type datachunk_ids: Optional[int]
    :param load_component: Loads also the associated Component object so it is available for usage \
        without context
    :type load_component: bool
    :param load_stats: Loads also the associated DatachunkStats object so it is available for usage \
        without context
    :type load_stats: bool
    :param load_timespan: Loads also the associated Timespan object so it is available for usage \
        without context
    :type load_timespan: bool
    :param load_processing_params: Loads also the associated DatachunkParams object \
        so it is available for usage without context
    :type load_processing_params: bool
    :param order_by_id: If results of the query should be ordered by id in ascending order
    :type order_by_id: bool
    :return: List of Datachunks loaded from DB
    :rtype: List[Datachunk]
    """

    if datachunk is None and datachunk_id is None:
        raise ValueError("Neither `datachunk` nor `datachunk_id` argument was provided. It is required to provide exactly one of them")
    elif None not in (datachunk, datachunk_id):
        raise ValueError("A datachunk AND a datachunk_id cannot be given at the same time.")

    if datachunk_id is not None:
        datachunk = fetch_datachunks(datachunk_ids=[datachunk_id])[0]

    query = _query_datachunks(
        components=fetch_components(
            networks=datachunk.component.network,  # type: ignore
            stations=datachunk.component.station,  # type: ignore
            ),
        timespans=[datachunk.timespan],  # type: ignore
        datachunk_params_id=datachunk.datachunk_params_id,  # type: ignore
        load_component=load_component,
        load_stats=load_stats,
        load_timespan=load_timespan,
        load_processing_params=load_processing_params,
        order_by_id=order_by_id,
    )

    return query.all()


def query_datachunks_without_qcone(
        qc_one: QCOneConfig,
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_params: Optional[DatachunkParams] = None,
        datachunk_params_id: Optional[int] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        not_datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
) -> Query:
    """filldocs"""
    filters, opts = _determine_filters_and_opts_for_datachunk(
        timespans=timespans,
        components=components,
        datachunk_params=datachunk_params,
        datachunk_params_id=datachunk_params_id,
        datachunk_ids=datachunk_ids,
        not_datachunk_ids=not_datachunk_ids,
        load_component=load_component,
        load_processing_params=load_processing_params,
        load_stats=load_stats,
        load_timespan=load_timespan,
    )
    objects_to_query = [Datachunk, DatachunkStats]
    if qc_one.uses_gps():
        objects_to_query.append(AveragedSohGps)

    query = (db.session
             .query(*tuple(objects_to_query))
             .join(DatachunkStats, Datachunk.id == DatachunkStats.datachunk_id)
             .filter(*filters).options(opts))

    return query


def _query_datachunks(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_params: Optional[DatachunkParams] = None,
        datachunk_params_id: Optional[int] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        not_datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
        load_device: bool = False,
        order_by_id: bool = True,
) -> Query:

    filters, opts = _determine_filters_and_opts_for_datachunk(
        components=components,
        timespans=timespans,
        datachunk_params=datachunk_params,
        datachunk_params_id=datachunk_params_id,
        datachunk_ids=datachunk_ids,
        not_datachunk_ids=not_datachunk_ids,
        load_component=load_component,
        load_stats=load_stats,
        load_processing_params=load_processing_params,
        load_timespan=load_timespan,
        load_device=load_device,
    )
    if order_by_id:
        return Datachunk.query.filter(*filters).options(opts).order_by(Datachunk.id)
    else:
        return Datachunk.query.filter(*filters).options(opts)


def _determine_filters_and_opts_for_datachunk(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_params: Optional[DatachunkParams] = None,
        datachunk_params_id: Optional[int] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        not_datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
        load_device: bool = False,
) -> Tuple[List, List]:
    try:
        validate_maximum_one_argument_provided(datachunk_params, datachunk_params_id)
    except ValueError:
        raise ValueError('You can provide maximum one of datachunk_params or datachunk_params_id')

    filters = []
    if components is not None:
        component_ids = extract_object_ids(components)
        filters.append(Datachunk.component_id.in_(component_ids))
    if timespans is not None:
        timespan_ids = extract_object_ids(timespans)
        filters.append(Datachunk.timespan_id.in_(timespan_ids))
    if datachunk_params is not None:
        filters.append(Datachunk.datachunk_params_id == datachunk_params.id)
    if datachunk_params_id is not None:
        filters.append(Datachunk.datachunk_params_id == datachunk_params_id)
    if datachunk_ids is not None:
        filters.append(Datachunk.id.in_(datachunk_ids))
    if not_datachunk_ids is not None:
        filters.append(~Datachunk.id.in_(not_datachunk_ids))
    if len(filters) == 0:
        filters.append(True)
    opts = []
    if load_timespan:
        opts.append(subqueryload(Datachunk.timespan))
    if load_stats:
        opts.append(subqueryload(Datachunk.stats))
    if load_component:
        opts.append(subqueryload(Datachunk.component))
    if load_processing_params:
        opts.append(subqueryload(Datachunk.params))
    if load_device:
        opts.append(subqueryload(Datachunk.device))

    return filters, opts


def _prepare_upsert_command_datachunk(datachunk: Datachunk) -> Insert:
    insert_datachunk = (
        insert(Datachunk)
        .values(
            processing_config_id=datachunk.datachunk_params_id,
            datachunk_file_id=datachunk.file.id,
            component_id=datachunk.component_id,
            timespan_id=datachunk.timespan_id,
            sampling_rate=datachunk.sampling_rate,
            npts=datachunk.npts,
            datachunk_file=datachunk.file,
            padded_npts=datachunk.padded_npts,
        )
        .on_conflict_do_update(
            constraint="unique_datachunk_per_timespan_per_station_per_processing",
            set_=dict(
                datachunk_file_id=datachunk.file.id,
                padded_npts=datachunk.padded_npts,
                sampling_rate=datachunk.sampling_rate,
                npts=datachunk.npts,
            )
        )
    )
    return insert_datachunk


def _generate_datachunk_preparation_inputs(
        stations: Optional[Tuple[str]],
        components: Optional[Tuple[str]],
        startdate: datetime.datetime,
        enddate: datetime.datetime,
        processing_config_id: int,
        skip_existing: bool = True,
) -> Generator[RunDatachunkPreparationInputs, None, None]:
    date_period = pendulum.period(startdate, enddate)  # type: ignore

    logger.info("Fetching processing config, timespans and components from db. ")
    processing_params = fetch_datachunkparams_by_id(id=processing_config_id)

    all_timespans = [(date, fetch_timespans_for_doy(
        year=date.year, doy=date.day_of_year
    )) for date in date_period.range('days')]

    if len(all_timespans) == 0:
        raise ValueError("There were no timespans for requested dates. Check if you created timespans at all.")

    fetched_components = fetch_components(networks=None,
                                          stations=stations,
                                          components=components)

    for component, (date, timespans) in itertools.product(fetched_components,
                                                          all_timespans):

        logger.info(f"Looking for data on {date} for {component}")

        try:
            time_series = fetch_raw_timeseries(
                component=component, execution_date=date
            )
        except NoDataException as e:
            logger.warning(f"{e} Skipping.")
            continue

        if skip_existing:
            logger.debug("Checking if some timespans already exists")
            existing_count = count_datachunks(
                components=(component,),
                timespans=timespans,
                datachunk_params=processing_params,
            )
            if existing_count == len(timespans):
                logger.debug("Number of existing timespans is sufficient. Skipping")
                continue
            if existing_count > 0:
                logger.debug(f"There are only {existing_count} existing Datachunks. "
                             f"Looking for those that are missing one by one.")
                new_timespans = [timespan for timespan in timespans if
                                 count_datachunks(
                                     components=(component,),
                                     timespans=(timespan,),
                                     datachunk_params=processing_params
                                 ) == 0]
                timespans = new_timespans

        logger.info(f"There are {len(timespans)} to be sliced for that seismic file.")

        db.session.expunge_all()
        yield RunDatachunkPreparationInputs(
            component=component,
            timespans=timespans,
            time_series=time_series,
            processing_params=processing_params,
        )


def run_datachunk_preparation(
        stations: Tuple[str],
        components: Tuple[str],
        startdate: datetime.datetime,
        enddate: datetime.datetime,
        processing_config_id: int,
        parallel: bool = True,
        batch_size: int = 1000,

):
    logger.info("Preparing jobs for execution")
    calculation_inputs = _generate_datachunk_preparation_inputs(
        stations=stations,
        components=components,
        startdate=startdate,
        enddate=enddate,
        processing_config_id=processing_config_id,
    )

    # TODO add more checks for bad seed files because they are crashing.
    # And instead of datachunk id there was something weird produced. It was found on TD26 in
    # 2019.04.~10-15

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=create_datachunks_for_component_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_datachunk,
            with_file=True,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=create_datachunks_for_component_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_datachunk,
            with_file=True,
        )


def run_stats_calculation(
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        datachunk_params_id: int,
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        batch_size: int = 5000,
        parallel: bool = True,
):

    calculation_inputs = _prepare_inputs_for_datachunk_stats_calculations(
        starttime=starttime,
        endtime=endtime,
        datachunk_params_id=datachunk_params_id,
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_datachunk_stats_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_datachunk_stats,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=calculate_datachunk_stats_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_datachunk_stats,
        )
    return


def _prepare_inputs_for_datachunk_stats_calculations(
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        datachunk_params_id: int,
        networks: Optional[Union[Collection[str], str]],
        stations: Optional[Union[Collection[str], str]],
        components: Optional[Union[Collection[str], str]],
        component_ids: Optional[Union[Collection[int], int]],
) -> Generator[CalculateDatachunkStatsInputs, None, None]:
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids
    )
    fetched_datachunk_params = fetch_datachunkparams_by_id(id=datachunk_params_id)
    fetched_datachunks = fetch_datachunks_without_stats(
        timespans=fetched_timespans,
        components=fetched_components,
        datachunk_params=fetched_datachunk_params
    )

    db.session.expunge_all()
    for datachunk in fetched_datachunks:
        yield CalculateDatachunkStatsInputs(
            datachunk=datachunk,
            datachunk_file=None,
        )

    return


def _prepare_upsert_command_datachunk_stats(datachunk_stats: DatachunkStats) -> Insert:
    insert_command = (
        insert(DatachunkStats)
        .values(
            datachunk_id=datachunk_stats.datachunk_id,
            energy=datachunk_stats.energy,
            min=datachunk_stats.min,
            max=datachunk_stats.max,
            mean=datachunk_stats.mean,
            variance=datachunk_stats.variance,
            skewness=datachunk_stats.skewness,
            kurtosis=datachunk_stats.kurtosis,
        )
        .on_conflict_do_update(
            constraint="unique_stats_per_datachunk",
            set_=dict(
                energy=datachunk_stats.energy,
                min=datachunk_stats.min,
                max=datachunk_stats.max,
                mean=datachunk_stats.mean,
                variance=datachunk_stats.variance,
                skewness=datachunk_stats.skewness,
                kurtosis=datachunk_stats.kurtosis,
            ),
        )
    )
    return insert_command


def run_datachunk_processing(
        processed_datachunk_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        batch_size: int = 2500,
        parallel: bool = True,
        skip_existing: bool = True,
):
    # filldocs
    calculation_inputs = _select_datachunks_for_processing(
        processed_datachunk_params_id=processed_datachunk_params_id,
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
            calculation_task=process_datachunk_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_processed_datachunk,
            with_file=True,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=process_datachunk_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_processed_datachunk,
            with_file=True,
        )

    return


def _select_datachunks_for_processing(
        processed_datachunk_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        skip_existing: bool = True,
        batch_size: int = 2500,
) -> Generator[ProcessDatachunksInputs, None, None]:
    # filldocs

    logger.debug(f"Fetching ProcessedDatachunkParams with id {processed_datachunk_params_id}")
    params = fetch_processed_datachunk_params_by_id(processed_datachunk_params_id)
    logger.debug(f"Fetching ProcessedDatachunkParams successful. {params}")

    logger.debug(f"Fetching timespans for {starttime} - {endtime}")
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    logger.debug(f"Fetched {len(fetched_timespans)} timespans")

    logger.debug("Fetching components")
    fetched_components = fetch_components(
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )
    logger.debug(f"Fetched {len(fetched_components)} components")

    logger.debug("Fetching datachunks")
    fetched_datachunks = fetch_datachunks(
        timespans=fetched_timespans,
        components=fetched_components,
        datachunk_params=params.datachunk_params,
        load_timespan=True,
        load_component=True,
        order_by_id=True,
    )
    logger.debug(f"Fetched {len(fetched_datachunks)} datachunks")

    fetched_datachunks_ids = extract_object_ids(fetched_datachunks)
    valid_chunks: Dict[bool, List[int]] = {True: [], False: []}

    if params.qcone_config_id is not None:
        logger.debug("Fetching QCOneResults associated with fetched datachunks")
        logger.info("QCOne will be used for selection of Datachunks for processing")
        fetched_qcone_results = db.session.query(QCOneResults.datachunk_id, QCOneResults).filter(
            QCOneResults.qcone_config_id == params.qcone_config_id,
            QCOneResults.datachunk_id.in_(fetched_datachunks_ids)).all()

        logger.debug(f"Fetched {len(fetched_qcone_results)} QCOneResults")
        for datachunk_id, qcone_results in fetched_qcone_results:
            valid_chunks[qcone_results.is_passing()].append(datachunk_id)
        logger.info(f"There were {len(valid_chunks[True])} valid QCOneResults. "
                    f"There were {len(valid_chunks[False])} invalid QCOneResults.")

    else:
        logger.info("QCOne is not used for selection of Datachunks. All fetched Datachunks will be processed.")
        valid_chunks[True].extend(fetched_datachunks_ids)

    for i, batch in enumerate(more_itertools.chunked(iterable=fetched_datachunks, n=batch_size)):
        if skip_existing:
            logger.info(f"Querying DB for existing ProcessedDatachunks. Batch no.{i}")
            batch_ids = extract_object_ids(batch)
            batch_existing_ids = (db.session.query(ProcessedDatachunk.datachunk_id)
                                  .filter(
                ProcessedDatachunk.processed_datachunk_params_id == params.id,
                ProcessedDatachunk.datachunk_id.in_(batch_ids)
            ).all())

            batch_ids.sort()
            batch_existing_ids.sort()

            if np.array_equal(np.array(batch_ids), np.array(batch_existing_ids)):
                logger.info("All queried datachunks are present in the db. Skipping this batch")
                continue
        else:
            batch_existing_ids = []

        logger.debug("Starting to check if each of input sets can be processed. Batch no.{i}")
        for chunk in batch:
            if skip_existing:
                exists = chunk.id in batch_existing_ids
            else:
                exists = False

            if all((chunk.id in valid_chunks[True], not skip_existing)) \
                    or all((chunk.id in valid_chunks[True], skip_existing, not exists)):

                logger.debug(f"There QCOneResult was True for datachunk with id {chunk.id}")
                db.session.expunge_all()
                yield ProcessDatachunksInputs(
                    datachunk=chunk,
                    params=params,
                    datachunk_file=None
                )
            elif chunk.id in valid_chunks[True] and skip_existing and exists:
                logger.debug(f"ProcessedDatachunk for datachunk with id {chunk.id} already exists.")
                continue
            elif chunk.id in valid_chunks[False]:
                logger.debug(f"There QCOneResult was False for datachunk with id {chunk.id}")
                continue
            else:
                logger.debug(f"There was no QCOneResult for datachunk with id {chunk.id}")
                continue
    return


def _prepare_upsert_command_processed_datachunk(proc_datachunk):
    insert_command = (
        insert(ProcessedDatachunk)
        .values(
            processed_datachunk_params_id=proc_datachunk.processed_datachunk_params_id,
            datachunk_id=proc_datachunk.datachunk_id,
            processed_datachunk_file_id=proc_datachunk.processed_datachunk_file_id,
        )
        .on_conflict_do_update(
            constraint="unique_processing_per_datachunk_per_config",
            set_=dict(processed_datachunk_file_id=proc_datachunk.processed_datachunk_file_id),
        )
    )
    return insert_command

import datetime
import itertools
from loguru import logger
import pendulum
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.dialects.postgresql import Insert, insert

from sqlalchemy.orm import subqueryload, Query
from typing import List, Iterable, Tuple, Collection, Optional, Dict, Union, Generator

from noiz.api.helpers import extract_object_ids, bulk_add_objects, _run_calculate_and_upsert_on_dask, \
    _run_calculate_and_upsert_sequentially
from noiz.api.component import fetch_components
from noiz.api.timeseries import fetch_raw_timeseries
from noiz.api.timespan import fetch_timespans_for_doy, fetch_timespans_between_dates
from noiz.api.processing_config import fetch_datachunkparams_by_id, fetch_processed_datachunk_params_by_id
from noiz.database import db
from noiz.exceptions import NoDataException
from noiz.models import (
    AveragedSohGps,
    Component,
    Datachunk,
    DatachunkParams,
    DatachunkStats,
    ProcessedDatachunk,
    ProcessedDatachunkParams,
    QCOneConfig,
    QCOneResults,
    Timespan
)
from noiz.processing.datachunk import create_datachunks_for_component, calculate_datachunk_stats, \
    create_datachunks_for_component_wrapper, calculate_datachunk_stats_wrapper
from noiz.api.type_aliases import CalculateDatachunkStatsInputs, RunDatachunkPreparationInputs, ProcessDatachunksInputs
from noiz.processing.datachunk_processing import process_datachunk, process_datachunk_wrapper


def count_datachunks(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_processing_config: Optional[DatachunkParams] = None,
        datachunk_ids: Optional[Collection[int]] = None,
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
    :param datachunk_processing_config: DatachunkParams to be checked. This have to be a single object.
    :type datachunk_processing_config: Optional[DatachunkParams]
    :param datachunk_ids: Ids of Datachunk objects to be fetched
    :type datachunk_ids: Optional[Collection[int]]
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
        datachunk_processing_config=datachunk_processing_config,
        datachunk_ids=datachunk_ids,
        load_component=load_component,
        load_stats=load_stats,
        load_timespan=load_timespan,
        load_processing_params=load_processing_params,
    )

    return query.count()


def fetch_datachunks(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_processing_config: Optional[DatachunkParams] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
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
    :param datachunk_processing_config: DatachunkParams to be checked. This have to be a single object.
    :type datachunk_processing_config: Optional[DatachunkParams]
    :param datachunk_ids: Ids of Datachunk objects to be fetched
    :type datachunk_ids: Optional[Collection[int]]
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
    :return: List of Datachunks loaded from DB
    :rtype: List[Datachunk]
    """

    query = _query_datachunks(
        components=components,
        timespans=timespans,
        datachunk_processing_config=datachunk_processing_config,
        datachunk_ids=datachunk_ids,
        load_component=load_component,
        load_stats=load_stats,
        load_timespan=load_timespan,
        load_processing_params=load_processing_params,
    )

    return query.all()


def fetch_datachunks_without_stats(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_processing_config: Optional[DatachunkParams] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
) -> List[Datachunk]:
    query = _query_datachunks(
        components=components,
        timespans=timespans,
        datachunk_processing_config=datachunk_processing_config,
        datachunk_ids=datachunk_ids,
        load_component=load_component,
        load_stats=load_stats,
        load_timespan=load_timespan,
        load_processing_params=load_processing_params,
    )
    return query.filter(~Datachunk.stats.has()).all()


def query_datachunks_without_qcone(
        qc_one: QCOneConfig,
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_processing_config: Optional[DatachunkParams] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
) -> Query:

    filters, opts = _determine_filters_and_opts_for_datachunk(
        components=components,
        datachunk_ids=datachunk_ids,
        datachunk_processing_config=datachunk_processing_config,
        load_component=load_component,
        load_processing_params=load_processing_params,
        load_stats=load_stats,
        load_timespan=load_timespan,
        timespans=timespans,
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
        datachunk_processing_config: Optional[DatachunkParams] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
) -> Query:

    filters, opts = _determine_filters_and_opts_for_datachunk(
        components=components,
        datachunk_ids=datachunk_ids,
        datachunk_processing_config=datachunk_processing_config,
        load_component=load_component,
        load_processing_params=load_processing_params,
        load_stats=load_stats,
        load_timespan=load_timespan,
        timespans=timespans,
    )

    return Datachunk.query.filter(*filters).options(opts)


def _determine_filters_and_opts_for_datachunk(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_processing_config: Optional[DatachunkParams] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
        load_stats: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,
) -> Tuple[List, List]:

    filters = []
    if components is not None:
        component_ids = extract_object_ids(components)
        filters.append(Datachunk.component_id.in_(component_ids))
    if timespans is not None:
        timespan_ids = extract_object_ids(timespans)
        filters.append(Datachunk.timespan_id.in_(timespan_ids))
    if datachunk_processing_config is not None:
        filters.append(Datachunk.datachunk_params_id == datachunk_processing_config.id)
    if datachunk_ids is not None:
        filters.append(Datachunk.id.in_(datachunk_ids))
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

    return filters, opts


def add_or_upsert_datachunks_in_db(datachunks: Iterable[Datachunk]):
    """
    Adds or upserts provided iterable of Datachunks to DB.
    Must be executed within AppContext.

    :param datachunks:
    :type datachunks: Iterable[Datachunk]
    :return:
    :rtype:
    """
    for datachunk in datachunks:

        if not isinstance(datachunk, Datachunk):
            logger.warning(f'Provided object is not an instance of Datachunk. '
                           f'Provided object was an {type(datachunk)}. Skipping.')
            continue

        logger.info("Querying db if the datachunk already exists.")
        existing_chunks = (
            db.session.query(Datachunk)
                      .filter(
                Datachunk.component_id == datachunk.component_id,
                Datachunk.timespan_id == datachunk.timespan_id,
            )
            .all()
        )

        if len(existing_chunks) == 0:
            logger.info("No existing chunks found. Adding Datachunk to DB.")
            db.session.add(datachunk)
        else:
            logger.info("The datachunk already exists in db. Updating.")
            insert_command = _prepare_upsert_command_datachunk(datachunk)
            db.session.execute(insert_command)

    logger.debug('Committing session.')
    db.session.commit()
    return


def _prepare_upsert_command_datachunk(datachunk: Datachunk) -> Insert:
    insert_command = (
        insert(Datachunk)
        .values(
            processing_config_id=datachunk.datachunk_params_id,
            component_id=datachunk.component_id,
            timespan_id=datachunk.timespan_id,
            sampling_rate=datachunk.sampling_rate,
            npts=datachunk.npts,
            datachunk_file=datachunk.datachunk_file,
            padded_npts=datachunk.padded_npts,
        )
        .on_conflict_do_update(
            constraint="unique_datachunk_per_timespan_per_station_per_processing",
            set_=dict(
                datachunk_file_id=datachunk.datachunk_file.id,
                padded_npts=datachunk.padded_npts),
        )
    )
    return insert_command


def _prepare_datachunk_preparation_parameter_lists(
        stations: Optional[Tuple[str]],
        components: Optional[Tuple[str]],
        startdate: pendulum.Pendulum,
        enddate: pendulum.Pendulum,
        processing_config_id: int,
        skip_existing: bool = False,
) -> Generator[RunDatachunkPreparationInputs, None, None]:
    date_period = pendulum.period(startdate, enddate)

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

        if not skip_existing:
            logger.info("Checking if some timespans already exists")
            existing_count = count_datachunks(
                components=(component,),
                timespans=timespans,
                datachunk_processing_config=processing_params,
            )
            if existing_count == len(timespans):
                logger.info("Number of existing timespans is sufficient. Skipping")
                continue

            logger.info(f"There are only {existing_count} existing Datachunks. "
                        f"Looking for those that are missing one by one.")
            new_timespans = [timespan for timespan in timespans if
                             count_datachunks(
                                 components=(component,),
                                 timespans=(timespan,),
                                 datachunk_processing_config=processing_params
                             ) == 0]
            timespans = new_timespans

        yield RunDatachunkPreparationInputs(
            component=component,
            timespans=timespans,
            time_series=time_series,
            processing_params=processing_params,
        )


def run_datachunk_preparation(
        stations: Tuple[str],
        components: Tuple[str],
        startdate: pendulum.Pendulum,
        enddate: pendulum.Pendulum,
        processing_config_id: int,

):
    logger.info("Preparing jobs for execution")
    joblist = _prepare_datachunk_preparation_parameter_lists(stations,
                                                             components,
                                                             startdate, enddate,
                                                             processing_config_id)

    # TODO add more checks for bad seed files because they are crashing.
    # And instead of datachunk id there was something weird produced. It was found on SI26 in
    # 2019.04.~10-15

    logger.info("Starting execution. Results will be saved to database after everything is done.")
    datachunks: List[Datachunk] = []
    for params in joblist:
        try:
            finished_chunks = create_datachunks_for_component(**params)
            datachunks.extend(finished_chunks)
        except ValueError as e:
            logger.error(e)
            raise e

    logger.info(f"There were {len(datachunks)} created")
    logger.info("Adding datachunks to db")

    add_or_upsert_datachunks_in_db(datachunks)
    return


def run_datachunk_preparation_parallel(
        stations: Tuple[str],
        components: Tuple[str],
        startdate: pendulum.Pendulum,
        enddate: pendulum.Pendulum,
        processing_config_id: int,
        parallel: bool = True,
        batch_size: int = 1000,

):
    logger.info("Preparing jobs for execution")
    calculation_inputs = _prepare_datachunk_preparation_parameter_lists(stations,
                                                                        components,
                                                                        startdate, enddate,
                                                                        processing_config_id)

    # TODO add more checks for bad seed files because they are crashing.
    # And instead of datachunk id there was something weird produced. It was found on SI26 in
    # 2019.04.~10-15

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=create_datachunks_for_component_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_datachunk,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=create_datachunks_for_component_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_datachunk,
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
        datachunk_processing_config=fetched_datachunk_params
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
):
    # filldocs
    datachunks_to_process = _select_datachunks_for_processing(
        processed_datachunk_params_id=processed_datachunk_params_id,
        starttime=starttime,
        endtime=endtime,
        networks=networks,
        stations=stations,
        components=components,
        component_ids=component_ids,
    )

    processed_datachunks = []
    for jobkwargs in datachunks_to_process:
        processed_datachunks.append(process_datachunk(**jobkwargs))

    add_or_upsert_processed_datachunks_in_db(processed_datachunks=processed_datachunks)

    return


def run_datachunk_processing_parallel(
        processed_datachunk_params_id: int,
        starttime: Union[datetime.date, datetime.datetime],
        endtime: Union[datetime.date, datetime.datetime],
        networks: Optional[Union[Collection[str], str]] = None,
        stations: Optional[Union[Collection[str], str]] = None,
        components: Optional[Union[Collection[str], str]] = None,
        component_ids: Optional[Union[Collection[int], int]] = None,
        batch_size: int = 2000,
        parallel: bool = True,

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
    )

    if parallel:
        _run_calculate_and_upsert_on_dask(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=process_datachunk_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_processed_datachunk,
        )
    else:
        _run_calculate_and_upsert_sequentially(
            batch_size=batch_size,
            inputs=calculation_inputs,
            calculation_task=process_datachunk_wrapper,  # type: ignore
            upserter_callable=_prepare_upsert_command_processed_datachunk,
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
        datachunk_processing_config=params.datachunk_params,
        load_timespan=True,
        load_component=True,
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

    for chunk in fetched_datachunks:
        if chunk.id in valid_chunks[True]:
            logger.debug(f"There QCOneResult was True for datachunk with id {chunk.id}")
            yield {"datachunk": chunk, "params": params}

        elif chunk.id in valid_chunks[False]:
            logger.debug(f"There QCOneResult was False for datachunk with id {chunk.id}")
            continue
        else:
            logger.debug(f"There was no QCOneResult for datachunk with id {chunk.id}")
            continue


def add_or_upsert_processed_datachunks_in_db(
        processed_datachunks: Union[ProcessedDatachunk, Collection[ProcessedDatachunk]],
        bulk_insert: bool = True
):
    """
    Adds or upserts provided iterable of ProcessedDatachunk to DB.
    Must be executed within AppContext.

    :param processed_datachunks:
    :type processed_datachunks: Union[ProcessedDatachunk, Iterable[ProcessedDatachunk]]
    :param bulk_insert: If bulk insert should be even attempted
    :type bulk_insert: bool
    :return:
    :rtype:
    """
    valid_processed_datachunks: Collection[ProcessedDatachunk]

    if isinstance(processed_datachunks, ProcessedDatachunk):
        valid_processed_datachunks = (processed_datachunks,)
    else:
        valid_processed_datachunks = processed_datachunks

    if bulk_insert:
        logger.info("Trying to do bulk insert")
        try:
            bulk_add_objects(valid_processed_datachunks)
        except (IntegrityError, UnmappedInstanceError) as e:
            logger.warning(f"There was an integrity error thrown. {e}. Performing rollback.")
            db.session.rollback()

            logger.warning("Retrying with upsert")
            upsert_processed_datachunks(valid_processed_datachunks)
    else:
        logger.info(f"Starting to perform careful upsert. There are {len(valid_processed_datachunks)} to insert")
        upsert_processed_datachunks(valid_processed_datachunks)

    return


def upsert_processed_datachunks(processed_datachunks):
    for proc_datachunk in processed_datachunks:
        if not isinstance(proc_datachunk, ProcessedDatachunk):
            logger.info(f"Got {type(proc_datachunk)} and not ProcessedDatachunk. Skipping")
            continue
        logger.info("ProcessedDatachunks already exists in db. Updating.")
        insert_command = _prepare_upsert_command_processed_datachunk(proc_datachunk)
        db.session.execute(insert_command)
        logger.debug('Committing session.')
    db.session.commit()


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

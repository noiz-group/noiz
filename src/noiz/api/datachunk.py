import datetime
import logging
import pendulum
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import subqueryload
from typing import List, Iterable, Tuple, Collection, Optional, Dict

import itertools

from noiz.api.helpers import extract_object_ids
from noiz.api.component import fetch_components
from noiz.api.timeseries import fetch_raw_timeseries
from noiz.api.timespan import fetch_timespans_for_doy
from noiz.api.processing_config import fetch_processing_config_by_id
from noiz.database import db
from noiz.exceptions import NoDataException
from noiz.models.component import Component
from noiz.models.datachunk import Datachunk
from noiz.models.processing_params import DatachunkParams
from noiz.models.timespan import Timespan
from noiz.processing.datachunk import create_datachunks_for_component

from loguru import logger as log


def fetch_datachunks_for_timespan(
        timespans: Collection[Timespan]
) -> List[Datachunk]:
    """
    DEPRECATED. Use noiz.api.datachunkfetch_datachunks instead

    Fetches all datachunks associated with provided timespans.
    Timespan can be a single one or Iterable of timespans.

    :param timespans: Instances of timespans to be checked
    :type timespans: Collection[Timespan]
    :return: List of Datachunks
    :rtype: List[Datachunk]
    """
    log.warning("Method deprected. Use noiz.api.datachunkfetch_datachunks instead.")
    return fetch_datachunks(timespans=timespans)


def count_datachunks(
        components: Collection[Component],
        timespans: Collection[Timespan],
        datachunk_processing_params: DatachunkParams,
) -> int:
    """
    Counts number of datachunks for all provided components associated with
    all provided timespans.

    :param components: Components to be checked
    :type components: Iterable[Component]
    :param timespans: Timespans to be checked
    :type timespans: Iterable[Timespan]
    :param datachunk_processing_params: DatachunkParams to be checked. \
        This have to be a single object.
    :type datachunk_processing_params: DatachunkParams
    :return: Count fo datachunks
    :rtype: int
    """
    # FIXME noiz-group/noiz#45
    timespan_ids = extract_object_ids(timespans)
    component_ids = extract_object_ids(components)
    count = Datachunk.query.filter(
        Datachunk.component_id.in_(component_ids),
        Datachunk.timespan_id.in_(timespan_ids),
        Datachunk.datachunk_params_id == datachunk_processing_params.id
    ).count()
    return count


def fetch_datachunks(
        components: Optional[Collection[Component]] = None,
        timespans: Optional[Collection[Timespan]] = None,
        datachunk_processing_config: Optional[DatachunkParams] = None,
        datachunk_ids: Optional[Collection[int]] = None,
        load_component: bool = False,
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
    :param components: Ids of Datachunk objects to be fetched
    :type components: Optional[Collection[int]]
    :param load_component: Loads also the associated Component object so it is available for usage \
    without context
    :type load_component: bold
    :param load_timespan: Loads also the associated Timespan object so it is available for usage \
    without context
    :type load_timespan: bool
    :param load_processing_params: Loads also the associated DatachunkParams object \
    so it is available for usage without context
    :type load_processing_params: bool
    :return: List of Datachunks loaded from DB/
    :rtype: List[Datachunk]
    """

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
    if load_component:
        opts.append(subqueryload(Datachunk.component))
    if load_processing_params:
        opts.append(subqueryload(Datachunk.processing_params))

    return Datachunk.query.filter(*filters).options(opts).all()


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
            log.warning(f'Provided object is not an instance of Datachunk. '
                        f'Provided object was an {type(datachunk)}. Skipping.')
            continue

        log.info("Querrying db if the datachunk already exists.")
        existing_chunks = (
            db.session.query(Datachunk)
                      .filter(
                        Datachunk.component_id == datachunk.component_id,
                        Datachunk.timespan_id == datachunk.timespan_id,
                      )
              .all()
        )

        if len(existing_chunks) == 0:
            log.info("No existing chunks found. Adding Datachunk to DB.")
            db.session.add(datachunk)
        else:
            log.info("The datachunk already exists in db. Updating.")
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
            db.session.execute(insert_command)

    log.debug('Commiting session.')
    db.session.commit()
    return


def create_datachunks_add_to_db(
        execution_date: datetime.datetime,
        component: Component,
        timespans: Collection[Timespan],
        processing_params: DatachunkParams,
        processed_data_dir: Path,
) -> None:
    no_datachunks = count_datachunks(
        components=(component,),
        timespans=timespans,
        datachunk_processing_params=processing_params
    )

    timespans_count = len(timespans)

    log.info(
        f"There are {no_datachunks} datachunks for {execution_date} in db")

    if no_datachunks == timespans_count:
        log.info("There is enough of datachunks in the db (no_datachunks == no_timespans)")
        return

    log.info(f"Fetching timeseries for {execution_date} {component}")
    try:
        time_series = fetch_raw_timeseries(
            component=component, execution_date=execution_date
        )
    except NoDataException as e:
        log.error(e)
        raise e

    finished_datachunks = create_datachunks_for_component(component=component,
                                                          timespans=timespans,
                                                          time_series=time_series,
                                                          processing_params=processing_params)
    add_or_upsert_datachunks_in_db(finished_datachunks)

    return


def prepare_datachunk_preparation_parameter_lists(
        stations: Optional[Tuple[str]],
        components: Optional[Tuple[str]],
        startdate: pendulum.Pendulum,
        enddate: pendulum.Pendulum,
        processing_config_id: int,
        skip_existing: bool = False,
) -> Iterable[Dict]:
    date_period = pendulum.period(startdate, enddate)

    log.info("Fetching processing config, timespans and componsents from db. ")
    processing_params = fetch_processing_config_by_id(id=processing_config_id)

    all_timespans = [(date, fetch_timespans_for_doy(
        year=date.year, doy=date.day_of_year
    )) for date in date_period.range('days')]

    fetched_components = fetch_components(networks=None,
                                          stations=stations,
                                          components=components)

    for component, (date, timespans) in itertools.product(fetched_components,
                                                          all_timespans):

        log.info(f"Looking for data on {date} for {component}")

        try:
            time_series = fetch_raw_timeseries(
                component=component, execution_date=date
            )
        except NoDataException as e:
            log.warning(f"{e} Skipping.")
            continue

        if not skip_existing:
            log.info("Checking if some timespans already exists")
            existing_count = count_datachunks(
                components=(component,),
                timespans=timespans,
                datachunk_processing_params=processing_params,
            )
            if existing_count == len(timespans):
                log.info("Number of existing timespans is sufficient. Skipping")
                continue

            log.info(f"There are only {existing_count} existing Datachunks. "
                     f"Looking for those that are missing one by one.")
            new_timespans = [timespan for timespan in timespans if
                             count_datachunks(
                                 components=(component,),
                                 timespans=(timespan,),
                                 datachunk_processing_params=processing_params
                             ) == 0]
            timespans = new_timespans

        yield {
            'component': component,
            'timespans': timespans,
            'time_series': time_series,
            'processing_params': processing_params,
        }


def run_paralel_chunk_preparation(
        stations: Tuple[str],
        components: Tuple[str],
        startdate: pendulum.Pendulum,
        enddate: pendulum.Pendulum,
        processing_config_id: int,

):
    log.info("Preparing jobs for execution")
    joblist = prepare_datachunk_preparation_parameter_lists(stations,
                                                            components,
                                                            startdate, enddate,
                                                            processing_config_id)

    # TODO add more checks for bad seed files because they are crashing.
    # And instead of datachunk id there was something weird produced. It was found on SI26 in
    # 2019.04.~10-15

    from dask.distributed import Client, as_completed
    client = Client()

    log.info(f'Dask client started succesfully. '
             f'You can monitor execution on {client.dashboard_link}')

    log.info("Submitting tasks to Dask client")
    futures = []
    for params in joblist:
        future = client.submit(create_datachunks_for_component, **params)
        futures.append(future)

    log.info(f"There are {len(futures)} tasks to be executed")

    log.info("Starting execution. Results will be saved to database on the fly. ")

    for future, result in as_completed(futures, with_results=True, raise_errors=False):
        add_or_upsert_datachunks_in_db(result)

    client.close()
    # TODO Add summary printout.


def run_chunk_preparation(
        app, station, component, execution_date, processed_data_dir, processing_config_id=1
):
    year = execution_date.year
    day_of_year = execution_date.timetuple().tm_yday

    log.info("Fetching processing config, timespans and componsents from db")
    with app.app_context():
        processing_config = (
            db.session.query(DatachunkParams)
                      .filter(DatachunkParams.id == processing_config_id)
                      .first()
        )
        timespans = fetch_timespans_for_doy(year=year, doy=day_of_year)

        components = Component.query.filter(
            Component.station == station, Component.component == component
        ).all()

    log.info("Invoking chunc creation itself")
    for component in components:
        with app.app_context():
            create_datachunks_for_component(component=component,
                                            timespans=timespans,
                                            time_series=None,  # type: ignore
                                            processing_params=processing_config)
    return

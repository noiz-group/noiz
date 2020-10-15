import datetime
import logging
import obspy
import pendulum
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import subqueryload
from typing import List, Iterable, Union, Tuple, Collection, Optional, Dict

import itertools

from noiz.api.helpers import extract_object_ids
from noiz.api.component import fetch_components
from noiz.api.timeseries import fetch_raw_timeseries
from noiz.api.timespan import fetch_timespans_for_doy
from noiz.api.processing_config import fetch_processing_config_by_id
from noiz.database import db
from noiz.exceptions import NoDataException, MissingDataFileException
from noiz.globals import PROCESSED_DATA_DIR
from noiz.models import Datachunk, Component, Timespan, ProcessingParams,\
    Tsindex, DatachunkFile

from noiz.processing.datachunk_preparation import  validate_slice, \
    preprocess_timespan, assembly_filepath, assembly_sds_like_dir, \
    assembly_preprocessing_filename, increment_filename_counter, \
    directory_exists_or_create

log = logging.getLogger("noiz.api")
# log = logging.getLogger(__name__)


def fetch_datachunks_for_timespan(
        timespans: Union[Timespan, Iterable[Timespan]]
) -> List[Datachunk]:
    """
    Fetches all datachunks associated with provided timespans. Timespan can be a single one or Iterable of timespans.
    :param timespans: Instances of timespans to be checked
    :type timespans: Union[Timespan, Iterable[Timespan]]
    :return: List of Datachunks
    :rtype: List[Datachunk]
    """
    timespan_ids = extract_object_ids(timespans)
    ret = Datachunk.query.filter(Datachunk.timespan_id.in_(timespan_ids)).all()
    return ret


def count_datachunks_for_timespans_and_components(
        components: Collection[Component],
        timespans: Collection[Timespan],
        processing_params: ProcessingParams,
) -> int:
    """
    Counts number of datachunks for all provided components associated with all provided timespans.

    :param components: Components to be checked
    :type components: Iterable[Component]
    :param timespans: Timespans to be checked
    :type timespans: Iterable[Timespan]
    :param processing_params: ProcessingParams to be checked.
    This have to be a single object.
    :type processing_params: ProcessingParams
    :return: Count fo datachunks
    :rtype: int
    """
    timespan_ids = extract_object_ids(timespans)
    component_ids = extract_object_ids(components)
    count = Datachunk.query.filter(
        Datachunk.component_id.in_(component_ids),
        Datachunk.timespan_id.in_(timespan_ids),
        Datachunk.processing_params_id == processing_params.id
    ).count()
    return count


def fetch_datachunks_for_timespans_and_components(
        components: Collection[Component],
        timespans: Collection[Timespan],
        processing_params: ProcessingParams,
        load_component: bool = False,
        load_timespan: bool = False,
        load_processing_params: bool = False,

) -> Collection[Datachunk]:
    """

        Fetches datachunks for all provided components associated with all provided timespans.

    :param components: Components to be checked
    :type components: Collection[Component]
    :param timespans: Timespans to be checked
    :type timespans: Collection[Timespan]
    :param processing_params: ProcessingParams to be checked.
    This have to be a single object.
    :type processing_params: ProcessingParams
    :param load_component: Loads also the associated Component
    object so it is available for usage without context
    :type load_component: bold
    :param load_timespan: Loads also the associated Timespan
    object so it is available for usage without context
    :type load_timespan: bool
    :param load_processing_params: Loads also the associated ProcessingParams
    object so it is available for usage without context
    :type load_processing_params: bool
    :return: Collection of Datachunks
    :rtype: Collection[Datachunk]
    """
    timespan_ids = extract_object_ids(timespans)
    component_ids = extract_object_ids(components)

    opts = []

    if load_timespan:
        opts.append(subqueryload(Datachunk.timespan))
    if load_component:
        opts.append(subqueryload(Datachunk.component))
    if load_processing_params:
        opts.append(subqueryload(Datachunk.processing_params))


    datachunks = Datachunk.query.filter(
        Datachunk.component_id.in_(component_ids),
        Datachunk.timespan_id.in_(timespan_ids),
        Datachunk.processing_params_id == processing_params.id
    ).options(opts).all()
    return datachunks


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
                        f'Provided object was an {type(datachunk)}. '
                        f'Skipping.')
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
            log.info("No existing chunks found. "
                     "Adding Datachunk to DB.")
            db.session.add(datachunk)
        else:
            log.info("The datachunk already exists in db. Updating.")
            insert_command = (
                insert(Datachunk)
                    .values(
                    processing_config_id=datachunk.processing_params_id,
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
        processing_params: ProcessingParams,
        processed_data_dir: Path,
) -> None:
    no_datachunks = count_datachunks_for_timespans_and_components((component,),
                                                                  timespans)

    timespans_count = len(timespans)

    log.info(
        f"There are {no_datachunks} datachunks for {execution_date} in db")

    if no_datachunks == timespans_count:
        log.info(
            f"There is enough of datachunks in the db "
            f"(no_datahcunks == no_timespans)"
        )
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


def create_datachunks_for_component(
        component: Component,
        timespans: Collection[Timespan],
        time_series: Tsindex,
        processing_params: ProcessingParams
) -> Collection[Datachunk]:
    """
    All around method that is takes prepared Component, Tsindex,
    ProcessingParams and bunch of Timespans to slice the continuous seed file
    into shorter one, reflecting all the Timespans.
    It saves the file to the drive but it doesn't add entry to DB.

    Returns collection of Datachunks with DatachunkFile associated to it,
    ready to be added to DB.

    :param component:
    :type component: Component
    :param timespans: Timespans on base of which you want your
    datachunks to be created.
    :type timespans: Collection[Timespans]
    :param time_series: Tsindex object that hs information about
    location of continuous seed file
    :type time_series: Tsindex
    :param processing_params:
    :type processing_params: ProcessingParams
    :return: Datachunks ready to be sent to DB.
    :rtype: Collection[Datachunk]
    """

    log.info("Reading timeseries and inventory")
    try:
        st: obspy.Stream = time_series.read_file()
    except MissingDataFileException as e:
        log.warning(f"Data file is missing. Skipping. {e}")
        return []
    except Exception as e:
        log.warning(f"There was some general exception from "
                    f"obspy.Stream.read function. Here it is: {e} ")
        return []


    inventory: obspy.Inventory = component.read_inventory()

    # log.info("Preprocessing initially full day timeseries")
    # st = preprocess_whole_day(st, processing_params)

    finished_datachunks = []

    log.info(f"Splitting full day into timespans for {component}")
    for timespan in timespans:

        log.info(f"Slicing timespan {timespan}")
        trimed_st: obspy.Trace = st.slice(
            starttime=timespan.starttime_obspy(),
            endtime=timespan.remove_last_microsecond(),
            nearest_sample=False,
        )

        try:
            trimed_st, padded_npts = validate_slice(
                trimed_st=trimed_st,
                timespan=timespan,
                processing_params=processing_params,
                raw_sps=float(time_series.samplerate)
            )
        except ValueError as e:
            log.warning(f"There was a problem with trace validation. "
                        f"There was raised exception {e}")
            continue

        log.info("Preprocessing timespan")
        trimed_st = preprocess_timespan(
            trimed_st=trimed_st,
            inventory=inventory,
            processing_params=processing_params,
        )

        filepath = assembly_filepath(
            PROCESSED_DATA_DIR, # type: ignore
            "datachunk",
            assembly_sds_like_dir(component, timespan) \
                .joinpath(
                assembly_preprocessing_filename(
                    component=component,
                    timespan=timespan,
                    count=0
                )
            ),
        )

        if filepath.exists():
            log.info(f'Filepath {filepath} exists. '
                     f'Trying to find next free one.')
            filepath = increment_filename_counter(filepath=filepath)
            log.info(f"Free filepath found. "
                     f"Datachunk will be saved to {filepath}")

        log.info(f"Chunk will be written to {str(filepath)}")
        directory_exists_or_create(filepath)

        datachunk_file = DatachunkFile(filepath=str(filepath))
        trimed_st.write(datachunk_file.filepath, format="mseed")

        datachunk = Datachunk(
            processing_params_id=processing_params.id,
            component_id=component.id,
            timespan_id=timespan.id,
            sampling_rate=trimed_st[0].stats.sampling_rate,
            npts=trimed_st[0].stats.npts,
            datachunk_file=datachunk_file,
            padded_npts=padded_npts,
        )
        log.info(
            "Checking if there are some chunks fot tht timespan and component in db"
        )

        finished_datachunks.append(datachunk)

    return finished_datachunks


def prepare_datachunk_preparation_parameter_lists(
        stations: Optional[Tuple[str]],
        components: Optional[Tuple[str]],
        startdate: pendulum.Pendulum,
        enddate: pendulum.Pendulum,
        processing_config_id: int,
        skip_existing: bool = False,
) -> Iterable[Dict]:
    date_period = pendulum.period(startdate, enddate)

    log.info(
        f"Fetching processing config, timespans and componsents from db")
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
            log.info(f"Checking if some timespans already exists")
            existing_count = count_datachunks_for_timespans_and_components(
                components=(component,),
                timespans=timespans
            )
            if existing_count == len(timespans):
                log.info('Number of existing timespans is sufficient. '
                         'Skipping')
                continue

            log.info(f"There are only {existing_count} existing Datachunks. "
                     f"Looking for those that are missing one by one.")
            new_timespans = [timespan for timespan in timespans if
                             count_datachunks_for_timespans_and_components(
                                 components=(component,),
                                 timespans=(timespan,)
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
    log.info(f"Preparing jobs for execution")
    joblist = prepare_datachunk_preparation_parameter_lists(stations,
                                                            components,
                                                            startdate, enddate,
                                                            processing_config_id)

    # TODO add more checks for bad seed files because they are crashing.
    # And instead of datachunk id there was something weird produced. It was found on SI26 in 2019.04.~10-15

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

    log.info("Starting execution. "
             "Results will be saved to database on the fly. ")

    for future, result in as_completed(futures, with_results=True, raise_errors=False):
        add_or_upsert_datachunks_in_db(result)

    client.close()
        # TODO Add summary printout.


def run_chunk_preparation(
    app, station, component, execution_date, processed_data_dir, processing_config_id=1
):
    year = execution_date.year
    day_of_year = execution_date.timetuple().tm_yday

    log.info(f"Fetching processing config, timespans and componsents from db")
    with app.app_context() as ctx:
        processing_config = (
            db.session.query(ProcessingParams)
            .filter(ProcessingParams.id == processing_config_id)
            .first()
        )
        timespans = fetch_timespans_for_doy(year=year, doy=day_of_year)

        components = Component.query.filter(
            Component.station == station, Component.component == component
        ).all()

    log.info(f"Invoking chunc creation itself")
    for component in components:
        with app.app_context() as ctx:
            create_datachunks_for_component(component=component,
                                            timespans=timespans,
                                            time_series=None, # type: ignore
                                            processing_params=processing_config)
    return
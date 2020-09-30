import datetime
import logging
import obspy
import pendulum
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from typing import List, Iterable, Union, Sized, Collection, Optional, Dict

import itertools

from noiz.api.component import fetch_components
from noiz.api.timeseries import fetch_raw_timeseries
from noiz.api.timespan import fetch_timespans_for_doy
from noiz.api.processing_config import fetch_processing_config_by_id
from noiz.database import db
from noiz.exceptions import NoDataException
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
        components: Collection[Component], timespans: Collection[Timespan]
) -> int:
    """
    Counts number of datachunks for all provided components associated with all provided timespans.

    :param components: Components to be checked
    :type components: Iterable[Component]
    :param timespans: Timespans to be checked
    :type timespans: Iterable[Timespan]
    :return: Count fo datachunks
    :rtype: int
    """
    timespan_ids = extract_object_ids(timespans)
    component_ids = extract_object_ids(components)
    count = Datachunk.query.filter(
        Datachunk.component_id.in_(component_ids),
        Datachunk.timespan_id.in_(timespan_ids),
    ).count()
    return count


def fetch_datachunks_for_timespans_and_components(
        components: Collection[Component], timespans: Collection[Timespan]
) -> Collection[Datachunk]:
    """
    Fetches datachunks for all provided components associated with all provided timespans.

    :param components: Components to be checked
    :type components: Iterable[Component]
    :param timespans: Timespans to be checked
    :type timespans: Iterable[Timespan]
    :return: Collection of Datachunks
    :rtype: Collection[Datachunk]
    """
    timespan_ids = extract_object_ids(timespans)
    component_ids = extract_object_ids(components)
    datachunks = Datachunk.query.filter(
        Datachunk.component_id.in_(component_ids),
        Datachunk.timespan_id.in_(timespan_ids),
    ).all()
    return datachunks


def extract_object_ids(instances: Iterable[Union[Timespan, Component]]) -> \
        List[int]:
    """
    Extracts parameter .id from all provided instances of objects. It can either be a single object or iterbale of them.
    :param instances: instances of objects to be checked
    :type instances:
    :return: ids of objects
    :rtype: List[int]
    """
    if not isinstance(instances, Iterable):
        instances = list(instances)
    ids = [x.id for x in instances]
    return ids


def add_or_upsert_datachunks_in_db(datachunks):
    for datachunk in datachunks:

        existing_chunks = (
            db.session.query(Datachunk)
                .filter(
                Datachunk.component_id == datachunk.component_id,
                Datachunk.timespan_id == datachunk.timespan_id,
            )
                .all()
        )
        log.info(
            "Checking if there are some timeseries files  for tht timespan and component on the disc"
        )
        if len(existing_chunks) == 0:
            log.info("Writing file to disc and adding entry to db")
            db.session.add(datachunk)
        else:
            if not Path(datachunk.datachunk_file.filepath).exists():
                log.info(
                    "There is some chunk in the db so I will update it and write/overwrite file to the disc."
                )
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
    db.session.commit()
    return


def run_paralel_chunk_preparation(
        stations: Collection[str],
        components: Collection[str],
        startdate: pendulum.Pendulum,
        enddate: pendulum.Pendulum,
        processing_config_id: int,

):
    log.info(f"Preparing jobs for execution")
    joblist = prepare_datachunk_preparation_parameter_lists(stations,
                                                            components,
                                                            startdate, enddate,
                                                            processing_config_id)

    import dask
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

    for future, result in as_completed(futures, with_results=True):
        add_or_upsert_datachunks_in_db(result)

    client.close()
    # TODO Add summary printout.


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
    st: obspy.Stream = time_series.read_file()
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
        except ValueError:
            continue

        log.info("Preprocessing timespan")
        trimed_st: obspy.Stream = preprocess_timespan(
            trimed_st=trimed_st,
            inventory=inventory,
            processing_params=processing_params,
        )

        filepath = assembly_filepath(
            PROCESSED_DATA_DIR,
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
        stations: Optional[Collection[str]],
        components: Optional[Collection[str]],
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
                                            time_series=None,
                                            processing_params=processing_config)
    return
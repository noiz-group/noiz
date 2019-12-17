import datetime
import logging
import os
from collections import Collection

from multiprocessing import Pool
from pathlib import Path
from typing import Union, Iterable, Sized, Optional, Tuple

import numpy as np
import obspy
import pandas as pd

from sqlalchemy.dialects.postgresql import insert

from noiz.api.datachunk import count_datachunks_for_timespans
from noiz.api.timespan import fetch_timespans_for_doy
from noiz.api.timeseries import fetch_raw_timeseries
from noiz.database import db
from noiz.exceptions import NoDataException
from noiz.models import Component, Datachunk, Timespan, ProcessingParams


def expected_npts(timespan_length: float, sampling_rate: float) -> int:
    """
    Calculates expected number of npts in a trace based on timespan length and sampling rate.
    Casted to int with floor rounding.
    :param timespan_length: Length of timespan in seconds
    :type timespan_length: float
    :param sampling_rate: Sampling rate in samples per second
    :type sampling_rate: float
    :return: Number of samples in the timespan
    :rtype: int
    """
    return int(timespan_length * sampling_rate)


def assembly_preprocessing_filename(component, timespan):
    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    fname = ".".join(
        [component.network, component.station, component.component, year, doy_time]
    )

    return fname


def assembly_sds_like_dir(component: Component, timespan: Timespan) -> Path:
    """
    Asembles a Path object in a SDS manner. Object consists of year/network/station/component codes.
    Warning: The component here is a single letter component!
    :param component: Component object containing information about used channel
    :type component: Component
    :param timespan: Timespan object containing information about time
    :type timespan: Timespan
    :return:  Pathlike object containing SDS-like directory hierarchy.
    :rtype: Path
    """
    return (
        Path(str(timespan.starttime.year))
        .joinpath(component.network)
        .joinpath(component.station)
        .joinpath(component.component)
    )


def assembly_filepath(
    processed_data_dir: Union[str, Path],
    processing_type: Union[str, Path],
    filepath: Union[str, Path],
) -> Path:
    """
    Assembles a filepath for processed files.
    It assembles a root processed-data directory with processing type and a rest of a filepath.
    :param processed_data_dir:
    :type processed_data_dir: Path
    :param processing_type:
    :type processing_type: Path
    :param filepath:
    :type filepath: Path
    :return:
    :rtype: Path
    """
    return Path(processed_data_dir).joinpath(processing_type).joinpath(filepath)


def directory_exists_or_create(filepath: Path) -> bool:
    """
    Checks if directory of a filepath exists. If doesnt, it creates it.
    Returns bool that indicates if the directory exists in the end. Should be always True.
    :param filepath: Path to the file you want to save and check it the parent directory exists.
    :type filepath: Path
    :return: If the directory exists in the end.
    :rtype: bool
    """
    directory = filepath.parent
    logging.info(f"Checking if directory {directory} exists")
    if not directory.exists():
        logging.info(f"Directory {directory} does not exists, trying to create.")
        directory.mkdir(parents=True)
    return directory.exists()


def next_pow_2(number: int) -> int:
    """
       Finds a number that is a power of two that is next after value provided to that method
    :param number: Value of which you need next power of 2
    :type number: int
    :return: Next power of two
    :rtype: int
    """
    return int(np.ceil(np.log2(number)))


def resample_with_padding(
    st: obspy.Stream, sampling_rate: Union[int, float]
) -> obspy.Stream:
    """
    Pads data of trace (assumes that stream has only one trace) with zeros up to next power of two
    and resamples it down to provided sampling rate. Furtherwards trims it to original starttime and endtime.
    :param st: Stream containing one trace to be resampled
    :type st: obspy.Stream
    :param sampling_rate: Target sampling rate
    :type sampling_rate: Union[int, float]
    :return: Resampled stream
    :rtype: obspy.Stream
    """

    tr = st[0].copy()
    starttime = tr.stats.starttime
    endtime = tr.stats.endtime
    npts = tr.stats.npts

    logging.info("Finding deficit of sampls up to next power of 2")
    deficit = 2 ** next_pow_2(npts) - npts

    logging.info("Padding with zeros up to the next power of 2")
    tr.data = np.concatenate((tr.data, np.zeros(deficit)))

    logging.info("Resampling")
    tr.resample(sampling_rate)

    logging.info(
        f"Slicing data to fit them between starttime {starttime} and endtime {endtime}"
    )
    st[0] = tr.slice(starttime=starttime, endtime=endtime)

    logging.info("Resampling done!")
    return st


def preprocess_whole_day(
    st: obspy.Stream, preprocessing_config: ProcessingParams
) -> obspy.Stream:
    logging.info(f"Trying to merge traces if more than 1")
    st.merge()

    if len(st) > 1:
        logging.error("There are more than one trace in the stream, raising error.")
        raise ValueError(f"There are {len(st)} traces in the stream!")

    logging.info(
        f"Resampling stream to {preprocessing_config.sampling_rate} Hz with padding to next power of 2"
    )
    st = resample_with_padding(st=st, sampling_rate=preprocessing_config.sampling_rate)
    logging.info(
        f"Filtering with bandpass to "
        f"low: {preprocessing_config.prefiltering_low};"
        f"high: {preprocessing_config.prefiltering_high};"
        f"order: {preprocessing_config.prefiltering_order}"
    )
    st.filter(
        type="bandpass",
        freqmin=preprocessing_config.prefiltering_low,
        freqmax=preprocessing_config.prefiltering_high,
        corners=preprocessing_config.prefiltering_order,
    )
    logging.info("Finished processing whole day")
    return st


def merge_traces_fill_zeros(st: obspy.Stream) -> obspy.Stream:
    """
    TODO Fill the docstring

    :param st: stream to be trimmed
    :type st: obspy.Stream
    :return: Trimmed stream
    :rtype: obspy.Stream
    """
    st.merge(fill_value=0)
    return st


def pad_zeros_to_exact_time_bounds(
    st: obspy.Stream, timespan: Timespan, expected_no_samples: int
) -> obspy.Stream:
    """
    Takes a obspy Stream and trims it with Stream.trim to starttime and endtime of provided Timespan.
    It also verifies if resulting number of samples is as expected.

    :param st: stream to be trimmed
    :type st: obspy.Stream
    :param timespan: Timespan to be used for trimming
    :type timespan: Timespan
    :param expected_no_samples: Expected number of samples to be verified
    :type expected_no_samples: int
    :return: Trimmed stream
    :rtype: obspy.Stream
    :raises ValueError
    """
    st.trim(
        starttime=obspy.UTCDateTime(timespan.starttime),
        endtime=obspy.UTCDateTime(timespan.endtime),
        nearest_sample=False,
        pad=True,
        fill_value=0,
    )

    if st[0].stats.npts != expected_no_samples:
        raise ValueError(
            f"The try of padding with zeros to {expected_no_samples} was not successful. Current length of data is {st[0].stats.npts}"
        )
    return st


def preprocess_timespan(
    trimed_st: obspy.Stream,
    inventory: obspy.Inventory,
    processing_params: ProcessingParams,
) -> obspy.Stream:
    """
    Applies standard preprocessing to a obspy.Stream. It consist of tapering, detrending,
    response removal and filtering.
    :param trimed_st: Stream to be treated
    :type trimed_st: obspy.Stream
    :param inventory: Inventory to have the response removed
    :type inventory: obspy.Inventory
    :param processing_params: Processing parameters object with all required info.
    :type processing_params: ProcessingParams
    :return: Processed Stream
    :rtype: obspy.Stream
    """
    logging.info(
        f"Tapering stream with type: {processing_params.preprocessing_taper_type}; "
        f"width: {processing_params.preprocessing_taper_width}; "
        f"side: {processing_params.preprocessing_taper_side}"
    )
    trimed_st.taper(
        type=processing_params.preprocessing_taper_type,
        max_percentage=processing_params.preprocessing_taper_width,
        side=processing_params.preprocessing_taper_side,
    )
    logging.info(f"Detrending")
    trimed_st.detrend(type="polynomial", order=3)
    logging.info("Removing response")
    trimed_st.remove_response(inventory)
    logging.info(
        f"Filtering with bandpass;"
        f"low: {processing_params.prefiltering_low}; "
        f"high: {processing_params.prefiltering_high}; "
        f"order: {processing_params.prefiltering_order};"
    )
    trimed_st.filter(
        type="bandpass",
        freqmin=processing_params.prefiltering_low,
        freqmax=processing_params.prefiltering_high,
        corners=processing_params.prefiltering_order,
        zerophase=True,
    )

    logging.info("Finished preprocessing with success")
    return trimed_st


def create_datachunks_for_component(
    execution_date: datetime.datetime,
    component: Component,
    timespans: Collection[Timespan],
    processing_params: ProcessingParams,
    processed_data_dir: Path,
) -> None:

    no_datachunks = count_datachunks_for_timespans((component,), timespans)

    timespans_count = len(timespans)

    logging.info(f"There are {no_datachunks} datachunks for {execution_date} in db")

    if no_datachunks == timespans_count:
        logging.info(
            f"There is enough of datachunks in the db (no_datahcunks == no_timespans)"
        )
        return

    logging.info("Fetching timeseries")
    try:
        time_series = fetch_raw_timeseries(
            component=component, execution_date=execution_date
        )
    except NoDataException as e:
        logging.error(e)
        raise e

    logging.info("Reading timeseries and inventory")
    st = time_series.read_file()
    inventory = component.read_inventory()

    # logging.info("Preprocessing initially full day timeseries")
    # st = preprocess_whole_day(st, processing_params)

    logging.info("Splitting full day into timespans")
    for i, timespan in enumerate(timespans):

        logging.info(f"Slicing timespan {i}/{timespans_count}")
        trimed_st = st.slice(
            starttime=timespan.starttime_obspy(),
            endtime=timespan.remove_last_microsecond(),
            nearest_sample=False,
        )

        trimed_st, deficit = validate_slice(
            trimed_st, timespan, processing_params, time_series.samplerate
        )

        logging.info("Preprocessing timespan")
        trimed_st = preprocess_timespan(
            trimed_st=trimed_st,
            inventory=inventory,
            processing_params=processing_params,
        )

        filepath = assembly_filepath(
            processed_data_dir,
            "datachunk",
            assembly_sds_like_dir(component, timespan).joinpath(
                assembly_preprocessing_filename(component, timespan)
            ),
        )
        logging.info(f"Chunk will be written to {str(filepath)}")
        directory_exists_or_create(filepath)

        datachunk = Datachunk(
            processing_params_id=processing_params.id,
            component_id=component.id,
            timespan_id=timespan.id,
            sampling_rate=trimed_st[0].stats.sampling_rate,
            npts=trimed_st[0].stats.npts,
            filepath=str(filepath),
            padded_npts=deficit,
        )
        logging.info(
            "Checking if there are some chunks fot tht timespan and component in db"
        )
        existing_chunks = (
            db.session.query(Datachunk)
            .filter(
                Datachunk.component_id == datachunk.component_id,
                Datachunk.timespan_id == datachunk.timespan_id,
            )
            .all()
        )
        logging.info(
            "Checking if there are some timeseries files  for tht timespan and component on the disc"
        )
        if len(existing_chunks) == 0:
            logging.info("Writing file to disc and adding entry to db")
            trimed_st.write(datachunk.filepath, format="mseed")
            db.session.add(datachunk)
        else:
            if not Path(datachunk.filepath).exists():
                logging.info(
                    "There is some chunk in the db so I will update it and write/overwrite file to the disc."
                )
                trimed_st.write(datachunk.filepath, format="mseed")
                insert_command = (
                    insert(Datachunk)
                    .values(
                        processing_config_id=datachunk.processing_params_id,
                        component_id=datachunk.component_id,
                        timespan_id=datachunk.timespan_id,
                        sampling_rate=datachunk.sampling_rate,
                        npts=datachunk.npts,
                        filepath=datachunk.filepath,
                        padded_npts=deficit,
                    )
                    .on_conflict_do_update(
                        constraint="unique_datachunk_per_timespan_per_station_per_processing",
                        set_=dict(filepath=datachunk.filepath, padded_npts=deficit),
                    )
                )
                db.session.execute(insert_command)
        db.session.commit()
    return


def validate_slice(
    trimed_st: obspy.Stream,
    timespan: Timespan,
    processing_params: ProcessingParams,
    raw_sps: int,
) -> Tuple[obspy.Stream, Optional[int]]:
    if len(trimed_st) == 0:
        ValueError(f"There was no data to be cut for that timespan")

    if len(trimed_st) > 1:
        logging.warning(
            f"There are {len(trimed_st)} traces in that stream. Trying to proceed anyway."
        )

    samples_in_stream = sum([x.stats.npts for x in trimed_st])

    if samples_in_stream < processing_params.get_raw_minimum_no_samples(raw_sps):
        logging.error(
            f"There were {samples_in_stream} in the trace"
            f" while {processing_params.get_raw_minimum_no_samples(raw_sps)} were expected. "
            f"Skipping this chunk."
        )
        raise ValueError(f"Not enough data in a chunk.")

    elif samples_in_stream < processing_params.get_raw_expected_no_samples(raw_sps):
        deficit = (
            processing_params.get_raw_expected_no_samples(raw_sps)
            - trimed_st[0].stats.npts
        )
        logging.warning(
            f"Datachunk has less samples than expected but enough to be accepted."
            f"It will be padded with {deficit} zeros to match exact length."
        )

        trimed_st = merge_traces_fill_zeros(trimed_st)

        if trimed_st[0].stats.npts < processing_params.get_raw_expected_no_samples(
            raw_sps
        ):
            try:
                trimed_st = pad_zeros_to_exact_time_bounds(
                    trimed_st,
                    timespan,
                    processing_params.get_raw_expected_no_samples(raw_sps),
                )
            except ValueError as e:
                logging.warning(f"Padding was not successful. SKipping chunk.")
                raise ValueError(f"Datachunk padding unsuccessful.")
    else:
        deficit = None
    return trimed_st, deficit


def run_chunk_preparation(
    app, station, component, execution_date, processed_data_dir, processing_config_id=1
):
    year = execution_date.year
    day_of_year = execution_date.timetuple().tm_yday

    logging.info(f"Fetching processing config, timespans and componsents from db")
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

    logging.info(f"Invoking chunc creation itself")
    for component in components:
        with app.app_context() as ctx:
            create_datachunks_for_component(
                execution_date=execution_date,
                component=component,
                timespans=timespans,
                processing_params=processing_config,
                processed_data_dir=processed_data_dir,
            )
    return


def run_paralel_chunk_preparation(
    stations, components, startdate, enddate, processing_config_id=1
):
    date_range = pd.date_range(start=startdate, end=enddate, freq="D")

    logging.info(f"Fetching processing config, timespans and componsents from db")
    processing_config = (
        db.session.query(ProcessingParams)
        .filter(ProcessingParams.id == processing_config_id)
        .first()
    )

    all_timespans = {}
    for date in date_range:
        all_timespans[date] = fetch_timespans_for_doy(
            year=date.year, doy=date.timetuple().tm_yday
        )
    processed_data_dir = os.environ.get("PROCESSED_DATA_DIR")

    components = Component.query.filter(
        Component.station.in_(stations), Component.component.in_(components)
    ).all()

    def generate_tasks_for_chunk_preparation(
        components, all_timespans, processing_config, processed_data_dir
    ):
        for component in components:
            for date, timespans in all_timespans.items():
                yield (
                    date,
                    component,
                    timespans,
                    processing_config,
                    processed_data_dir,
                )

    joblist = generate_tasks_for_chunk_preparation(
        components, all_timespans, processing_config, processed_data_dir
    )

    logging.info(f"Invoking chunk creation itself")
    with Pool(10) as p:
        p.starmap(create_datachunks_for_component, joblist)

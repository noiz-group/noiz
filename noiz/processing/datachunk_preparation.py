import obspy
from pathlib import Path
import numpy as np
import os
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import datetime
from tqdm import tqdm
import pendulum
import itertools

from noiz.app import create_app
from noiz.database import db
from noiz.models import Component, Soh, DataChunk, Timespan, Tsindex, ProcessingParams

from sqlalchemy.dialects.postgresql import insert

import logging


class NoDataException(Exception):
    pass


def expected_npts(timespan_length, sampling_rate):
    return timespan_length * sampling_rate


def assembly_preprocessing_filename(component, timespan):
    year = str(timespan.starttime.year)
    doy_time = timespan.starttime.strftime("%j.%H%M")

    fname = ".".join(
        [component.network, component.station, component.component, year, doy_time]
    )

    return fname


def assembly_sds_like_dir(component, timespan):
    return (
        Path(str(timespan.starttime.year))
        .joinpath(component.network)
        .joinpath(component.station)
        .joinpath(component.component)
    )


def assembly_filepath(processed_data_dir, processing_type, filepath):
    return Path(processed_data_dir).joinpath(processing_type).joinpath(filepath)


def directory_exists_or_create(filepath):
    directory = filepath.parent
    if not directory.exists():
        directory.mkdir(parents=True)
    return directory.exists()


def fetch_timeseries(component, execution_date):
    year = execution_date.year
    day_of_year = execution_date.timetuple().tm_yday
    time_series = Tsindex.query.filter(
        Tsindex.network == component.network,
        Tsindex.station == component.station,
        Tsindex.component == component.component,
        Tsindex.starttime_year == year,
        Tsindex.starttime_doy == day_of_year,
    ).all()
    if len(time_series) > 1:
        raise ValueError(
            f"There are more then one files for that day in timeseries! {component._make_station_string()} {year}.{day_of_year}"
        )
    elif len(time_series) == 0:
        raise NoDataException
    else:
        return time_series[0]


def check_datachunks_for_timespans(component, timespans):
    timespan_ids = [x.id for x in timespans]
    count = DataChunk.query.filter(
        DataChunk.component_id == component.id, DataChunk.timespan_id.in_(timespan_ids)
    ).count()
    return count


def get_timespans_for_doy(year, doy):
    timespans = Timespan.query.filter(
        Timespan.starttime_year == year, Timespan.starttime_doy == doy
    ).all()
    return timespans


def preprocess_whole_day(st: obspy.Stream, preprocessing_config) -> obspy.Stream:
    st.merge()
    st.resample(sampling_rate=preprocessing_config.sampling_rate)
    st.filter(
        type="bandpass",
        freqmin=preprocessing_config.prefiltering_low,
        freqmax=preprocessing_config.prefiltering_high,
        corners=preprocessing_config.prefiltering_order,
    )
    return st


def preprocess_timespan(
    trimed_st: obspy.Stream,
    inventory: obspy.Inventory,
    processing_params: ProcessingParams,
) -> obspy.Stream:
    trimed_st.taper(
        type=processing_params.preprocessing_taper_type,
        max_percentage=processing_params.preprocessing_taper_width,
        side=processing_params.preprocessing_taper_side,
    )
    trimed_st.detrend(type="polynomial", order=3)
    trimed_st.remove_response(inventory)
    trimed_st.filter(
        type="bandpass",
        freqmin=processing_params.prefiltering_low,
        freqmax=processing_params.prefiltering_high,
        corners=processing_params.prefiltering_order,
        zerophase=True,
    )

    return trimed_st


def create_datachunks_for_component(
    app, execution_date, component, timespans, processing_params, processed_data_dir
):
    no_datachunks = check_datachunks_for_timespans(component, timespans)
    logging.info(f"There are {no_datachunks} datachunks for {execution_date} in db")
    if no_datachunks == len(timespans):
        logging.info(
            f"There is enough of datachunks in the db (no_datahcunks==no_timespans)"
        )
        return

    logging.info("Fetching timeseries")
    time_series = fetch_timeseries(component=component, execution_date=execution_date)

    logging.info("Reading timeseries and inventory")
    st = time_series.read_file()
    inventory = component.read_inventory()

    logging.info("Preprocessing initially full day timeseries")
    st = preprocess_whole_day(st, processing_params)

    logging.info("Splitting full day into timespans")
    for i, timespan in enumerate(timespans):
        logging.info(f"Slicing timespan {i}/{len(timespans)}")
        trimed_st = st.slice(
            starttime=timespan.starttime_obspy(),
            endtime=timespan.remove_last_nanosecond(),
            nearest_sample=False,
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

        datachunk = DataChunk(
            processing_params_id=processing_params.id,
            component_id=component.id,
            timespan_id=timespan.id,
            sampling_rate=trimed_st[0].stats.sampling_rate,
            npts=trimed_st[0].stats.npts,
            filepath=str(filepath),
        )
        logging.info(
            "Checking if there are some chunks fot tht timespan and component in db"
        )
        existing_chunks = (
            db.session.query(DataChunk)
            .filter(
                DataChunk.component_id == datachunk.component_id,
                DataChunk.timespan_id == datachunk.timespan_id,
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
                    insert(DataChunk)
                    .values(
                        processing_config_id=processing_params.id,
                        component_id=datachunk.component_id,
                        timespan_id=datachunk.timespan_id,
                        sampling_rate=datachunk.sampling_rate,
                        npts=datachunk.npts,
                        filepath=datachunk.filepath,
                    )
                    .on_conflict_do_update(
                        constraint="unique_datachunk_per_timespan_per_station_per_processing",
                        set_=dict(filepath=datachunk.filepath),
                    )
                )
                db.session.execute(insert_command)
        db.session.commit()
    return


def run_chunk_preparation(
    app, station, execution_date, processed_data_dir, processing_config_id=1
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
        timespans = get_timespans_for_doy(year=year, doy=day_of_year)

        components = Component.query.filter(Component.station == station).all()

    logging.info(f"Invoking chunc creation itself")
    for component in components:
        with app.app_context() as ctx:
            create_datachunks_for_component(
                app=app,
                execution_date=execution_date,
                component=component,
                timespans=timespans,
                processed_data_dir=processed_data_dir,
                processing_params=processing_config,
            )

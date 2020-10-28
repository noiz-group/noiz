import datetime
import itertools
import logging
import pandas as pd
import warnings
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, Collection, Generator, Union
from sqlalchemy.orm.query import Query

import numpy as np

from noiz.api import fetch_timespans_between_dates
from noiz.api.component import fetch_components
from noiz.api.helpers import validate_exactly_one_argument_provided, extract_object_ids
from noiz.database import db
from noiz.models import SohInstrument, SohGps, Component, Timespan
from noiz.models.soh import association_table_soh_instr, association_table_soh_gps, AveragedSohGps, \
    association_table_averaged_soh_gps_components
from noiz.processing.soh import load_parsing_parameters, read_multiple_soh, __postprocess_soh_dataframe, \
    glob_soh_directory, __calculate_mean_gps_soh


def fetch_raw_soh_gps_df(
        components: Collection[Component],
        starttime: datetime.datetime,
        endtime: datetime.datetime
) -> pd.DataFrame:
    query = __fetch_raw_soh_gps_query(components=components, starttime=starttime, endtime=endtime)

    c = query.statement.compile(query.session.bind)
    df = pd.read_sql(c.string, query.session.bind, params=c.params)

    return df


def fetch_raw_soh_gps_all(
        components: Collection[Component],
        starttime: datetime.datetime,
        endtime: datetime.datetime
) -> Collection[SohGps]:

    query = __fetch_raw_soh_gps_query(components=components, starttime=starttime, endtime=endtime)

    return query.all()


def count_raw_soh_gps(
        components: Collection[Component],
        starttime: datetime.datetime,
        endtime: datetime.datetime
) -> int:

    query = __fetch_raw_soh_gps_query(components=components, starttime=starttime, endtime=endtime)

    return query.count()


def __fetch_raw_soh_gps_query(
        components: Collection[Component],
        starttime: datetime.datetime,
        endtime: datetime.datetime
) -> Query:
    components_ids = extract_object_ids(components)

    fetched_soh_gps_query = SohGps.query.filter(
        SohGps.z_component_id.in_(components_ids),
        SohGps.datetime >= starttime,
        SohGps.datetime <= endtime,
    )

    return fetched_soh_gps_query


def fetch_averaged_soh_gps_df(
        timespans: Collection[Timespan],
        components: Collection[Component],
) -> pd.DataFrame:

    query = __fetch_averaged_soh_gps_query(timespans=timespans, components=components)

    c = query.statement.compile(query.session.bind)
    df = pd.read_sql(c.string, query.session.bind, params=c.params)

    return df


def fetch_averaged_soh_gps_all(
        timespans: Collection[Timespan],
        components: Collection[Component],
) -> Collection[AveragedSohGps]:

    query = __fetch_averaged_soh_gps_query(timespans=timespans, components=components)

    return query.all()


def count_averaged_soh_gps(
        timespans: Collection[Timespan],
        components: Collection[Component],
) -> int:

    query = __fetch_averaged_soh_gps_query(timespans=timespans, components=components)

    return query.count()


def __fetch_averaged_soh_gps_query(
        timespans: Collection[Timespan],
        components: Collection[Component],
) -> Query:
    timespans_ids = extract_object_ids(timespans)
    components_ids = extract_object_ids(components)

    query = AveragedSohGps.query.filter(
        AveragedSohGps.z_component_id.in_(components_ids),
        AveragedSohGps.timespan_id.in_(timespans_ids),
    )

    return query


def ingest_soh_files(
        station: str,
        station_type: str,
        soh_type: str,
        main_filepath: Optional[Path] = None,
        filepaths: Optional[Collection[Path]] = None,
        network: Optional[str] = None,
) -> None:
    """
    Method that take either a directory or collection of paths and parses it according to predefined rules for
    containing the StateOfHealth (SOH) information from compatible station types and soh types.
    It upserts it into DB and makes a connection between each SOH datapoint and all Components on the station you
    provided in the arguments.

    :param station: Station to associate SOH with
    :type station: str
    :param station_type: Type of station. Available types are defined in noiz.processing.soh.SohInstrumentNames
    :type station_type: str
    :param soh_type: Type of soh. Available types are defined in noiz.processing.soh.SohType
    :type soh_type: str
    :param main_filepath: Filepath to be globbed.
    :type main_filepath: Optional[Path] = None
    :param filepaths: Selected filepaths to be parsed
    :type filepaths: Optional[Collection[Path]] = None
    :param network: Network that the station belongs to, optional
    :type network: Optional[str] = None
    :return: None
    :rtype: NoneType
    """

    parsing_parameters = load_parsing_parameters(soh_type, station_type)

    try:
        validate_exactly_one_argument_provided(filepaths, main_filepath)
    except ValueError:
        raise ValueError("Exactly one of filepath or main_filepath arguments has to be provided.")

    if main_filepath is not None:
        filepaths: Generator[Path, None, None] = glob_soh_directory(   # type: ignore
            parsing_parameters=parsing_parameters,
            main_filepath=main_filepath
        )

    df = read_multiple_soh(filepaths=filepaths, parsing_params=parsing_parameters)  # type: ignore
    df = __postprocess_soh_dataframe(df, station_type=station_type, soh_type=soh_type)

    if soh_type == "instrument":
        __upsert_into_db_soh_instrument(df=df, station=station, network=network)
    elif soh_type in ("gpstime", "gnsstime"):
        __upsert_into_db_soh_gps(df=df, station=station, network=network)
    else:
        raise ValueError(f'Provided soh_type not supported for database insertion. '
                         f'Supported types: instrument, gpstime, gnsstime. '
                         f'You provided {soh_type}')
    return


def __upsert_into_db_soh_instrument(
        df: pd.DataFrame,
        station: str,
        network: Optional[str] = None,
) -> None:
    """
    Internal method that is used for upserting instrument SOH data into the DB.
    The passed pd.DataFrame has to be indexed with datetime index, and contain columns named as:
    "Supply voltage(V)", "Total current(A)", "Temperature(C)"

    :param df: Dataframe containing SOH information that should be upserted into DB.
    :type df: pd.DataFrame
    :param station: Station to associate all SOH with.
    :type station: str
    :param network: Network that station belongs to
    :type network: str
    :return: None
    :rtype: NoneType
    """

    fetched_components = fetch_components(networks=network, stations=station)

    z_component_id = None
    fetched_components_ids = []
    for cmp in fetched_components:
        fetched_components_ids.append(cmp.id)
        if cmp.component == 'Z':
            z_component_id = cmp.id

    command_count = len(df)

    insert_commands = []
    for i, (timestamp, row) in enumerate(df.iterrows()):
        if i % int(command_count / 10) == 0:
            logging.info(f"Prepared already {i}/{command_count} commands")
        insert_command = (
            insert(SohInstrument)
            .values(
                z_component_id=z_component_id,
                datetime=timestamp,
                voltage=row["Supply voltage(V)"],
                current=row["Total current(A)"],
                temperature=row["Temperature(C)"],
            )
            .on_conflict_do_update(
                constraint="unique_timestamp_per_station_in_sohinstrument",
                set_=dict(
                    voltage=row["Supply voltage(V)"],
                    current=row["Total current(A)"],
                    temperature=row["Temperature(C)"],
                ),
            )
        )
        insert_commands.append(insert_command)

    for i, insert_command in enumerate(insert_commands):
        if i % int(command_count / 10) == 0:
            logging.info(f"Inserted already {i}/{command_count} rows")
        db.session.execute(insert_command)

    logging.info('Commiting to db')
    db.session.commit()
    logging.info('Commit succesfull')

    logging.info('Preparing to insert information about db relationship/')

    soh_env_inserted = SohInstrument.query.filter(SohInstrument.z_component_id.in_(fetched_components_ids),
                                                  SohInstrument.datetime.in_(df.index.to_list())).all()

    command_count = len(soh_env_inserted) * len(fetched_components)

    insert_commands = []
    for i, (inserted_soh, component_id) in enumerate(itertools.product(soh_env_inserted, fetched_components_ids)):
        if i % int(command_count / 10) == 0:
            logging.info(f"Prepared already {i}/{command_count} commands")

        insert_command = (
            insert(association_table_soh_instr)
            .values(
                soh_instrument_id=inserted_soh.id,
                component_id=component_id
            )
            .on_conflict_do_nothing()
        )
        insert_commands.append(insert_command)

    for i, insert_command in enumerate(insert_commands):
        if i % int(command_count / 10) == 0:
            logging.info(f"Inserted already {i}/{command_count} rows")
        db.session.execute(insert_command)

    logging.info('Commiting to db')
    db.session.commit()
    logging.info('Commit succesfull')

    return


def __upsert_into_db_soh_gps(
        df: pd.DataFrame,
        station: str,
        network: Optional[str] = None,
) -> None:
    """
        Internal method that is used for upserting GPS SOH data into the DB.
        The passed pd.DataFrame has to be indexed with datetime index, and contain columns named as:
        "Time error(ms)" and "Time uncertainty(ms)"

        :param df: Dataframe containing SOH information that should be upserted into DB.
        :type df: pd.DataFrame
        :param station: Station to associate all SOH with.
        :type station: str
        :param network: Network that station belongs to
        :type network: str
        :return: None
        :rtype: NoneType
        """

    fetched_components = fetch_components(networks=network, stations=station)

    z_component_id = None
    fetched_components_ids = []
    for cmp in fetched_components:
        fetched_components_ids.append(cmp.id)
        if cmp.component == 'Z':
            z_component_id = cmp.id

    command_count = len(df)

    insert_commands = []
    for i, (timestamp, row) in enumerate(df.iterrows()):
        if i % int(command_count / 10) == 0:
            logging.info(f"Prepared already {i}/{command_count} commands")
        insert_command = (
            insert(SohGps)
            .values(
                z_component_id=z_component_id,
                datetime=timestamp,
                time_error=row["Time error(ms)"],
                time_uncertainty=row["Time uncertainty(ms)"],
            )
            .on_conflict_do_update(
                constraint="unique_timestamp_per_station_in_sohgps",
                set_=dict(
                    time_error=row["Time error(ms)"],
                    time_uncertainty=row["Time uncertainty(ms)"],
                ),
            )
        )
        insert_commands.append(insert_command)

    for i, insert_command in enumerate(insert_commands):
        if i % int(command_count / 10) == 0:
            logging.info(f"Inserted already {i}/{command_count} rows")
        db.session.execute(insert_command)

    logging.info('Commiting to db')
    db.session.commit()
    logging.info('Commit succesfull')

    logging.info('Preparing to insert information about db relationship/')

    fetched_soh = SohGps.query.filter(SohGps.z_component_id.in_(fetched_components_ids),
                                      SohGps.datetime.in_(df.index.to_list())).all()

    command_count = len(fetched_soh) * len(fetched_components)

    insert_commands = []
    for i, (inserted_soh, component_id) in enumerate(itertools.product(fetched_soh, fetched_components_ids)):
        if i % int(command_count / 10) == 0:
            logging.info(f"Prepared already {i}/{command_count} commands")

        insert_command = (
            insert(association_table_soh_gps)
            .values(
                soh_gps_id=inserted_soh.id,
                component_id=component_id
            )
            .on_conflict_do_nothing()
        )
        insert_commands.append(insert_command)

    for i, insert_command in enumerate(insert_commands):
        if i % int(command_count / 10) == 0:
            logging.info(f"Inserted already {i}/{command_count} rows")
        db.session.execute(insert_command)

    logging.info('Commiting to db')
    db.session.commit()
    logging.info('Commit succesfull')

    return


def average_raw_gps_soh(
        starttime: datetime.datetime,
        endtime: datetime.datetime,
        stations: Optional[Union[Collection[str], str]] = None,
        networks: Optional[Union[Collection[str], str]] = None,
) -> None:
    """
    Method that averages the data from SohGps according between starttime and endtime along with
    Timespans and their starttimes and endtimes.

    :param starttime: The earliest time when the Timespans will be searched for
    :type starttime: datetime.datetime
    :param endtime: The latest time when the Timespans will be searched for
    :type endtime: datetime.datetime
    :param stations: Stations to limit the averaging
    :type stations: Optional[Union[Collection[str], str]]
    :param networks: Networks to limit the averaging
    :type networks: Optional[Union[Collection[str], str]]
    :return: None
    :rtype: NoneType
    """
    fetched_timespans = fetch_timespans_between_dates(starttime=starttime, endtime=endtime)
    fetched_components = fetch_components(stations=stations, networks=networks)

    averaged_results = __calculate_averages_of_gps_soh(
        fetched_timespans=fetched_timespans,
        fetched_components=fetched_components
    )

    __insert_averaged_gps_soh_into_db(avg_results=averaged_results)

    return


def __calculate_averages_of_gps_soh(
        fetched_timespans: Collection[Timespan],
        fetched_components: Collection[Component],
) -> pd.DataFrame:
    """
    Private method that fetched raw SohGps data and calculates average of it for each of the Timespans.

    :param fetched_timespans: Timespans to average SOH data on
    :type fetched_timespans: Collection[Timespan]
    :param fetched_components: Components to average SOH data for
    :type fetched_components: Collection[Component]
    :return: Averaged GPS data as well as all auxiliary information that it's necessary to insert them into db
    :rtype: pd.DataFrame
    """

    component_ids = extract_object_ids(fetched_components)

    averaged_results = []
    for timespan in fetched_timespans:

        query = (
            db.session
            .query(SohGps, (Component.id).label('component_id'))
            .join((SohGps.components, Component))
            .filter(
                SohGps.datetime >= timespan.starttime,
                SohGps.datetime <= timespan.endtime,
                Component.id.in_(component_ids)
            )
        )

        c = query.statement.compile(query.session.bind)
        df = pd.read_sql(c.string, query.session.bind, params=c.params)

        try:
            res = __calculate_mean_gps_soh(df, timespan_id=timespan.id)
        except ValueError:
            continue

        averaged_results.extend(res)

    return pd.DataFrame(averaged_results)


def __insert_averaged_gps_soh_into_db(avg_results: pd.DataFrame) -> None:
    """
    Private method that upserts averaged GPS SOH data into the DB.
    It also inserts data to the association table so the Many-To-Many relation is possible.

    :param avg_results: Data to be inserted into the DB
    :type avg_results: pd.DataFrame
    :return: None
    :rtype: NoneType
    """

    command_count = len(avg_results)
    avg_results = avg_results

    insert_commands = []
    for i, (timestamp, row) in enumerate(avg_results.iterrows()):
        if i % int(command_count / 10) == 0:
            logging.info(f"Prepared already {i}/{command_count} commands")
        insert_command = (
            insert(AveragedSohGps)
            .values(
                z_component_id=row["z_component_id"],
                timespan_id=row["timespan_id"],
                time_error=row["time_error"],
                time_uncertainty=row["time_uncertainty"],
            )
            .on_conflict_do_update(
                constraint="unique_tispan_per_station_in_avgsohgps",
                set_=dict(
                    time_error=row["time_error"],
                    time_uncertainty=row["time_uncertainty"],
                ),
            )
        )
        insert_commands.append(insert_command)

    for i, insert_command in enumerate(insert_commands):
        if i % int(command_count / 10) == 0:
            logging.info(f"Inserted already {i}/{command_count} rows")
        db.session.execute(insert_command)

    logging.info('Commiting to db')
    db.session.commit()
    logging.info('Commit succesfull')

    logging.info('Preparing to insert information about db relationship/')

    unique_z_cmp_ids = avg_results.loc[:, "z_component_id"].drop_duplicates().values.astype(pd.Int64Dtype)

    insert_commands = []

    command_count = len(avg_results) * 3

    for z_cmp in unique_z_cmp_ids:
        all_components = np.unique(
            np.vstack(
                avg_results.loc[avg_results.loc[:, 'z_component_id'] == z_cmp, "all_components"].values)
            .flatten()
            .astype(pd.Int64Dtype)
        )
        used_timespan_ids = np.unique(
            np.vstack(
                avg_results.loc[avg_results.loc[:, 'z_component_id'] == z_cmp, "timespan_id"].values)
            .flatten()
            .astype(pd.Int64Dtype)
        )

        fetched_soh = AveragedSohGps.query.filter(
            AveragedSohGps.z_component_id.in_(unique_z_cmp_ids),
            AveragedSohGps.timespan_id.in_(used_timespan_ids)).all()

        for i, (inserted_soh, component_id) in enumerate(itertools.product(fetched_soh, all_components)):
            if i % int(command_count / 10) == 0:
                logging.info(f"Prepared already {i}/{command_count} commands")

            insert_command = (
                insert(association_table_averaged_soh_gps_components)
                .values(
                    averaged_soh_gps_id=inserted_soh.id,
                    component_id=component_id
                )
                .on_conflict_do_nothing()
            )
            insert_commands.append(insert_command)

    for i, insert_command in enumerate(insert_commands):
        if i % int(command_count / 10) == 0:
            logging.info(f"Inserted already {i}/{command_count} rows")
        db.session.execute(insert_command)

    logging.info('Commiting to db')
    db.session.commit()
    logging.info('Commit succesfull')

    return


def parse_soh_insert_into_db(
    station, station_type, saint_illiers_fulldir, single_day, execution_date
):
    """
    DEPRECATED. Do not use.
    It's a wrapped just to preserve compatibility with current code.
    """

    warnings.warn("Deprecated. Use other methods")

    soh_path = (
        Path(saint_illiers_fulldir)
        .joinpath("STI-soh")
        .joinpath(station)
        .joinpath(execution_date.strftime("%Y/%m"))
    )
    if single_day:
        glob_instrument_soh = f"*Instrument*{execution_date.strftime('%Y%m%d')}*.csv"
        soh_files = list(soh_path.rglob(glob_instrument_soh))

        ingest_soh_files(
            station=station,
            station_type=station_type,
            filepaths=soh_files,
            soh_type='instrument'
        )
    else:
        ingest_soh_files(
            station=station,
            station_type=station_type,
            main_filepath=soh_path,
            soh_type='instrument'
        )
    return

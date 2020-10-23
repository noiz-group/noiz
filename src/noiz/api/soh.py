import warnings

import itertools

from typing import Optional, Collection, Generator, Dict
import pandas as pd
import datetime

import logging
from pathlib import Path

from sqlalchemy.dialects.postgresql import insert

from noiz.database import db
from noiz.models import Component, SohEnvironment
from noiz.models.soh import association_table_soh_env
from noiz.processing.soh import SOH_PARSING_PARAMETERS, read_multiple_soh, postprocess_soh_dataframe

from noiz.api.component import fetch_components


def ingest_soh_files(
        station: str,
        station_type: str,
        soh_type: str,
        main_filepath: Optional[Path] = None,
        filepaths: Optional[Collection[Path]] = None,
        network: Optional[str] = None,
):

    parsing_parameters = load_parsing_parameters(soh_type, station_type)

    if (main_filepath is None and filepaths is None) or (main_filepath is not None and filepaths is not None):
        raise ValueError('There has to be either main_filepath or filepaths provided.')

    if main_filepath is not None:
        filepaths: Generator[Path, None, None] = glob_soh_directory(parsing_parameters=parsing_parameters, main_filepath=main_filepath)  # type: ignore

    df = read_multiple_soh(filepaths=filepaths, parsing_params=parsing_parameters)  # type: ignore
    df = postprocess_soh_dataframe(df, station_type=station_type, soh_type=soh_type)

    if soh_type == "environment":
        insert_into_db_soh_environment(df=df, station=station, network=network)


def glob_soh_directory(parsing_parameters: dict, main_filepath: Path) -> Generator[Path, None, None]:

    if not isinstance(main_filepath, Path):
        if not isinstance(main_filepath, str):
            raise ValueError(f"Expected a filepath to the directory. Got {main_filepath}")
        else:
            main_filepath = Path(main_filepath)

    if not main_filepath.exists():
        raise FileNotFoundError(f"Provided path does not exist. {main_filepath}")

    if not main_filepath.is_dir():
        raise NotADirectoryError(f"It is not a directory! {main_filepath}")

    filepaths_to_parse = main_filepath.rglob(parsing_parameters['search_regex'])

    return filepaths_to_parse


def load_parsing_parameters(soh_type: str, station_type: str) -> Dict:

    if station_type not in SOH_PARSING_PARAMETERS.keys():
        raise ValueError(f"Not supported station type. Supported types are: {SOH_PARSING_PARAMETERS.keys()}, "
                         f"You provided {station_type}")

    if soh_type not in SOH_PARSING_PARAMETERS[station_type].keys():
        raise ValueError(f"Not supported soh type for this station type. "
                         f"For this station type the supported soh types are: "
                         f"{SOH_PARSING_PARAMETERS[station_type].keys()}, "
                         f"You provided {soh_type}")

    parsing_parameters = SOH_PARSING_PARAMETERS[station_type][soh_type]

    return parsing_parameters


def insert_into_db_soh_environment(
        df: pd.DataFrame,
        station: str,
        network: Optional[str] = None,
) -> None:
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
            insert(SohEnvironment)
            .values(
                z_component_id=z_component_id,
                datetime=timestamp,
                voltage=row["Supply voltage(V)"],
                current=row["Total current(A)"],
                temperature=row["Temperature(C)"],
            )
            .on_conflict_do_update(
                constraint="unique_timestamp_per_station_in_sohenvironment",
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

    soh_env_inserted = SohEnvironment.query.filter(SohEnvironment.z_component_id.in_(fetched_components_ids),
                                                   SohEnvironment.datetime.in_(df.index.to_list())).all()

    command_count = len(soh_env_inserted) * len(fetched_components)

    insert_commands = []
    for i, (inserted_soh, component_id) in enumerate(itertools.product(soh_env_inserted, fetched_components_ids)):
        if i % int(command_count / 10) == 0:
            logging.info(f"Prepared already {i}/{command_count} commands")

        insert_command = (
            insert(association_table_soh_env)
            .values(
                soh_environment_id=inserted_soh.id,
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
    """

    warnings.warn("Deprecated. Use other methods")
    soh_type = "instrument"

    logging.info(f"Working on {station}")

    component = (
        db.session.query(Component)
        .filter(Component.component == "Z", Component.station == station)
        .first()
    )
    component_id = component.id

    soh_path = (
        Path(saint_illiers_fulldir)
        .joinpath("STI-soh")
        .joinpath(station)
        .joinpath(execution_date.strftime("%Y/%m"))
    )
    if single_day:
        glob_instrument_soh = f"*Instrument*{execution_date.strftime('%Y%m%d')}*.csv"
    else:
        glob_instrument_soh = f"*Instrument*{execution_date.strftime('%Y%m')}*.csv"

    instrument_parsing_params = SOH_PARSING_PARAMETERS[station_type][soh_type]

    soh_files = list(soh_path.rglob(glob_instrument_soh))
    logging.info(f"found {len(soh_files)} files")

    if len(soh_files) == 0:
        raise ValueError(
            "There are no soh files to be parsed. Maybe those are in miniseed format?"
        )

    df = read_multiple_soh(soh_files, instrument_parsing_params)
    df = postprocess_soh_dataframe(df, station_type=station_type, soh_type=soh_type)

    no_rows = len(df)

    logging.info(f"Parsed into df {len(df)} lines long")
    logging.info("Starting preparation of insert commands")
    insert_commands = []
    for i, (timestamp, row) in enumerate(df.iterrows()):
        if i % int(no_rows / 10) == 0:
            logging.info(f"Prepared already {i}/{no_rows} commands")
        insert_command = (
            insert(SohEnvironment)
            .values(
                component_id=component_id,
                datetime=timestamp,
                voltage=row["Supply voltage(V)"],
                current=row["Total current(A)"],
                temperature=row["Temperature(C)"],
            )
            .on_conflict_do_update(
                constraint="unique_timestamp_per_station",
                set_=dict(
                    voltage=row["Supply voltage(V)"],
                    current=row["Total current(A)"],
                    temperature=row["Temperature(C)"],
                ),
            )
        )
        insert_commands.append(insert_command)

    logging.info("Starting inserting operation")

    for i, insert_command in enumerate(insert_commands):
        if i % int(no_rows / 10) == 0:
            logging.info(f"Inserted already {i}/{no_rows} rows")
        db.session.execute(insert_command)
    db.session.commit()

    return


def parse_instrument_soh(
        station: str,
        station_type: str,
        dir_to_glob,
        date: Optional[datetime.datetime] = None,
):
    """
    DEPRECATED. Do not use.
    """
    warnings.warn("Deprecated. Use other methods")

    soh_type = "instrument"

    logging.info(f"Working on {station}")

    components = fetch_components(stations=(station,))

    z_component = None
    for cmp in components:
        if cmp.component == 'Z':
            z_component = cmp
            break

    if z_component is None:
        raise ValueError(f'There is no Z component for station {station}')

    if date is not None:
        globbing_command = f"*Instrument*{date.strftime('%Y%m%d')}*.csv"
    else:
        globbing_command = "*Instrument*.csv"

    instrument_parsing_params = SOH_PARSING_PARAMETERS[station_type][soh_type]

    soh_files = list(dir_to_glob.rglob(globbing_command))
    logging.info(f"found {len(soh_files)} files")

    if len(soh_files) == 0:
        raise ValueError(
            "There are no soh files to be parsed. Maybe those are in miniseed format?"
        )

    df = read_multiple_soh(soh_files, instrument_parsing_params)
    df = postprocess_soh_dataframe(df, station_type=station_type, soh_type=soh_type)

    no_rows = len(df)

    logging.info(f"Parsed into df {len(df)} lines long")
    logging.info("Starting preparation of insert commands")
    insert_commands = []
    for i, (timestamp, row) in enumerate(df.iterrows()):
        if i % int(no_rows / 10) == 0:
            logging.info(f"Prepared already {i}/{no_rows} commands")
        insert_command = (
            insert(SohEnvironment)
            .values(
                z_component=z_component,
                datetime=timestamp,
                voltage=row["Supply voltage(V)"],
                current=row["Total current(A)"],
                temperature=row["Temperature(C)"],
                components=components
            )
            .on_conflict_do_update(
                constraint="unique_timestamp_per_station",
                set_=dict(
                    voltage=row["Supply voltage(V)"],
                    current=row["Total current(A)"],
                    temperature=row["Temperature(C)"],
                ),
            )
        )
        insert_commands.append(insert_command)

    logging.info("Starting inserting operation")

    for i, insert_command in enumerate(insert_commands):
        if i % int(no_rows / 10) == 0:
            logging.info(f"Inserted already {i}/{no_rows} rows")
        db.session.execute(insert_command)
    db.session.commit()

    return

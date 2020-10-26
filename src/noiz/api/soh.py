import datetime
import itertools
import logging
import pandas as pd
import warnings
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
from typing import Optional, Collection, Generator

from noiz.api.component import fetch_components
from noiz.database import db
from noiz.models import SohInstrument, SohGps
from noiz.models.soh import association_table_soh_instr, association_table_soh_gps
from noiz.processing.soh import load_parsing_parameters, read_multiple_soh, __postprocess_soh_dataframe, \
    glob_soh_directory


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
        filepaths: Generator[Path, None, None] = glob_soh_directory(   # type: ignore
            parsing_parameters=parsing_parameters,
            main_filepath=main_filepath
        )

    df = read_multiple_soh(filepaths=filepaths, parsing_params=parsing_parameters)  # type: ignore
    df = __postprocess_soh_dataframe(df, station_type=station_type, soh_type=soh_type)

    if soh_type == "instrument":
        insert_into_db_soh_instrument(df=df, station=station, network=network)
    elif soh_type in ("gpstime", "gnsstime"):
        insert_into_db_soh_gps(df=df, station=station, network=network)
    else:
        raise ValueError(f'Provided soh_type not supported for database insertion. '
                         f'Supported types: environment, gpstime, gnsstime. '
                         f'You provided {soh_type}')
    return


def insert_into_db_soh_instrument(
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


def insert_into_db_soh_gps(
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


def parse_soh_insert_into_db(
    station, station_type, saint_illiers_fulldir, single_day, execution_date
):
    """
    DEPRECATED. Do not use.
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

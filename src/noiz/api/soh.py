import logging
from pathlib import Path

from sqlalchemy.dialects.postgresql import insert

from noiz.database import db
from noiz.models import Component, Soh
from noiz.processing.soh.parsing import read_multiple_soh, postprocess_soh_dataframe
from noiz.processing.soh.soh_column_names import parsing_parameters


def parse_soh_insert_into_db(
    station, station_type, saint_illiers_fulldir, single_day, execution_date
):
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

    instrument_parsing_params = parsing_parameters[station_type][soh_type]

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
            insert(Soh)
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

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

# mypy: ignore-errors

import click
import os
import pendulum

from flask.cli import AppGroup, with_appcontext, FlaskGroup
import datetime
from pathlib import Path
from typing import Iterable, Optional

from noiz.app import create_app, setup_logging, set_global_verbosity

cli = AppGroup("noiz")
configs_group = AppGroup("configs")  # type: ignore
data_group = AppGroup("data")  # type: ignore
processing_group = AppGroup("processing")  # type: ignore
plotting_group = AppGroup("plotting")  # type: ignore
export_group = AppGroup("export")  # type: ignore

DEFAULT_STARTDATE = pendulum.datetime(2010, 1, 1, 0, 0)
DEFAULT_ENDDATE = pendulum.today(tz="UTC")


def _register_subgroups_to_cli(cli: AppGroup, custom_groups: Iterable[AppGroup]):
    for custom_group in custom_groups:
        cli.add_command(custom_group)
    return


def _setup_logging_verbosity(ctx, param, value) -> None:
    if value > 0:
        set_global_verbosity(verbosity=value)
        setup_logging()


def _setup_quiet(ctx, param, value) -> None:
    if value is True:
        set_global_verbosity(verbosity=0, quiet=True)
        setup_logging()


def _parse_as_date(ctx, param, value) -> Optional[datetime.datetime]:
    """
    This method is used internally as a callback for date arguments to parse the input string
    with use of :py:meth:`pendulum.parse` and return a :class:`datetime.date` object
    """
    if value is None:
        return value
    if not isinstance(value, datetime.datetime):
        return pendulum.parse(value)
    else:
        return value


def _validate_zero_length_as_none(ctx, param, value):
    """
    This method is used to check if value of option with `multiple=True` argument
    contains something or not. If it does not contain any elements, it's converted to None,
    if it contains anything, it is returned as-is.
    """
    if isinstance(value, tuple) and len(value) == 0:
        return None
    else:
        return value


@cli.group("noiz", cls=FlaskGroup, create_app=create_app)
@click.pass_context
def cli(ctx):  # type: ignore
    """Perform operations with noiz package"""
    # This sets up the context to be accessible from callbacks
    # https://github.com/pallets/flask/issues/2410#issuecomment-686581337

    app_ctx = ctx.obj.load_app().app_context()
    app_ctx.push()
    ctx.call_on_close(app_ctx.pop)


@configs_group.group("configs")
def configs_group():  # type: ignore
    """Operations on processing configs in noiz"""
    pass


@configs_group.command("add_datachunk_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add DatachunkParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_datachunk_params(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with DatachunkParams config and add to db."""

    from noiz.api.processing_config import create_and_add_datachunk_params_config_from_toml as parse_and_add

    if add_to_db:
        params = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The DatachunkParams were added to db with id {params.id}")
    else:
        parsing_results, _ = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_processed_datachunk_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add ProcessedDatachunkParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_processed_datachunk_params(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with ProcessedDatachunkParams config and add to db."""

    from noiz.api.processing_config import create_and_add_processed_datachunk_params_from_toml as parse_and_add

    if add_to_db:
        params = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The ProcessedDatachunkParams were added to db with id {params.id}")
    else:
        parsing_results, _ = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_crosscorrelation_cartesian_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add CrosscorrelationCartesianParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_crosscorrelation_cartesian_params(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with CrosscorrelationCartesianParams config and add to db."""

    from noiz.api.processing_config import create_and_add_crosscorrelation_cartesian_params_from_toml as parse_and_add

    if add_to_db:
        params = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The CrosscorrelationCartesianParams were added to db with id {params.id}")
    else:
        parsing_results, _ = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_crosscorrelation_cylindrical_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add CrosscorrelationCylindricalParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_crosscorrelation_cylindrical_params(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with CrosscorrelationCylindricalParams config and add to db."""

    from noiz.api.processing_config import create_and_add_crosscorrelation_cylindrical_params_from_toml as parse_and_add

    if add_to_db:
        params = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The CrosscorrelationCylindricalParams were added to db with id {params.id}")
    else:
        parsing_results, _ = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_beamforming_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add BeamformingParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_beamforming_params(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with BeamformingParams config and add to db."""

    from noiz.api.processing_config import create_and_add_beamforming_params_from_toml as parse_and_add

    if add_to_db:
        params = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The BeamformingParams were added to db with id {params.id}")
    else:
        parsing_results = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("generate_beamforming_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option("-fn", "--freq_min", nargs=1, type=float, required=True)
@click.option("-fx", "--freq_max", nargs=1, type=float, required=True)
@click.option("-fp", "--freq_step", nargs=1, type=float, required=True)
@click.option("-fw", "--freq_window_width", nargs=1, type=float, required=True)
@click.option("-rp", "--rounding_precision", nargs=1, type=int, default=4)
@click.option("-slf", "--slowness_limits_folder", nargs=1, type=str, default=None)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add BeamformingParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def generate_beamforming_params(
        filepath: str,
        add_to_db: bool,
        freq_min: float,
        freq_max: float,
        freq_step: float,
        freq_window_width: float,
        rounding_precision: int,
        **kwargs
):
    """Generate multiple BeamformingParams for different frequencies based on a TOML file
     with a single BeamformingParams config and add them to db."""

    if (kwargs["slowness_limits_folder"]) is not None:
        slowness_limits_folder = Path(kwargs["slowness_limits_folder"])
    else:
        slowness_limits_folder = None
    
    from noiz.api.processing_config import create_and_add_beamforming_params_from_toml
    results = create_and_add_beamforming_params_from_toml(
        filepath=Path(filepath),
        add_to_db=add_to_db,
        generate_multiple=True,
        freq_min=freq_min,
        freq_max=freq_max,
        freq_step=freq_step,
        freq_window_width=freq_window_width,
        rounding_precision=rounding_precision,
        slowness_limits_folder = slowness_limits_folder
    )

    click.echo(f"There were {len(results)} param sets generated.")
    if add_to_db:
        param_ids = [str(params.id) for params in results]
        click.echo(f"The BeamformingParams were added to db with ids {', '.join(param_ids)}")
    else:
        for holder, _ in results:
            click.echo("\n")
            click.echo(holder)


@configs_group.command("add_ppsd_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add PPSDParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_ppsd_params(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with PPSDParams config and add to db."""

    from noiz.api.processing_config import create_and_add_ppsd_params_from_toml as parse_and_add

    if add_to_db:
        params = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The PPSDParams were added to db with id {params.id}")
    else:
        parsing_results, _ = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_event_detection_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add EventDetectionParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_event_detection_params(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with EventDetectionParams config and add to db."""

    from noiz.api.processing_config import create_and_add_event_detection_params_from_toml as parse_and_add

    if add_to_db:
        params = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The EventDetectionParams were added to db with id {params.id}")
    else:
        parsing_results, _ = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_event_confirmation_params")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add EventConfirmationParams to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_event_confirmation_params(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with EventDetectionParams config and add to db."""

    from noiz.api.processing_config import create_and_add_event_confirmation_params_from_toml as parse_and_add

    if add_to_db:
        params = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The EventDetectionParams were added to db with id {params.id}")
    else:
        parsing_results, _ = parse_and_add(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_qcone_config")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add QCOneConfig to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_qcone_config(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with QCOneConfig and add to db."""

    from noiz.api.processing_config import create_and_add_qcone_config_from_toml

    if add_to_db:
        params = create_and_add_qcone_config_from_toml(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The QCOneConfig was added to db with id {params.id}")
    else:
        parsing_results, _ = create_and_add_qcone_config_from_toml(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_qctwo_config")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add QCTwoConfig to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_qctwo_config(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with QCTwoConfig and add to db."""

    from noiz.api.processing_config import create_and_add_qctwo_config_from_toml

    if add_to_db:
        params = create_and_add_qctwo_config_from_toml(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The QCTwoConfig was added to db with id {params.id}")
    else:
        parsing_results, _ = create_and_add_qctwo_config_from_toml(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@configs_group.command("add_stacking_schema")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add StackingSchema to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_stacking_schema(
        filepath: str,
        add_to_db: bool,
        **kwargs
):
    """Read a TOML file with StackingSchema and add to db."""

    from noiz.api.processing_config import create_and_add_stacking_schema_from_toml
    from noiz.api.stacking import create_stacking_timespans_add_to_db

    if add_to_db:
        params = create_and_add_stacking_schema_from_toml(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo(f"The StackingSchema was added to db with if {params.id}")
        click.echo("Proceeding with creation of StackingTimespans")
        create_stacking_timespans_add_to_db(stacking_schema_id=params.id, bulk_insert=True)
    else:
        parsing_results, _ = create_and_add_stacking_schema_from_toml(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@data_group.group("data")
def data_group():  # type: ignore
    """Ingest raw data"""
    pass


@data_group.command("add_seismic_data")
@with_appcontext
@click.argument("basedir", nargs=-1, required=True, type=click.Path(exists=True))
@click.option("-fp", "--filename_pattern", default="*", show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_seismic_data(
        basedir,
        filename_pattern,
        parallel,
        **kwargs
):
    """Find seismic files in the directory and add them to DB"""

    from noiz.api.timeseries import add_seismic_data

    add_seismic_data(
        basedir=basedir,
        current_dir=Path(os.curdir),
        filename_pattern=filename_pattern,
        parallel=parallel,
    )

    return


@data_group.command("add_inventory")
@with_appcontext
@click.argument("filepath", nargs=1, required=True, type=click.Path(exists=True))
@click.option("-t", "--filetype", default="stationxml", show_default=True)
@click.option('--upsert/--no-upsert', default=False)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_inventory(
        filepath,
        filetype,
        upsert,
        **kwargs
):
    """Read the stationxml file and add components to database.
    Also creates all components pairs for added components."""

    from noiz.api.component import parse_inventory_insert_stations_and_components_into_db
    from noiz.api.component_pair import create_all_componentpairs_cartesian

    if upsert:
        raise NotImplementedError("This option is not implemented yet. You cannot update the inventory entries in DB.")

    parse_inventory_insert_stations_and_components_into_db(inventory_path=filepath, filetype=filetype)

    create_all_componentpairs_cartesian()
    return


@data_group.command("add_cylindrical_componentpair")
@with_appcontext
@click.option('--upsert/--no-upsert', default=False)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_cylindrical_component(
        upsert,
        **kwargs
):
    """
        From existing components in database, creating cylindrical components pair. Cartesian pairs need to be created before.
    """

    from noiz.api.component_pair import create_all_componentpairs_cylindrical

    create_all_componentpairs_cylindrical()

    return


@data_group.command("add_timespans")
@with_appcontext
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-wl", "--window_length", nargs=1, type=int, required=True)
@click.option("-wo", "--window_overlap", nargs=1, type=int, required=False)
@click.option('--generate_over_midnight', is_flag=True, expose_value=True)
@click.option('--bulk_insert/--no_bulk_insert', is_flag=True, expose_value=True, default=True, required=False)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add those timespans to DB? `N` will just preview it. ')
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_timespans(
        startdate,
        enddate,
        window_length,
        window_overlap,
        generate_over_midnight,
        bulk_insert,
        add_to_db,
        **kwargs
):
    """Generate Timespans and add them to database"""

    from noiz.api.timespan import create_and_insert_timespans_to_db

    if add_to_db:
        create_and_insert_timespans_to_db(
            startdate=startdate,
            enddate=enddate,
            window_length=window_length,
            window_overlap=window_overlap,
            generate_over_midnight=generate_over_midnight,
            bulk_insert=bulk_insert,
            add_to_db=add_to_db,
        )
    else:
        timespans = create_and_insert_timespans_to_db(
            startdate=startdate,
            enddate=enddate,
            window_length=window_length,
            window_overlap=window_overlap,
            generate_over_midnight=generate_over_midnight,
            add_to_db=add_to_db,
        )
        timespans = list(timespans)
        timespan_count = len(timespans)
        click.echo(f"There are {timespan_count} generated for that call. \nHere are 10 samples of them: \n")
        for i in range(11):
            idx = int(timespan_count*i/100)
            click.echo(f"Timespan at index {idx}:\n {timespans[idx]}")


@data_group.command("add_soh_dir")
@with_appcontext
@click.option("-s", "--station", required=True, type=str)
@click.option("-t", "--station_type", required=True, type=str)
@click.option("-p", "--soh_type", required=True, type=str)
@click.option("-n", "--network", type=str, default=None)
@click.argument("dirpath", nargs=1, type=click.Path(exists=True))
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_soh_dir(
        station,
        station_type,
        soh_type,
        dirpath,
        network,
        **kwargs
):
    """Find files and parse them for SOH information"""

    from noiz.api.soh import ingest_soh_files
    from noiz.exceptions import SohParsingException
    try:
        ingest_soh_files(
            station=station,
            station_type=station_type,
            soh_type=soh_type,
            main_filepath=dirpath,
            filepaths=None,
            network=network,
        )
    except SohParsingException as e:
        click.echo(e)

    return


@data_group.command("add_soh_files")
@with_appcontext
@click.option("-s", "--station", required=True, type=str)
@click.option("-t", "--station_type", required=True, type=str)
@click.option("-p", "--soh_type", required=True, type=str)
@click.option("-n", "--network", type=str, default=None)
@click.argument("paths", nargs=-1, type=click.Path(exists=True))
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def add_soh_files(
        station,
        station_type,
        soh_type,
        paths,
        network,
        **kwargs
):
    """Parse provided files for SOH information"""

    from noiz.api.soh import ingest_soh_files

    ingest_soh_files(
        station=station,
        station_type=station_type,
        soh_type=soh_type,
        main_filepath=None,
        filepaths=paths,
        network=network,
    )

    return


@processing_group.group("processing")
def processing_group():  # type: ignore
    """Data processing"""
    pass


@processing_group.command("prepare_datachunks")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--datachunk_params_id", nargs=1, type=int,
              default=1, show_default=True)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def prepare_datachunks(
        station,
        component,
        startdate,
        enddate,
        datachunk_params_id,
        batch_size,
        parallel,
        **kwargs
):
    """Start preparation of datachunks in linear or parallel fashion"""

    from noiz.api.datachunk import run_datachunk_preparation
    run_datachunk_preparation(
        stations=station,
        components=component,
        startdate=startdate,
        enddate=enddate,
        processing_config_id=datachunk_params_id,
        parallel=parallel,
        batch_size=batch_size,
    )


@processing_group.command("calc_datachunk_stats")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--datachunk_params_id", nargs=1, type=int, required=True)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def calc_datachunk_stats(
        station,
        component,
        startdate,
        enddate,
        datachunk_params_id,
        batch_size,
        parallel,
        **kwargs
):
    """Start parallel calculation of datachunk statistical parameters"""

    from noiz.api.datachunk import run_stats_calculation

    run_stats_calculation(
        stations=station,
        components=component,
        starttime=startdate,
        endtime=enddate,
        datachunk_params_id=datachunk_params_id,
        batch_size=batch_size,
        parallel=parallel,
    )


@processing_group.command("average_soh_gps")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-s", "--station", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def average_soh_gps(
        network,
        station,
        startdate,
        enddate,
        **kwargs
):
    """
    This command averages the GPS Soh data for timespans between starttime and endtime.
    The starttime and endtime are required because it could take too much time for processing everything at
    once by default.
    """

    from noiz.api.soh import average_raw_gps_soh
    average_raw_gps_soh(
        stations=station,
        networks=network,
        starttime=startdate,
        endtime=enddate,
    )


@processing_group.command("run_qcone")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--qcone_config_id", nargs=1, type=int, default=1, show_default=True)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def run_qcone(
        station,
        component,
        startdate,
        enddate,
        qcone_config_id,
        batch_size,
        parallel,
        **kwargs
):
    """Calculate QCOne results """

    from noiz.api.qc import process_qcone

    process_qcone(
        stations=station,
        components=component,
        starttime=startdate,
        endtime=enddate,
        qcone_config_id=qcone_config_id,
        batch_size=batch_size,
        parallel=parallel,
    )


@processing_group.command("run_beamforming")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--beamforming_params_id", multiple=True, type=int, required=True)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('--skip_existing/--no_skip_existing', default=True)
@click.option('--raise_errors/--no_raise_errors', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def run_beamforming(
        station,
        startdate,
        enddate,
        beamforming_params_id,
        batch_size,
        parallel,
        skip_existing,
        raise_errors,
        **kwargs
):
    """Start performing beamforming"""

    from noiz.api.beamforming import run_beamforming
    run_beamforming(
        stations=station,
        starttime=startdate,
        endtime=enddate,
        beamforming_params_ids=beamforming_params_id,
        batch_size=batch_size,
        parallel=parallel,
        skip_existing=skip_existing,
        raise_errors=raise_errors
    )


@processing_group.command("run_ppsd")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--ppsd_params_id", nargs=1, type=int, required=True)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('--skip_existing/--no_skip_existing', default=True)
@click.option('--raise_errors/--no_raise_errors', default=True)
def run_ppsd(
        station,
        component,
        startdate,
        enddate,
        ppsd_params_id,
        batch_size,
        parallel,
        skip_existing,
        raise_errors,
):
    """Start calculating psds"""

    from noiz.api.ppsd import run_psd_calculations
    run_psd_calculations(
        stations=station,
        components=component,
        starttime=startdate,
        endtime=enddate,
        ppsd_params_id=ppsd_params_id,
        batch_size=batch_size,
        parallel=parallel,
        skip_existing=skip_existing,
        raise_errors=raise_errors
    )


@processing_group.command("process_datachunks")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--processed_datachunk_params_id", nargs=1, type=int,
              default=1, show_default=True)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('--skip_existing/--no_skip_existing', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def process_datachunks(
        station,
        component,
        startdate,
        enddate,
        processed_datachunk_params_id,
        batch_size,
        parallel,
        skip_existing,
        **kwargs
):
    """Start processing of datachunks"""

    from noiz.api.datachunk import run_datachunk_processing
    run_datachunk_processing(
        stations=station,
        components=component,
        starttime=startdate,
        endtime=enddate,
        processed_datachunk_params_id=processed_datachunk_params_id,
        batch_size=batch_size,
        parallel=parallel,
        skip_existing=skip_existing,
    )


@processing_group.command("run_crosscorrelations_cartesian")
@with_appcontext
@click.option("-s", "--station_code", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component_code_pair", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--crosscorrelation_cartesian_params_id", nargs=1, type=int,
              default=1, show_default=True)
@click.option('-ia', '--include_autocorrelation', is_flag=True)
@click.option('-ii', '--include_intracorrelation', is_flag=True)
@click.option('--raise_errors/--no_raise_errors', default=False)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def run_crosscorrelations_cartesian(
        station_code,
        component_code_pair,
        startdate,
        enddate,
        crosscorrelation_cartesian_params_id,
        include_autocorrelation,
        include_intracorrelation,
        raise_errors,
        batch_size,
        parallel,
        **kwargs
):
    """Start processing of crosscorrelations_cartesian. Limited amount of pair selection arguments, use API directly if needed."""

    from noiz.api.crosscorrelations import perform_crosscorrelations_cartesian
    perform_crosscorrelations_cartesian(
        crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
        starttime=startdate,
        endtime=enddate,
        station_codes_a=station_code,
        accepted_component_code_pairs=component_code_pair,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        raise_errors=raise_errors,
        batch_size=batch_size,
        parallel=parallel,
    )


@processing_group.command("run_crosscorrelations_cylindrical")
@with_appcontext
@click.option("-s1", "--station_code1", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-s2", "--station_code2", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component_code_pair", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--crosscorrelation_cylindrical_params_id", nargs=1, type=int,
              default=1, show_default=True)
@click.option('--raise_errors/--no_raise_errors', default=False)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def run_crosscorrelations_cylindrical(
        station_code1,
        station_code2,
        component_code_pair,
        startdate,
        enddate,
        crosscorrelation_cylindrical_params_id,
        raise_errors,
        batch_size,
        parallel,
        **kwargs
):
    """Start processing of crosscorrelations_cylindrical. Limited amount of pair selection arguments, use API directly if needed."""

    from noiz.api.crosscorrelations import perform_crosscorrelations_cylindrical
    perform_crosscorrelations_cylindrical(
        crosscorrelation_cylindrical_params_id=crosscorrelation_cylindrical_params_id,
        starttime=startdate,
        endtime=enddate,
        station_codes_a=station_code1,
        station_codes_b=station_code2,
        accepted_component_code_pairs_cylindrical=component_code_pair,
        raise_errors=raise_errors,
        batch_size=batch_size,
        parallel=parallel,
    )


@processing_group.command("run_qctwo")
@with_appcontext
@click.option("-c", "--qctwo_config_id", nargs=1, type=int, default=1, show_default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def run_qctwo(
        qctwo_config_id,
        **kwargs
):
    """Calculate QCTwo results """

    from noiz.api.qc import process_qctwo

    process_qctwo(
        qctwo_config_id=qctwo_config_id,
    )


@processing_group.command("run_stacking")
@with_appcontext
@click.option("-s", "--station_code", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component_code_pair", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=False, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=False, callback=_parse_as_date)
@click.option("-p", "--stacking_schema_id", nargs=1, type=int,
              default=1, show_default=True)
@click.option('-ia', '--include_autocorrelation', is_flag=True)
@click.option('-ii', '--include_intracorrelation', is_flag=True)
@click.option('--raise_errors/--no_raise_errors', default=False)
@click.option("-b", "--batch_size", nargs=1, type=int, default=1000, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def run_stacking(
        station_code,
        component_code_pair,
        startdate,
        enddate,
        stacking_schema_id,
        include_autocorrelation,
        include_intracorrelation,
        raise_errors,
        batch_size,
        parallel,
        **kwargs
):
    """Start stacking of crosscorrelations_cartesian. Limited amount of pair selection arguments, use API directly if needed."""

    from noiz.api.stacking import stack_crosscorrelation_cartesian
    stack_crosscorrelation_cartesian(
        stacking_schema_id=stacking_schema_id,
        starttime=startdate,
        endtime=enddate,
        station_codes_a=station_code,
        accepted_component_code_pairs=component_code_pair,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        raise_errors=raise_errors,
        batch_size=batch_size,
        parallel=parallel,
    )


@processing_group.command("run_event_detection")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--event_detection_params_id", nargs=1, type=int, required=True)
@click.option("-b", "--batch_size", nargs=1, type=int, default=500, show_default=True)
@click.option('--plot_figures/--no_plot_figures', is_flag=True, expose_value=True, default=True, required=False)
@click.option('--parallel/--no_parallel', default=True)
@click.option('--skip_existing/--no_skip_existing', default=True)
@click.option('--raise_errors/--no_raise_errors', default=True)
def run_event_detection(
        network,
        station,
        component,
        startdate,
        enddate,
        event_detection_params_id,
        plot_figures,
        batch_size,
        parallel,
        skip_existing,
        raise_errors,
):
    """Start event detection"""

    from noiz.api.event_detection import perform_event_detection
    perform_event_detection(
        networks=network,
        stations=station,
        components=component,
        starttime=startdate,
        endtime=enddate,
        event_detection_params_id=event_detection_params_id,
        plot_figures=plot_figures,
        batch_size=batch_size,
        parallel=parallel,
        skip_existing=skip_existing,
        raise_errors=raise_errors
    )


@processing_group.command("run_event_confirmation")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-ex", "--exclude_station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sp", "--specific_params_station", multiple=True, type=str, callback=_validate_zero_length_as_none,
              help="arg must be in a station:event_detection_params_id format. Can be repeated.")
@click.option("-c", "--component", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--event_confirmation_params_id", nargs=1, type=int, required=True)
@click.option("-b", "--batch_size", nargs=1, type=int, default=128, show_default=True)
@click.option('--parallel/--no_parallel', default=True)
@click.option('--skip_existing/--no_skip_existing', default=True)
@click.option('--raise_errors/--no_raise_errors', default=True)
def run_event_confirmation(
        network,
        station,
        exclude_station,
        specific_params_station,
        component,
        startdate,
        enddate,
        event_confirmation_params_id,
        batch_size,
        parallel,
        skip_existing,
        raise_errors,
):
    """Start event confirmation"""

    from noiz.api.event_detection import perform_event_confirmation
    perform_event_confirmation(
        networks=network,
        stations=station,
        exclude_stations=exclude_station,
        specific_stations_params=specific_params_station,
        components=component,
        starttime=startdate,
        endtime=enddate,
        event_confirmation_params_id=event_confirmation_params_id,
        batch_size=batch_size,
        parallel=parallel,
        skip_existing=skip_existing,
        raise_errors=raise_errors
    )


@plotting_group.group("plot")
def plotting_group():  # type: ignore
    """Plotting routines"""
    pass


@plotting_group.command("datachunk_availability")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-s", "--station", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-c", "--component", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option("-p", "--processing_config_id", nargs=1, type=int,
              default=1, show_default=True)
@click.option('--savefig/--no-savefig', default=True)
@click.option('-pp', '--plotpath', type=click.Path())
@click.option('--showfig', is_flag=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def plot_datachunk_availability(
        network,
        station,
        component,
        startdate,
        enddate,
        datachunk_processing_params_id,
        savefig,
        plotpath,
        showfig
):
    """
    Plot datachunk availability
    """

    if savefig is True and plotpath is None:
        plotpath = Path('.')\
            .joinpath(f'datachunk_availability_{startdate}_{enddate}.png')
        click.echo(f"The --plotpath argument was not provided."
                   f"plot will be saved to {plotpath}")
    elif not isinstance(plotpath, Path):
        plotpath = Path(plotpath)

    from noiz.api.datachunk_plotting import plot_datachunk_availability

    plot_datachunk_availability(
        networks=network,
        stations=station,
        components=component,
        datachunk_processing_params_id=datachunk_processing_params_id,
        starttime=startdate,
        endtime=enddate,
        filepath=plotpath,
        showfig=showfig
    )
    return


@plotting_group.command("beamforming_freq_slowness")
@with_appcontext
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option("-p", "--beamforming_params_id", multiple=True, type=int, required=False,
              callback=_validate_zero_length_as_none)
@click.option("-t", "--beamforming_result_type", default="avg_abspower",
              type=click.Choice(["avg_abspower", "avg_relpower", "all_abspower", "all_relpower"], case_sensitive=False))
@click.option("-m", "--minimum_trace_used_count", nargs=1, type=int, default=None)
@click.option('--savefig/--no-savefig', default=True)
@click.option('-pp', '--plotpath', type=click.Path())
@click.option('--showfig', is_flag=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def plot_beamforming_freq_slowness(
        startdate,
        enddate,
        beamforming_params_id,
        beamforming_result_type,
        minimum_trace_used_count,
        savefig,
        plotpath,
        showfig,
        **kwargs
):
    """
    Plot beamforming results in frequency-slowness space
    """

    if savefig is True and plotpath is None:
        plotpath = Path('.') \
            .joinpath(f'beamforming_freq_slowness_{startdate}_{enddate}.png')
        click.echo(f"The --plotpath argument was not provided."
                   f"plot will be saved to {plotpath}")
    elif not isinstance(plotpath, Path):
        plotpath = Path(plotpath)

    from noiz.api.beamforming_plotting import plot_histogram_frequency_slowness_velocity

    plot_histogram_frequency_slowness_velocity(
        beamforming_params_ids=beamforming_params_id,
        minimum_trace_used_count=minimum_trace_used_count,
        beamforming_result_type=beamforming_result_type,
        histogram_space_type="slowness",
        fig_title=None,
        filepath=plotpath,
        showfig=showfig,
        starttime=startdate,
        endtime=enddate,
    )
    return


@plotting_group.command("beamforming_freq_velocity")
@with_appcontext
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option("-p", "--beamforming_params_id", multiple=True, type=int, required=False,
              callback=_validate_zero_length_as_none)
@click.option("-t", "--beamforming_result_type", default="avg_abspower",
              type=click.Choice(["avg_abspower", "avg_relpower", "all_abspower", "all_relpower"], case_sensitive=False))
@click.option("-m", "--minimum_trace_used_count", nargs=1, type=int, default=None)
@click.option('--savefig/--no-savefig', default=True)
@click.option('-pp', '--plotpath', type=click.Path())
@click.option('--showfig', is_flag=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def plot_beamforming_freq_velocity(
        startdate,
        enddate,
        beamforming_params_id,
        beamforming_result_type,
        minimum_trace_used_count,
        savefig,
        plotpath,
        showfig,
        **kwargs
):
    """
    Plot beamforming results in frequency-slowness space
    """

    if savefig is True and plotpath is None:
        plotpath = Path('.') \
            .joinpath(f'beamforming_freq_slowness_{startdate}_{enddate}.png')
        click.echo(f"The --plotpath argument was not provided."
                   f"plot will be saved to {plotpath}")
    elif not isinstance(plotpath, Path):
        plotpath = Path(plotpath)

    from noiz.api.beamforming_plotting import plot_histogram_frequency_slowness_velocity

    plot_histogram_frequency_slowness_velocity(
        beamforming_params_ids=beamforming_params_id,
        minimum_trace_used_count=minimum_trace_used_count,
        beamforming_result_type=beamforming_result_type,
        histogram_space_type="velocity",
        fig_title=None,
        filepath=plotpath,
        showfig=showfig,
        starttime=startdate,
        endtime=enddate,
    )
    return


@plotting_group.command("average_psd")
@with_appcontext
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option("-p", "--psd_params_id", nargs=1, multiple=False, type=int, required=True)
@click.option("-s", "--station", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-c", "--component_codes", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option('--showfig', is_flag=True)
@click.option('--show_legend/--no_show_legend', default=True)
@click.option('--savefig/--no-savefig', default=True)
@click.option('-pp', '--plotpath', type=click.Path())
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def plot_average_psd(
        startdate,
        enddate,
        psd_params_id,
        station,
        component_codes,
        savefig,
        show_legend,
        plotpath,
        showfig,
        **kwargs
):
    """
    Plot average PSD for multiple Components
    """

    if savefig is True and plotpath is None:
        plotpath = Path('.') \
            .joinpath(f'beamforming_freq_slowness_{startdate}_{enddate}.png')
        click.echo(f"The --plotpath argument was not provided."
                   f"plot will be saved to {plotpath}")
    elif not isinstance(plotpath, Path):
        plotpath = Path(plotpath)

    from noiz.api.psd_plotting import plot_average_psd_between_dates

    plot_average_psd_between_dates(
        starttime=startdate,
        endtime=enddate,
        ppsd_params_id=psd_params_id,
        stations=station,
        component_codes=component_codes,
        fig_title=None,
        show_legend=show_legend,
        filepath=plotpath,
        showfig=showfig,
    )
    return


@plotting_group.command("spectrogram")
@with_appcontext
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option("-p", "--psd_params_id", nargs=1, multiple=False, type=int, required=True)
@click.option("-s", "--station", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-c", "--component_codes", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-r", "--rolling_window_average", nargs=1, type=str)
@click.option('--log_freq_scale/--no_log_freq_scale', default=True)
@click.option('--showfig', is_flag=True)
@click.option('--savefig/--no-savefig', default=True)
@click.option('-pp', '--dirpath', type=click.Path(dir_okay=True, file_okay=False))
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def plot_spectrogram(
        startdate,
        enddate,
        psd_params_id,
        station,
        component_codes,
        rolling_window_average,
        log_freq_scale,
        savefig,
        dirpath,
        showfig,
        **kwargs
):
    """
    Plot spectrogram for multiple Components
    """

    if savefig is True and dirpath is None:
        dirpath = Path.cwd()
        click.echo(f"The --plotpath argument was not provided."
                   f"plots will be saved to {dirpath}")
    elif not isinstance(dirpath, Path):
        dirpath = Path(dirpath)
    dirpath.mkdir(exist_ok=True, parents=True)

    from noiz.api.psd_plotting import plot_spectrograms_between_dates

    plot_spectrograms_between_dates(
        starttime=startdate,
        endtime=enddate,
        ppsd_params_id=psd_params_id,
        stations=station,
        component_codes=component_codes,
        return_figures=False,
        fig_title=None,
        dirpath=dirpath,
        showfig=showfig,
        rolling_window=rolling_window_average,
        log_freq_scale=log_freq_scale,
    )
    return


@plotting_group.command("raw_gps_soh")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-s", "--station", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-sd", "--starttime", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--endtime", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option('--savefig/--no-savefig', default=True)
@click.option('-pp', '--plotpath', type=click.Path())
@click.option('--showfig', is_flag=True)
@click.option('--legend/--no-legend', default=True)
@click.option('--keep-empty/--no-keep-empty', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def plot_raw_gps_soh(
        network,
        station,
        starttime,
        endtime,
        savefig,
        plotpath,
        showfig,
        legend,
        keep_empty,
        **kwargs
):
    """
    Plot raw GPS SOH
    """

    if savefig is True and plotpath is None:
        plotpath = Path('.')\
            .joinpath(f'raw_gps_soh_{starttime}_{endtime}.png')
        click.echo(f"The --plotpath argument was not provided."
                   f"plot will be saved to {plotpath}")
    elif not isinstance(plotpath, Path):
        plotpath = Path(plotpath)

    from noiz.api.soh_plotting import plot_raw_gps_data_availability

    plot_raw_gps_data_availability(
        networks=network,
        stations=station,
        starttime=starttime,
        endtime=endtime,
        filepath=plotpath,
        showfig=showfig,
        show_legend=legend,
        keep_empty=keep_empty
    )
    return


@plotting_group.command("averaged_gps_soh")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-s", "--station", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-sd", "--starttime", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--endtime", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option('--savefig/--no-savefig', default=True)
@click.option('-pp', '--plotpath', type=click.Path())
@click.option('--showfig', is_flag=True)
@click.option('--legend/--no-legend', default=True)
@click.option('--keep-empty/--no-keep-empty', default=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def plot_averaged_gps_soh(
        network,
        station,
        starttime,
        endtime,
        savefig,
        plotpath,
        showfig,
        legend,
        keep_empty,
        **kwargs
):
    """
    Plot Averaged GPS SOH based
    """

    if savefig is True and plotpath is None:
        plotpath = Path('.')\
            .joinpath(f'raw_gps_soh_{starttime.date()}_{endtime.date()}.png')
        click.echo(f"The --plotpath argument was not provided."
                   f"plot will be saved to {plotpath}")
    elif savefig is True and not isinstance(plotpath, Path):
        plotpath = Path(plotpath)

    from noiz.api.soh_plotting import plot_averaged_gps_data_availability

    plot_averaged_gps_data_availability(
        networks=network,
        stations=station,
        starttime=starttime,
        endtime=endtime,
        filepath=plotpath,
        showfig=showfig,
        show_legend=legend,
        keep_empty=keep_empty,
    )
    return


@export_group.group("export")
def export_group():  # type: ignore
    """Data exporting"""
    pass


@export_group.command("raw_gps_soh")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-s", "--station", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option('-p', '--path', type=click.Path())
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def export_raw_gps_soh(
        network,
        station,
        startdate,
        enddate,
        path,
        **kwargs
):
    """
    Export raw GPS SOH data to CSV.
    """

    if path is None:
        path = Path('.') \
            .joinpath(f'raw_gps_soh_{startdate}_{enddate}.csv')
        click.echo(f"The --path argument was not provided."
                   f"File will be saved to {path}")
    elif not isinstance(path, Path):
        path = Path(path)

    from noiz.api.soh import export_raw_soh_gps_data_to_csv

    export_raw_soh_gps_data_to_csv(
        networks=network,
        stations=station,
        starttime=startdate,
        endtime=enddate,
        filepath=path
    )

    return


@export_group.command("raw_ccfs")
@with_appcontext
@click.option("-p", "--crosscorrelation_cartesian_params_id", nargs=1, type=int,
              default=1, show_default=True)
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=DEFAULT_STARTDATE, show_default=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=DEFAULT_ENDDATE, show_default=True, callback=_parse_as_date)
@click.option("-d", "--dirpath", type=click.Path())
@click.option("--overwrite", is_flag=True)
@click.option("-na", "--network_codes_a", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-sa", "--station_codes_a", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-ca", "--component_codes_a", multiple=True, type=str, default=None,
              callback=_validate_zero_length_as_none)
@click.option("-nb", "--network_codes_b", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-sb", "--station_codes_b", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-cb", "--component_codes_b", multiple=True, type=str, default=None,
              callback=_validate_zero_length_as_none)
@click.option("-cp", "--accepted_component_code_pairs", multiple=True, type=str, default=None,
              callback=_validate_zero_length_as_none)
@click.option("--include_autocorrelation", is_flag=True)
@click.option("--include_intracorrelation", is_flag=True)
@click.option("--only_autocorrelation", is_flag=True)
@click.option("--only_intracorrelation", is_flag=True)
@click.option('-v', '--verbose', count=True, callback=_setup_logging_verbosity)
@click.option('--quiet', is_flag=True, callback=_setup_quiet)
def export_raw_ccfs(
        crosscorrelation_cartesian_params_id,
        startdate,
        enddate,
        dirpath,
        overwrite,
        network_codes_a,
        station_codes_a,
        component_codes_a,
        network_codes_b,
        station_codes_b,
        component_codes_b,
        accepted_component_code_pairs,
        include_autocorrelation,
        include_intracorrelation,
        only_autocorrelation,
        only_intracorrelation,
        **kwargs
):
    """
    Export raw crosscorrelation_cartesian data to npz file.
    """

    if dirpath is None:
        dirpath = Path('.')
        click.echo(f"The --dirpath argument was not provided."
                   f"Files will be saved to {dirpath}")
    elif not isinstance(dirpath, Path):
        dirpath = Path(dirpath)

    from noiz.api.crosscorrelations_cartesian import fetch_crosscorrelations_cartesian_and_save

    fetch_crosscorrelations_cartesian_and_save(
        crosscorrelation_cartesian_params_id=crosscorrelation_cartesian_params_id,
        starttime=startdate,
        endtime=enddate,
        dirpath=dirpath,
        overwrite=overwrite,
        network_codes_a=network_codes_a,
        station_codes_a=station_codes_a,
        component_codes_a=component_codes_a,
        network_codes_b=network_codes_b,
        station_codes_b=station_codes_b,
        component_codes_b=component_codes_b,
        accepted_component_code_pairs=accepted_component_code_pairs,
        include_autocorrelation=include_autocorrelation,
        include_intracorrelation=include_intracorrelation,
        only_autocorrelation=only_autocorrelation,
        only_intracorrelation=only_intracorrelation,
    )

    return


_register_subgroups_to_cli(
    cli,
    (
        configs_group,
        data_group,
        processing_group,
        plotting_group,
        export_group
    )
)


if __name__ == "__main__":
    cli()

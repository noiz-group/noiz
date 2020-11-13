# mypy: ignore-errors

import click
import logging
import os
import pendulum

from flask.cli import AppGroup, with_appcontext
from flask import current_app
from flask.cli import FlaskGroup
from pendulum.date import Date
from pathlib import Path
from typing import Iterable

from noiz.api.inventory import parse_inventory_insert_stations_and_components_into_db
from noiz.api.processing_config import upsert_default_params
from noiz.app import create_app
from noiz.processing.inventory import read_inventory

log = logging.getLogger(__name__)

cli = AppGroup("noiz")
configs_group = AppGroup("configs")  # type: ignore
data_group = AppGroup("data")  # type: ignore
qc_group = AppGroup("qc")  # type: ignore
processing_group = AppGroup("processing")  # type: ignore
plotting_group = AppGroup("plotting")  # type: ignore

DEFAULT_STARTDATE = pendulum.Pendulum(2010, 1, 1).date()
DEFAULT_ENDDATE = pendulum.today().date()


def _register_subgroups_to_cli(cli: AppGroup, custom_groups: Iterable[AppGroup]):
    for custom_group in custom_groups:
        cli.add_command(custom_group)
    return


def _parse_as_date(ctx, param, value) -> Date:
    """
    This method is used internally as a callback for date arguments to parse the input string and
    return a :class:`pendulum.date.Date` object
    """
    if not isinstance(value, Date):
        return pendulum.parse(value).date()
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
def cli():  # type: ignore
    """Perform operations with noiz package"""
    pass


@configs_group.group("configs")
def configs_group():  # type: ignore
    """Initiate operation in noiz"""
    pass


@configs_group.command("load_processing_params")
def load_processing_params():
    """Replaces current processing config with default one"""
    click.echo("This is a placeholder of an option")


@configs_group.command("reset_config")
def reset_config():
    """Replaces current processing config with default one"""
    upsert_default_params()


@data_group.group("data")
def data_group():  # type: ignore
    """Ingest raw data by Noiz"""
    pass


@data_group.command("add_files_recursively")
@click.argument("paths", nargs=-1, required=True, type=click.Path(exists=True))
def add_files_recursively(paths):
    """Globs over provided directories in search of files"""

    click.echo("Unfortunately this option is not implemented yet.")
    click.echo("You need to run this command")
    click.echo(
        f"{os.environ['MSEEDINDEX_EXECUTABLE']} -v "
        f"-pghost {os.environ['POSTGRES_HOST']} "
        f"-dbuser {os.environ['POSTGRES_USER']} "
        f"-dbpass {os.environ['POSTGRES_PASSWORD']} "
        f"-dbname {os.environ['POSTGRES_DB']} "
        f"{' '.join(paths.rglob('*'))}"
    )
    return


@data_group.command("add_inventory")
@with_appcontext
@click.argument("filepath", nargs=1, required=True, type=click.Path(exists=True))
@click.option("-t", "--filetype", default="stationxml", show_default=True)
def add_inventory(filepath, filetype):
    """Globs over provided directories in search of files"""

    inventory = read_inventory(filepath=filepath, filetype=filetype)

    parse_inventory_insert_stations_and_components_into_db(
        app=current_app, inventory=inventory
    )
    return


@data_group.command("add_soh_dir")
@with_appcontext
@click.option("-s", "--station", required=True, type=str)
@click.option("-t", "--station_type", required=True, type=str)
@click.option("-p", "--soh_type", required=True, type=str)
@click.option("-n", "--network", type=str, default=None)
@click.option("-d", "--dirpath", nargs=1, type=click.Path(exists=True))
def add_soh_dir(station, station_type, soh_type, dirpath, network):
    """Globs over provided directories in search of soh files fitting parsing requirements"""

    from noiz.api.soh import ingest_soh_files

    ingest_soh_files(
        station=station,
        station_type=station_type,
        soh_type=soh_type,
        main_filepath=dirpath,
        filepaths=None,
        network=network,
    )

    return


@data_group.command("add_soh_files")
@with_appcontext
@click.option("-s", "--station", required=True, type=str)
@click.option("-t", "--station_type", required=True, type=str)
@click.option("-p", "--soh_type", required=True, type=str)
@click.option("-n", "--network", type=str, default=None)
@click.argument("paths", nargs=-1, type=click.Path(exists=True))
def add_soh_files(station, station_type, soh_type, paths, network):
    """Globs over provided directories in search of soh files fitting parsing requirements"""

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


@qc_group.group("qc")
def qc_group():  # type: ignore
    """Manage QCConfigs"""
    pass


@qc_group.command("read_qc_one_config")
@with_appcontext
@click.option("-f", "--filepath", nargs=1, type=click.Path(exists=True), required=True)
@click.option('--add_to_db', is_flag=True, expose_value=True,
              prompt='Are you sure you want to add QCOneConfig to DB? `N` will just preview it. ')
def read_qc_one_config(
        filepath: str,
        add_to_db: bool,
):
    """This command allows for reading a TOML file with QCOne config and adding it to database"""

    from noiz.api.qc import create_and_add_qc_one_config_from_toml

    if add_to_db:
        create_and_add_qc_one_config_from_toml(filepath=Path(filepath), add_to_db=add_to_db)
    else:
        parsing_results, _ = create_and_add_qc_one_config_from_toml(filepath=Path(filepath), add_to_db=add_to_db)
        click.echo("\n")
        click.echo(parsing_results)


@processing_group.group("processing")
def processing_group():  # type: ignore
    """Processing subcommands"""
    pass


@processing_group.command("prepare_datachunks")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-c", "--component", multiple=True, type=str, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-p", "--processing_config_id", nargs=1, type=int,
              default=1, show_default=True)
def prepare_datachunks(
        station,
        component,
        startdate,
        enddate,
        processing_config_id
):
    """This command starts parallel processing of datachunks"""

    from noiz.api.datachunk import run_paralel_chunk_preparation

    run_paralel_chunk_preparation(
        stations=station,
        components=component,
        startdate=startdate,
        enddate=enddate,
        processing_config_id=processing_config_id
    )


@processing_group.command("average_soh_gps")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-s", "--station", multiple=True, type=str, default=None, callback=_validate_zero_length_as_none)
@click.option("-sd", "--startdate", nargs=1, type=str, required=True, callback=_parse_as_date)
@click.option("-ed", "--enddate", nargs=1, type=str, required=True, callback=_parse_as_date)
def average_soh_gps(
        network,
        station,
        startdate,
        enddate,
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


@plotting_group.group("plotting")
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
    Method to plot datachunk availability based on passed arguments.
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
def plot_raw_gps_soh(
        network,
        station,
        starttime,
        endtime,
        savefig,
        plotpath,
        showfig,
        legend
):
    """
    Method to plot raw GPS SOH based on passed arguments.
    """

    if savefig is True and plotpath is None:
        plotpath = Path('.')\
            .joinpath(f'raw_gps_soh_{starttime.date()}_{endtime.date()}.png')
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
def averaged_gps_soh(
        network,
        station,
        starttime,
        endtime,
        savefig,
        plotpath,
        showfig,
        legend
):
    """
    Method to plot Averaged GPS SOH based on passed arguments.
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
    )
    return


_register_subgroups_to_cli(cli, (configs_group, data_group, processing_group, qc_group, plotting_group))


if __name__ == "__main__":
    cli()

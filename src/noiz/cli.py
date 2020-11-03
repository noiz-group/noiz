# mypy: ignore-errors
from pathlib import Path

import os
from typing import Iterable

import click
from flask.cli import AppGroup, with_appcontext
from flask import current_app
from flask.cli import FlaskGroup

import logging
import pendulum
from pendulum.date import Date

import noiz
from noiz.api.inventory import parse_inventory_insert_stations_and_components_into_db
from noiz.api.processing_config import upsert_default_params
from noiz.processing.inventory import read_inventory

from noiz.app import create_app

log = logging.getLogger(__name__)

cli = AppGroup("noiz")
configs_group = AppGroup("configs")  # type: ignore
data_group = AppGroup("data")  # type: ignore
processing_group = AppGroup("processing")  # type: ignore
plotting_group = AppGroup("plotting")  # type: ignore


def _register_subgroups_to_cli(cli: AppGroup, custom_groups: Iterable[AppGroup]):
    for custom_group in custom_groups:
        cli.add_command(custom_group)
    return


@cli.group("noiz", cls=FlaskGroup, create_app=create_app)
def cli():  # type: ignore
    "Perform operations with noiz package"
    pass


@configs_group.group("configs")
def configs_group():  # type: ignore
    "Initiate operation in noiz"
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
    "Ingest raw data by Noiz"
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
        f"-dbname {os.environ['POSTGRES_DB_NOIZ']} "
        f"{' '.join(paths)}"
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


@processing_group.group("processing")
def processing_group():  # type: ignore
    """Processing subcommands"""
    pass


@processing_group.command("prepare_datachunks")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str)
@click.option("-c", "--component", multiple=True, type=str)
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=pendulum.Pendulum(2010, 1, 1).date, show_default=True)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=pendulum.today().date, show_default=True)
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

    if not isinstance(startdate, Date):
        startdate = pendulum.parse(startdate).date()
    if not isinstance(enddate, Date):
        enddate = pendulum.parse(enddate).date()

    if len(station) == 0:
        station = None
    if len(component) == 0:
        component = None

    from noiz.api.datachunk import run_paralel_chunk_preparation

    run_paralel_chunk_preparation(
        stations=station,
        components=component,
        startdate=startdate,
        enddate=enddate,
        processing_config_id=processing_config_id
    )


@plotting_group.group("plotting")
def plotting_group():  # type: ignore
    """Plotting routines"""
    pass


@plotting_group.command("datachunk_availability")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, default=None)
@click.option("-s", "--station", multiple=True, type=str, default=None)
@click.option("-c", "--component", multiple=True, type=str, default=None)
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=pendulum.Pendulum(2010, 1, 1).date, show_default=True)
@click.option("-ed", "--enddate", nargs=1, type=str,
              default=pendulum.today().date, show_default=True)
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
        processing_config_id,
        savefig,
        plotpath,
        showfig
):
    """
    Method to plot datachunk availability based on passed arguments.
    """
    if not isinstance(startdate, Date):
        startdate = pendulum.parse(startdate).date()
    if not isinstance(enddate, Date):
        enddate = pendulum.parse(enddate).date()

    if len(network) == 0:
        network = None
    if len(station) == 0:
        station = None
    if len(component) == 0:
        component = None

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
        processingparams_id=processing_config_id,
        starttime=startdate,
        endtime=enddate,
        filepath=plotpath,
        showfig=showfig
    )
    return


@plotting_group.command("raw_gps_soh")
@with_appcontext
@click.option("-n", "--network", multiple=True, type=str, default=None)
@click.option("-s", "--station", multiple=True, type=str, default=None)
@click.option("-sd", "--starttime", nargs=1, type=str,
              default=pendulum.Pendulum(2010, 1, 1), show_default=True)
@click.option("-ed", "--endtime", nargs=1, type=str,
              default=pendulum.now(), show_default=True)
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

    if not isinstance(starttime, Date):
        starttime = pendulum.parse(starttime)
    if not isinstance(endtime, Date):
        endtime = pendulum.parse(endtime)

    if len(network) == 0:
        network = None
    if len(station) == 0:
        station = None

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
@click.option("-n", "--network", multiple=True, type=str, default=None)
@click.option("-s", "--station", multiple=True, type=str, default=None)
@click.option("-sd", "--starttime", nargs=1, type=str,
              default=pendulum.Pendulum(2010, 1, 1), show_default=True)
@click.option("-ed", "--endtime", nargs=1, type=str,
              default=pendulum.now(), show_default=True)
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

    if not isinstance(starttime, Date):
        starttime = pendulum.parse(starttime)
    if not isinstance(endtime, Date):
        endtime = pendulum.parse(endtime)

    if len(network) == 0:
        network = None
    if len(station) == 0:
        station = None

    if savefig is True and plotpath is None:
        plotpath = Path('.')\
            .joinpath(f'raw_gps_soh_{starttime.date()}_{endtime.date()}.png')
        click.echo(f"The --plotpath argument was not provided."
                   f"plot will be saved to {plotpath}")
    elif not isinstance(plotpath, Path):
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


_register_subgroups_to_cli(cli, (configs_group, data_group, processing_group, plotting_group))


if __name__ == "__main__":
    cli()

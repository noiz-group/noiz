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
from noiz.api.datachunk import run_paralel_chunk_preparation
from noiz.processing.inventory import read_inventory

from noiz.app import create_app

log = logging.getLogger(__name__)

cli = AppGroup("noiz")
init_group = AppGroup("init")  # type: ignore
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


@init_group.group("init")
def init_group():  # type: ignore
    "Initiate operation in noiz"
    pass

@init_group.command("load_processing_params")
def load_processing_params():
    """Replaces current processing config with default one"""
    click.echo("This is a placeholder of an option")

@init_group.command("reset_config")
def reset_config():
    """Replaces current processing config with default one"""
    upsert_default_params()


@init_group.command("add_files_recursively")
@click.argument("paths", nargs=-1, required=True, type=click.Path(exists=True))
def add_files_recursively(paths):
    """Globs over provided directories in search of files"""

    click.echo(f"Unfortunately this option is not implemented yet.")
    click.echo(f"You need to run this command")
    click.echo(
        f"{os.environ['MSEEDINDEX_EXECUTABLE']} -v "
        f"-pghost {os.environ['POSTGRES_HOST']}"
        f"-dbuser {os.environ['POSTGRES_USER']} "
        f"-dbpass {os.environ['POSTGRES_PASSWORD']} "
        f"-dbname {os.environ['POSTGRES_DB_NOIZ']} "
        f"{' '.join(paths)}"
    )
    return


@init_group.command("add_inventory")
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


@processing_group.group("processing")
def processing_group():  # type: ignore
    """Processing subcommands"""
    pass


@processing_group.command("prepare_datachunks")
@with_appcontext
@click.option("-s", "--station", multiple=True, type=str)
@click.option("-c", "--component", multiple=True, type=str)
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=pendulum.Pendulum(2000,1,1).date, show_default=True)
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
@click.option("-n", "--network", multiple=True, type=str)
@click.option("-s", "--station", multiple=True, type=str)
@click.option("-c", "--component", multiple=True, type=str)
@click.option("-sd", "--startdate", nargs=1, type=str,
              default=pendulum.Pendulum(2000,1,1).date, show_default=True)
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
    elif len(network) == 1:
        network = tuple(network)
    if len(station) == 0:
        station = None
    elif len(station) == 1:
        station = tuple(station)
    if len(component) == 0:
        component = None
    elif len(component) == 1:
        component = tuple(component)

    if savefig is True and plotpath is None:
        plotpath = Path('.')\
            .joinpath(f'datachunk_availability_{startdate}_{enddate}.png')
        click.echo(f"The --plotpath argument was not provided."
                   f"plot will be saved to {plotpath}")
    elif not isinstance(plotpath, Path):
        plotpath = Path(plotpath)

    noiz.api.datachunk_plotting.plot_datachunk_availability(
        networks=network,
        stations=station,
        components=component,
        processingparams_id=processing_config_id,
        starttime=startdate,
        endtime=enddate,
        filepath=plotpath,
        showfig=showfig
    )


_register_subgroups_to_cli(cli, (init_group, processing_group, plotting_group))


if __name__ == "__main__":
    cli()

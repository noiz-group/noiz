# mypy: ignore-errors
import os
from typing import Iterable

import click
from flask.cli import AppGroup, with_appcontext
from flask import current_app
from flask.cli import FlaskGroup


from noiz.api.inventory import parse_inventory_insert_stations_and_components_into_db
from noiz.api.processing_config import upsert_default_params
from noiz.processing.datachunk_preparation import run_paralel_chunk_preparation
from noiz.processing.inventory import read_inventory

from noiz.app import create_app


cli = AppGroup("noiz")
init_group = AppGroup("init")  # type: ignore
processing_group = AppGroup("processing")  # type: ignore


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
@click.option("-a", "--station", multiple=True, required=True, type=str)
@click.option("-c", "--component", multiple=True, required=True, type=str)
@click.option("-s", "--startdate", nargs=1, required=True, type=str)
@click.option("-e", "--enddate", nargs=1, required=True, type=str)
def prepare_datachunks(station, component, startdate, enddate):
    """That's the explanation of second command of the group"""
    run_paralel_chunk_preparation(
        stations=station, components=component, startdate=startdate, enddate=enddate
    )

_register_subgroups_to_cli(cli, (init_group, processing_group))


if __name__ == "__main__":
    cli()

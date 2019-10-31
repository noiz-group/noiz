import click
import logging
from flask.cli import AppGroup, with_appcontext, current_app

from noiz.database import db

from noiz.processing.processing_config import upsert_default_params
from noiz.processing.file import search_for_seismic_files, get_not_processed_files

# from noiz.processing.trace import scan_file_for_traces
from noiz.processing.inventory import (
    parse_inventory_insert_stations_and_components_into_db,
    read_inventory,
)

from pathlib import Path

logger = logging.getLogger(__name__)

# from celery import group

cli = AppGroup("Main")
init_group = AppGroup("init")
flask_custom_cli = AppGroup("noizfff")


def _register_subgroups_to_cli(cli: AppGroup):
    for custom_group in (init_group, flask_custom_cli):
        cli.add_command(custom_group)
    return


@init_group.group("init-noiz")
def init_group():
    "Initiate operation in noiz"
    pass


@init_group.command("reset_config")
def reset_config():
    """Replaces current processing config with default one"""
    upsert_default_params()


@init_group.command("add_files_recursively")
@click.argument("paths", nargs=-1, required=True, type=click.Path(exists=True))
@click.option("-g", "--glob", default="*", show_default=True)
@click.option("-t", "--filetype", default="mseed", show_default=True)
@click.option("-f", "--commit_frequency", default=150, show_default=True)
def add_files_recursively(paths, glob, filetype, commit_frequency):
    """Globs over provided directories in search of files"""

    search_for_seismic_files(
        paths=paths, glob=glob, commit_frequency=commit_frequency, filetype=filetype
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


# @init_group.command("scan_files")
# def scan_files():
#     """Replaces current processing config with default one"""
#
#     res = []
#     count = 0
#     for r1, r2 in izip(FastqGeneralIterator(f1), FastqGeneralIterator(f2)):
#         count += 1
#         res.append(tasks.process_read_pair.s(r1, r2))
#         if count == 10000:
#             break
#
#
#     g = group(res)
#     for task in g.tasks:
#         task.set()
#
#
#     rlist = []
#     for file in get_not_processed_files(session=db.session):
#         logger.info(f'Pushing {file} to worker')
#         rlist.extend(scan_file_for_traces(session=db.session ,file=file))
#
#     db.session.add_all(rlist)


@flask_custom_cli.group("This is explanation of the first group")
def flask_custom_cli():
    """This is short explanation?"""
    pass


@flask_custom_cli.command()
def firstf():
    """That's the explanation of first command of the group"""
    click.echo("That's the first command of the group")


@flask_custom_cli.command()
# @click.pass_context
def secondf():
    """That's the explanation of second command of the group"""
    click.echo("That's the second command of the group")


_register_subgroups_to_cli(cli)


if __name__ == "__main__":
    cli()

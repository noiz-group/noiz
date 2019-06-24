import click
from flask.cli import AppGroup

from pathlib import Path

from noiz.processing.processing_config import upsert_default_config
from noiz.processing.file import insert_seismic_files_recursively

user_cli_group = AppGroup('init', help='Performs actions to initiate')
flask_custom_cli = AppGroup('noizfff')

@user_cli_group.group("""Introductory actions in the noiz app""")
def user_cli_group():
    pass

@user_cli_group.command('reset_config')
def reset_config():
    '''Replaces current processing config with default one'''
    upsert_default_config()

@user_cli_group.command('add_files_recursively')
# @click.Path('-p', '--path', dir_okay=True, readable=True)\
@click.option('--path', required=True)
@click.option('-g', '--glob', default='*', show_default=True)
@click.option('-t', '--filetype', default='mseed', show_default=True)
@click.option('-f', '--commit_frequency', default=150, show_default=True)
def add_files_recursively(path, glob, filetype, commit_frequency):

    path = Path(path)
    if path.exists():
        insert_seismic_files_recursively(main_path=path,
                                         glob_call=glob,
                                         filetype=filetype,
                                         commit_freq=commit_frequency)
    else:
        raise ValueError(f'Provided path {path} does not exist.')
    return



@flask_custom_cli.group("This is explanation of the first group")
def flask_custom_cli():
    '''This is short explanation?'''
    pass

@flask_custom_cli.command()
def firstf():
    '''That's the explanation of first command of the group'''
    click.echo("That's the first command of the group")


@flask_custom_cli.command()
# @click.pass_context
def secondf():
    '''That's the explanation of second command of the group'''
    click.echo("That's the second command of the group")


cli = click.CommandCollection(sources=[user_cli_group, flask_custom_cli])


if __name__ == '__main__':
    cli()
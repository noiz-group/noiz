import click
from flask.cli import AppGroup

from processing.processing_config import upsert_default_config

user_cli_group = AppGroup('init', help='Performs actions to initiate')
flask_custom_cli = AppGroup('noizfff')

@user_cli_group.group("""Introductory actions in the noiz app""")
def user_cli_group():
    pass

@user_cli_group.command('populate_config')
def populate_config():
    '''Replaces current processing config with default one'''
    upsert_default_config()

@user_cli_group.command()
def second():
    '''That's the explanation of second command of the group'''
    click.echo("That's the second command of the group")




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
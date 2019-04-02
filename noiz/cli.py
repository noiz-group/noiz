import click
from flask import Flask
from flask.cli import AppGroup, FlaskGroup

user_cli = AppGroup('noiz')

@user_cli.group("This is explanation of the first group")
def user_cli_group():
    '''This is short explanation?'''
    pass

@user_cli.command()
def first():
    '''That's the explanation of first command of the group'''
    click.echo("That's the first command of the group")


# @click.command()
# @click.pass_context
# def init_db():
#     app = create_app()
#     db.create_all(app)

@user_cli.command()
# @click.pass_context
def second():
    '''That's the explanation of second command of the group'''
    click.echo("That's the second command of the group")

# app.cli.add_command(user_cli)

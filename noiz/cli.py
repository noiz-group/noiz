import click
from flask import Flask, current_app
from flask.cli import AppGroup, FlaskGroup

from noiz.models.processingconfig import ProcessingConfig
from noiz.extensions import db

user_cli_group = AppGroup('init', help='Performs actions to initiate')
flask_custom_cli = AppGroup('noizfff')

@user_cli_group.group("""Introductory actions in the noiz app""")
def user_cli_group():
    pass

@user_cli_group.command('populate_config')
def populate_config():
    '''That's the explanation of first command of the group'''

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



def upsert_default_config():

    default_config = ProcessingConfig(
            use_winter_time=False,
            f_sampling_out=25,
            downsample_filt_order=3,
            window_length_spectrum_sec=100,
            window_spectrum_overlap=0.1,
            window_spectrum_reject_method=None,
            window_spectrum_crit_std=3,
            window_spectrum_crit_quantile=0.1,
            window_spectrum_num_for_stat=10,
            sequence_length_sec=1800,
            sequence_overlap=0,
            sequence_reject_method=None,
            sequence_crit_std=0,
            sequence_crit_quantile=0,
            sequence_num_for_stat=10,
            sequence_reject_tolerance_on_window_spectrum=5,
            f_min_reject_crit=0.01,
            f_max_reject_crit=12,
            filter_type='butterworth',
            taper_time_type='cosine',
            taper_time_width_periods=200,
            taper_time_width_min_samples=25,
            taper_time_width_max_proportion=0.1,
            taper_freq_type='cosine',
            taper_freq_width_proportion=0.1,
            taper_freq_width_min_samples=100,
            taper_freq_width_max_freqs=25,
        )

    current_config = db.session.query(ProcessingConfig).first()

    if  current_config is not None:
        db.session.delete(current_config)

    db.session.add(default_config)
    db.session.commit()

    return

cli = click.CommandCollection(sources=[user_cli_group, flask_custom_cli])


if __name__ == '__main__':
    cli()
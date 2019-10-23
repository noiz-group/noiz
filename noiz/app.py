from flask import Flask
from flask.helpers import get_root_path
import os

# from celery import Celery
import dash
import logging.config

from noiz.logg import logger_config
from noiz.routes import simple_page
from noiz.database import db, migrate


# from noiz.cli import cli
# from noiz.tasks import celery


# celery = Celery(__name__,
#                 broker=noiz.settings.CELERY_BROKER_URL,
#                 backend=noiz.settings.CELERY_RESULT_BACKEND)


def create_app(config_object="noiz.settings", mode="app"):
    app = Flask(__name__)
    app.config.from_object(config_object)

    register_extensions(app)
    register_blueprints(app)
    register_cli_extensions(app)
    # configure_celery(app, celery)

    logger = configure_logger(app)
    logger.info("App initialization successful")

    register_dashapps(app)

    load_noiz_config(app)
    app.processing_config = fetch_processing_config(app)

    if mode == "app":
        return app
    # if mode == 'celery':
    #     return celery


def register_dashapps(app):
    app.logger.info("Starting to initailize dashapps")
    from noiz.dashapp.layout import layout
    from noiz.dashapp.callbacks import register_callbacks

    # Meta tags for viewport responsiveness
    meta_viewport = {
        "name": "viewport",
        "content": "width=device-width, initial-scale=1, shrink-to-fit=no",
    }
    #
    dashapp1 = dash.Dash(
        __name__,
        server=app,
        url_base_pathname="/dashboard/",
        assets_folder=get_root_path(__name__) + "/dashboard/assets/",
        meta_tags=[meta_viewport],
    )
    #
    with app.app_context():
        dashapp1.title = "Dashapp 1"
        dashapp1.layout = layout
        register_callbacks(dashapp1)


def configure_celery(app, celery):

    # set broker url and result backend from app config
    celery.conf.broker_url = app.config["CELERY_BROKER_URL"]
    celery.conf.result_backend = app.config["CELERY_RESULT_BACKEND"]

    # subclass task base for app context
    # http://flask.pocoo.org/docs/0.12/patterns/celery/
    TaskBase = celery.Task

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask

    # run finalize to process decorated tasks
    celery.finalize()
    return


def load_noiz_config(app):
    app.noiz_config = {}
    processed_data_dir = os.environ.get("PROCESSED_DATA_DIR")
    if processed_data_dir is None:
        raise ValueError("You have to set a PROCESSED_DATA_DIR env variable.")
    app.noiz_config["processed_data_dir"] = processed_data_dir


def register_extensions(app):
    db.init_app(app)
    import noiz.models

    migrate.init_app(app, db)
    return None


def register_blueprints(app):
    app.register_blueprint(simple_page)
    return None


def register_cli_extensions(app):
    # app.cli.add_command(cli)

    return None


def configure_logger(app):
    """Configure loggers."""

    logging.config.dictConfig(logger_config)
    logger = logging.getLogger("app")
    logger.debug("Initializing logger")
    return logger


def fetch_processing_config(app):
    # app.
    return True

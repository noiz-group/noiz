from flask import Flask
from flask.helpers import get_root_path
import os

import dash
import logging.config

from noiz.logg import logger_config
from noiz.routes import simple_page
from noiz.database import db, migrate


def create_app(config_object="noiz.settings", mode="app", external_logger=False):
    app = Flask(__name__)
    app.config.from_object(config_object)

    register_extensions(app)
    register_blueprints(app)

    if not external_logger:
        logger = configure_logger(app)
        logger.info("App initialization successful")

    load_noiz_config(app)

    if mode == "app":
        return app


def load_noiz_config(app):
    app.noiz_config = {}
    processed_data_dir = os.environ.get("PROCESSED_DATA_DIR")
    if processed_data_dir is None:
        raise ValueError("You have to set a PROCESSED_DATA_DIR env variable.")
    app.noiz_config["processed_data_dir"] = processed_data_dir

    return None


def register_extensions(app):
    db.init_app(app)
    import noiz.models

    migrate.init_app(app, db)
    return None


def register_blueprints(app):
    app.register_blueprint(simple_page)
    return None


def configure_logger(app):
    """Configure loggers."""

    logging.config.dictConfig(logger_config)
    logger = logging.getLogger("app")
    logger.debug("Initializing logger")
    return logger

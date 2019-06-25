from flask import Flask
import logging
import logging.config

from noiz.logging import logger_config
from noiz.routes import simple_page
from noiz.database import db, migrate
from noiz.cli import init_group


def create_app(config_object="noiz.settings"):
    app = Flask(__name__)
    app.config.from_object(config_object)

    from noiz.models import ProcessingConfig, File

    register_extensions(app)
    register_blueprints(app)
    register_cli_extensions(app)
    logger = configure_logger(app)
    logger.info("App initialization successful")
    return app


def register_extensions(app):
    db.init_app(app)
    migrate.init_app(app, db)
    return None


def register_blueprints(app):
    app.register_blueprint(simple_page)
    return None


def register_cli_extensions(app):
    app.cli.add_command(init_group)

    return None


def configure_logger(app):
    """Configure loggers."""

    logging.config.dictConfig(logger_config)
    logger = logging.getLogger("app")
    logger.debug("Initializing logger")
    return logger

import logging
import os
import sys
from flask import Flask
from loguru import logger
from pathlib import Path
from typing import Union

from noiz.database import db, migrate
from noiz.routes import simple_page

DEFAULT_LOGGING_LEVEL = 30


def create_app(
        config_object: str = "noiz.settings",
        mode: str = "app",
        verbosity: int = 0,
):
    app = Flask(__name__)
    app.config.from_object(config_object)

    register_extensions(app)
    register_blueprints(app)

    load_noiz_config(app)

    configure_logging(app=app, verbosity=verbosity)
    logger.info("App initialization successful")

    if mode == "app":
        return app


def configure_logging(app: Flask, verbosity: int, quiet: bool = False):

    loglevel = os.environ.get("LOGLEVEL", DEFAULT_LOGGING_LEVEL)

    if isinstance(loglevel, int):
        baselevel = loglevel
    elif isinstance(loglevel, str):
        baselevel = logger.level(loglevel).no
    else:
        raise ValueError("LOGLEVEL should be either positive int or string parsable by loguru")

    logger_level = baselevel - (verbosity * 10)

    if quiet:
        logger_level = logger.level("ERROR").no

    logger.remove()
    logger.add(sys.stderr, filter="noiz", level=logger_level, enqueue=True)

    class InterceptHandler(logging.Handler):
        def emit(self, record):
            # Retrieve context where the logging call occurred, this happens to be in the 6th frame upward
            logger_opt = logger.opt(depth=6, exception=record.exc_info)
            logger_opt.log(record.levelno, record.getMessage())

    handler = InterceptHandler()
    handler.setLevel(0)
    for hndlr in app.logger.handlers:
        app.logger.removeHandler(hndlr)
    app.logger.addHandler(handler)


def load_noiz_config(app: Flask):
    # FIXME Remove that noiz config, it's useless I think. Fix usages in inventory CLI also
    app.noiz_config = {}
    processed_data_dir = os.environ.get("PROCESSED_DATA_DIR")
    if processed_data_dir is None:
        raise ValueError("You have to set a PROCESSED_DATA_DIR env variable.")
    if not Path(processed_data_dir).exists():
        raise NotADirectoryError(f"Directory provided with `PROCESSED_DATA_DIR` have to exist. {processed_data_dir} ")
    app.noiz_config["processed_data_dir"] = processed_data_dir

    return None


def register_extensions(app: Flask):
    db.init_app(app)

    migrate.init_app(app, db)
    return None


def register_blueprints(app: Flask):
    app.register_blueprint(simple_page)
    return None

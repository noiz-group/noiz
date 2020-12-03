import logging
import os
import sys
from flask import Flask
from loguru import logger
from pathlib import Path
from typing import Union

from noiz.database import db, migrate
from noiz.routes import simple_page


def create_app(
        config_object: str = "noiz.settings",
        mode: str = "app",
        logging_level: str = "INFO",
):
    app = Flask(__name__)
    app.config.from_object(config_object)

    register_extensions(app)
    register_blueprints(app)

    load_noiz_config(app)

    configure_logging(app, logging_level)
    logger.info("App initialization successful")

    if mode == "app":
        return app


def configure_logging(app: Flask, logging_level: Union[str, int]):
    logger.remove()
    logger.add(sys.stderr, filter="noiz", level=logging_level)

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

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import os
import sys
from flask import Flask, g
from loguru import logger
from pathlib import Path

from noiz.database import db, migrate
from noiz.routes import simple_page

DEFAULT_LOGGING_LEVEL = logger.level("INFO").no


def create_app(config_object: str = "noiz.settings", mode: str = "app", verbosity: int = 0, quiet: bool = False):
    app = Flask(__name__)
    app.config.from_object(config_object)

    register_extensions(app)
    register_blueprints(app)

    with app.app_context():
        set_global_verbosity(verbosity=verbosity, quiet=quiet)
        setup_logging()
    logger.debug("App initialization successful")

    if mode == "app":
        return app
    else:
        raise NotImplementedError(f"Mode {mode} is not implemented")


def set_global_verbosity(verbosity: int = 0, quiet: bool = False):
    loglevel = os.environ.get("LOGLEVEL", DEFAULT_LOGGING_LEVEL)
    if isinstance(loglevel, int):
        baselevel = loglevel
    elif isinstance(loglevel, str):
        baselevel = logger.level(loglevel).no
    else:
        raise ValueError("LOGLEVEL should be either positive int or string parsable by loguru")

    logger_level = baselevel - (verbosity * 10)
    if logger_level < 0:
        logger_level = 0

    if quiet:
        logger_level = logger.level("ERROR").no

    g.logger_level = logger_level
    return


def setup_logging():
    logger.remove()
    logger.add(sys.stderr, level=g.logger_level, enqueue=True)

    # class InterceptHandler(logging.Handler):
    #     def emit(self, record):
    #         # Retrieve context where the logging call occurred, this happens to be in the 6th frame upward
    #         logger_opt = logger.opt(depth=6, exception=record.exc_info)
    #         logger_opt.log(record.levelno, record.getMessage())
    #
    # handler = InterceptHandler()
    # handler.setLevel(0)
    # for hndlr in app.logger.handlers:
    #     app.logger.removeHandler(hndlr)
    # app.logger.addHandler(handler)


def register_extensions(app: Flask):
    db.init_app(app)

    migrate.init_app(app, db)
    return None


def register_blueprints(app: Flask):
    app.register_blueprint(simple_page)
    return None

# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import os
import sys
from flask import Flask, g
from loguru import logger
from pathlib import Path

from noiz.config import get_config
from noiz.database import db, migrate
from noiz.routes import simple_page

DEFAULT_LOGGING_LEVEL = logger.level("INFO").no


def create_app(mode: str = "app", verbosity: int = 0, quiet: bool = False):
    """Create Flask application with pydantic-settings based configuration.

    Configuration is loaded automatically from environment variables via get_config().

    :param mode: Application mode ('app' is the only supported value)
    :type mode: str
    :param verbosity: Logging verbosity level (0-3, higher = more verbose)
    :type verbosity: int
    :param quiet: If True, only show ERROR level logs
    :type quiet: bool
    :return: Configured Flask application instance
    :rtype: Flask
    """
    app = Flask(__name__)

    # Load configuration from environment using pydantic-settings
    # get_config() is cached, so reload behavior is automatic within same process
    noiz_config = get_config()

    # Populate Flask config from NoizConfig
    app.config.from_mapping(noiz_config.to_flask_config())

    # Store NoizConfig object for direct access
    app.config["NOIZ_CONFIG"] = noiz_config

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
    """Set application logging verbosity level.

    Reads log level from app config (set by NoizConfig) and adjusts based
    on verbosity flags.

    :param verbosity: Additional verbosity (0-3, each level subtracts 10 from base level)
    :type verbosity: int
    :param quiet: If True, force ERROR level logging
    :type quiet: bool
    :return: None
    :rtype: NoneType
    """
    from flask import current_app

    # Get base log level from config (new way) or environment (legacy fallback)
    loglevel = current_app.config.get("NOIZ_LOGLEVEL") or os.environ.get("LOGLEVEL", DEFAULT_LOGGING_LEVEL)

    # Convert to numeric level
    if isinstance(loglevel, int):
        baselevel = loglevel
    elif isinstance(loglevel, str):
        baselevel = logger.level(loglevel).no
    else:
        raise ValueError("LOGLEVEL should be either positive int or string parsable by loguru")

    # Apply verbosity adjustment
    logger_level = baselevel - (verbosity * 10)
    if logger_level < 0:
        logger_level = 0

    # Override with quiet flag
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

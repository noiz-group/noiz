# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import sys
from flask import Flask, g
from loguru import logger

from noiz.settings import NoizSettings
from noiz.database import db, migrate
from noiz.routes import simple_page


def create_app(
        mode: str = "app",
        verbosity: int = 20,
        quiet: bool = False,
):
    """
    Initializes Noiz application.

    You can control by it a logging level that is used across noiz.
    Since Noiz is using Loguru for logging, please refer to their documentation for details about levels:
    https://loguru.readthedocs.io/en/stable/api/logger.html#levels

    Used levels from most verbose to least verbose:
    Trace    => 5
    DEBUG    => 10
    INFO     => 20
    SUCCESS  => 25
    WARNING  => 30
    ERROR    => 40
    CRITICAL => 50

    Please use desired value in the verbosity in :paramref:`create_app.verbosity`

    :param mode:
    :type mode: str
    :param verbosity: Log verbosity of Noiz. Levels provided above.
    :type verbosity: int
    :param quiet: Overrides verbosity level and sets it to ERROR (40)
    :type quiet: bool
    :return:
    """
    app = Flask(__name__)
    settings = NoizSettings()
    app.config.from_mapping(settings)

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


def set_global_verbosity(
        verbosity: int = 0,
        quiet: bool = False,
) -> None:
    """
    Sets global verbosity level for logs.

    :param verbosity: Log verbosity. Should be positive int.
    :type verbosity: int
    :param quiet: Overrides log verbosity level and sets it to ERROR (40)
    :type quiet: bool
    """

    if verbosity < 0:
        verbosity = 0

    if quiet:
        verbosity = logger.level("ERROR").no

    g.logger_level = verbosity
    return


def setup_logging():
    logger.remove()
    logger.add(sys.stderr, level=g.logger_level, enqueue=True)


def register_extensions(app: Flask):
    db.init_app(app)

    migrate.init_app(app, db)
    return None


def register_blueprints(app: Flask):
    app.register_blueprint(simple_page)
    return None

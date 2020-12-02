from flask import Flask
import os

from noiz.routes import simple_page
from noiz.database import db, migrate


def create_app(
        config_object: str = "noiz.settings",
        mode: str = "app",
        logging_level: str = "WARNING",
):
    app = Flask(__name__)
    app.config.from_object(config_object)

    register_extensions(app)
    register_blueprints(app)

    load_noiz_config(app)

    configure_logging(logging_level)

    app.logger.info("App initialization successful")

    if mode == "app":
        return app


def configure_logging(logging_level: str):
    from noiz import rich_handler, log

    rich_handler.setLevel(logging_level)
    log.handlers = []
    log.addHandler(rich_handler)


def load_noiz_config(app: Flask):
    # FIXME Remove that noiz config, it's useless I think. Fix usages in inventory CLI also
    app.noiz_config = {}
    processed_data_dir = os.environ.get("PROCESSED_DATA_DIR")
    if processed_data_dir is None:
        raise ValueError("You have to set a PROCESSED_DATA_DIR env variable.")
    app.noiz_config["processed_data_dir"] = processed_data_dir

    return None


def register_extensions(app: Flask):
    db.init_app(app)

    migrate.init_app(app, db)
    return None


def register_blueprints(app: Flask):
    app.register_blueprint(simple_page)
    return None

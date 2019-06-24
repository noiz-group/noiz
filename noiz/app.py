from flask import Flask
import logging
from logging.handlers import RotatingFileHandler
import sys

from noiz.routes import simple_page
from noiz.database import db, migrate

from noiz.models import ProcessingConfig, File



def create_app(config_object="noiz.settings"):
    app = Flask(__name__)
    app.config.from_object(config_object)

    from noiz.models import ProcessingConfig, File

    register_extensions(app)
    register_blueprints(app)
    register_cli_extensions(app)
    configure_logger(app)
    return app


def register_extensions(app):
    db.init_app(app)
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
    formatter = logging.Formatter(app.config['LOG_FORMATTER'])

    stream_handler = logging.StreamHandler(sys.stdout)
    rot_file_handler = RotatingFileHandler(app.config['LOG_FILE'], maxBytes=10000, backupCount=1)


    if not app.logger.handlers:
        for handler in (stream_handler, rot_file_handler):
            handler.setFormatter(formatter)
            handler.setLevel(app.config['LOG_LEVEL'])
            app.logger.addHandler(handler)

# # create and configure the app
# app = Flask(__name__, instance_relative_config=True)
# # app.config.from_mapping(
# #     SECRET_KEY='dev',
# #     SQLALCHEMY_DATABASE_URI=
# # )
# #
# # from noiz.models import db
# # db.init_app(app)

# if test_config is None:
#     # load the instance config, if it exists, when not testing
#     app.config.from_pyfile('config.py', silent=True)
# else:
#     # load the test config if passed in
#     app.config.from_mapping(test_config)

# # ensure the instance folder exists
# try:
#     os.makedirs(app.instance_path)
# except OSError:
#     pass


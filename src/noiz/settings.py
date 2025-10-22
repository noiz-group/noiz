# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from environs import Env
from noiz.database_backends import DatabaseBackend

env = Env()
env.read_env()

FLASK_ENV = env.str("FLASK_ENV", default="development")
DATABASE_BACKEND = env.str("DATABASE_BACKEND", default=DatabaseBackend.POSTGRESQL.value)

if FLASK_ENV == "development":
    DEBUG = True
else:
    DEBUG = False

POSTGRES_HOST = env.str("POSTGRES_HOST", default="")
POSTGRES_PORT = env.str("POSTGRES_PORT", default="")
POSTGRES_USER = env.str("POSTGRES_USER", default="")
POSTGRES_PASSWORD = env.str("POSTGRES_PASSWORD", default="")
POSTGRES_DB = env.str("POSTGRES_DB", default="")
SQLALCHEMY_DATABASE_URI = env.str("DATABASE_URL", default="")

postgres_params_empty = all(
    (x in ("", None) for x in (POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD))
)

db_uri_empty = SQLALCHEMY_DATABASE_URI in ("", None)

# Handle database backend selection
if DATABASE_BACKEND == DatabaseBackend.SQLITE.value:
    # SQLite backend - use default path if not specified
    if db_uri_empty:
        SQLALCHEMY_DATABASE_URI = "sqlite:///noiz.db"
elif DATABASE_BACKEND == DatabaseBackend.POSTGRESQL.value:
    # PostgreSQL backend - existing logic
    if not postgres_params_empty and db_uri_empty:
        SQLALCHEMY_DATABASE_URI = (
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )

    if postgres_params_empty and db_uri_empty:
        raise ConnectionError(
            "You have to specify either all POSTGRES_ connection variables or a SQLALCHEMY_DATABASE_URI"
        )
else:
    raise ValueError(f"Invalid DATABASE_BACKEND: {DATABASE_BACKEND}. Must be 'sqlite' or 'postgresql'.")

PROCESSED_DATA_DIR = env.str("PROCESSED_DATA_DIR", default="")
if PROCESSED_DATA_DIR == "":
    raise ValueError("You have to set a PROCESSED_DATA_DIR env variable.")

MSEEDINDEX_EXECUTABLE = env.str("MSEEDINDEX_EXECUTABLE", default="")
if MSEEDINDEX_EXECUTABLE == "":
    raise ValueError("You have to set a MSEEDINDEX_EXECUTABLE env variable")

# SECRET_KEY = env.str('SECRET_KEY')
# BCRYPT_LOG_ROUNDS = env.int('BCRYPT_LOG_ROUNDS', default=13)
DEBUG_TB_ENABLED = DEBUG
DEBUG_TB_INTERCEPT_REDIRECTS = False
CACHE_TYPE = "simple"  # Can be "memcached", "redis", etc.
SQLALCHEMY_TRACK_MODIFICATIONS = False
# WEBPACK_MANIFEST_PATH = 'webpack/manifest.json'

CELERY_BROKER_URL = ("redis://redis:6379",)
CELERY_RESULT_BACKEND = "redis://redis:6379"

import logging
from environs import Env

env = Env()
env.read_env()

ENV = env.str("FLASK_ENV", default="development")

if ENV == "development":
    DEBUG = True
else:
    DEBUG = False

POSTGRES_HOST = env.str("POSTGRES_HOST", default="")
POSTGRES_USER = env.str("POSTGRES_USER", default="")
POSTGRES_PASSWORD = env.str("POSTGRES_PASSWORD", default="")
POSTGRES_DB = env.str("POSTGRES_DB", default="")

# SECRET_KEY = env.str('SECRET_KEY')
# BCRYPT_LOG_ROUNDS = env.int('BCRYPT_LOG_ROUNDS', default=13)
DEBUG_TB_ENABLED = DEBUG
DEBUG_TB_INTERCEPT_REDIRECTS = False
CACHE_TYPE = "simple"  # Can be "memcached", "redis", etc.
SQLALCHEMY_TRACK_MODIFICATIONS = False
# WEBPACK_MANIFEST_PATH = 'webpack/manifest.json'

CELERY_BROKER_URL = ("redis://redis:6379",)
CELERY_RESULT_BACKEND = "redis://redis:6379"

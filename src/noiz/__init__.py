__version__ = "0.5.20201110"
__author__ = "Damian Kula"

from .logg import logger_config
import logging.config

logging.config.dictConfig(logger_config)
log = logging.getLogger(__name__)

from . import models
from . import api
from . import processing

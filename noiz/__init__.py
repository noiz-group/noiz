__version__ = "2020.273.0"
__author__ = "Damian Kula"


from .logg import logger_config
from . import api
from . import processing

import logging.config

logging.config.dictConfig(logger_config)
log = logging.getLogger(__name__)

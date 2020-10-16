__version__ = "0.5.2020290"
__author__ = "Damian Kula"


from .logg import logger_config
from . import api
from . import processing

import logging.config

logging.config.dictConfig(logger_config)
log = logging.getLogger(__name__)

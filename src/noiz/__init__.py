__version__ = "0.5.20201110"
__author__ = "Damian Kula"

import logging
from rich.logging import RichHandler
rich_handler = RichHandler(rich_tracebacks=True)
log = logging.getLogger(__name__)
log.addHandler(rich_handler)
log.error(__name__)

from . import models
from . import api
from . import processing

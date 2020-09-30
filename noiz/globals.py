import os
from pathlib import Path

PROCESSED_DATA_DIR: Path = Path(os.environ.get("PROCESSED_DATA_DIR"))

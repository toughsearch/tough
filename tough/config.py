import os
from pathlib import Path

CONF_NAME = os.getenv("CONF_NAME", "conf.yaml")
INDEX_DIR = Path(os.getenv("INDEX_DIR", ".index"))
DATE_INDEX_NAME = "date_index"
NUM_WORKERS = int(os.getenv("NUM_WORKERS", os.cpu_count()))
MIN_CHUNK_LENGTH = int(os.getenv("MIN_CHUNK_LENGTH", 300_000))

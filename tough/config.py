import os

CONF_NAME = "conf.yaml"
INDEX_DIR = ".index"
NUM_WORKERS = int(os.getenv("NUM_WORKERS", os.cpu_count()))
MIN_CHUNK_LENGTH = 300_000

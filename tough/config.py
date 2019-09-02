import os

CONF_NAME = os.getenv("CONF_NAME", "conf.ini")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", os.cpu_count()))

import yaml

from .config import CONF_NAME

try:
    indexes = yaml.safe_load(open(CONF_NAME))
except FileNotFoundError:
    indexes = {}

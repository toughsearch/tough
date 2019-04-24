import yaml

from .config import CONF_NAME

try:
    indexes = yaml.load(open(CONF_NAME), Loader=yaml.FullLoader)
except FileNotFoundError:
    indexes = {}

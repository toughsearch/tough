import yaml

from .config import CONF_NAME


def get_indexes(conf_name=CONF_NAME):
    try:
        return yaml.safe_load(open(CONF_NAME))
    except FileNotFoundError:
        return {}

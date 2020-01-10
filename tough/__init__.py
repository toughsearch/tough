import yaml

from .config import CONF_NAME


def get_indexes(conf_name: str = CONF_NAME) -> dict:
    try:
        return yaml.safe_load(open(conf_name))
    except FileNotFoundError:
        return {}

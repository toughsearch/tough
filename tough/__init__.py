import configparser

from .config import CONF_NAME


def get_indexes(conf_name=CONF_NAME):
    try:
        config = configparser.RawConfigParser()
        config.read(CONF_NAME)
        return config
    except FileNotFoundError:
        return {}

from datetime import datetime, timedelta, timezone
import os
import re

from . import indexes
from .config import INDEX_DIR


def dotify(file_path):
    path, fname = os.path.split(file_path)
    return os.path.join(path, "." + fname)


def date_range(str_d1, str_d2):
    fmt = "%Y-%m-%d"

    d1 = datetime.strptime(str_d1, fmt)
    d2 = datetime.strptime(str_d2, fmt)

    while d1.date() <= d2.date():
        yield str(d1.date())
        d1 += timedelta(days=1)


def ensure_index_dir(index_dir=INDEX_DIR):
    os.makedirs(index_dir, exist_ok=True)


def get_datetime(row, index_name):
    regex = indexes[index_name]["datetime_regex"]
    fmt = indexes[index_name]["datetime_format"]
    m = re.search(regex.encode(), row)
    if not m:
        raise ValueError

    dt = datetime.strptime(m.group(1).decode(), fmt)
    return str(dt.astimezone(timezone.utc).date())

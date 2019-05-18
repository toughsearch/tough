from datetime import datetime, timedelta, timezone
import os
import re

from . import indexes
from .config import INDEX_DIR
from .dt import get_date


def date_range(str_d1, str_d2):
    fmt = "%Y-%m-%d"

    d1 = datetime.strptime(str_d1, fmt)
    d2 = datetime.strptime(str_d2, fmt)

    while d1.date() <= d2.date():
        yield str(d1.date())
        d1 += timedelta(days=1)


def ensure_index_dir(index_dir=INDEX_DIR):
    for index_name in indexes:
        os.makedirs(os.path.join(index_dir, index_name), exist_ok=True)


def get_datetime(row, index_name):
    return get_datetime_ex(
        row,
        indexes[index_name]["datetime_regex"],
        indexes[index_name]["datetime_format"],
    )


def get_datetime_ex(row, regex, fmt):
    # m = re.search(regex, row)
    # if not m:
    #     raise ValueError
    # dt = str(datetime.strptime(m.group(1).decode(), fmt.decode()).astimezone(timezone.utc))
    dt = get_date(row, regex, fmt)
    return dt

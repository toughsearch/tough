from datetime import datetime, timedelta, timezone
import re

from . import get_indexes


def date_range(str_d1, str_d2):
    fmt = "%Y-%m-%d"

    d1 = datetime.strptime(str_d1, fmt)
    d2 = datetime.strptime(str_d2, fmt)

    while d1.date() <= d2.date():
        yield str(d1.date())
        d1 += timedelta(days=1)


def get_datetime(row, index_name):
    indexes = get_indexes()
    return get_date_ex(
        row,
        indexes[index_name]["datetime_regex"],
        indexes[index_name]["datetime_format"],
    )


def get_date_ex(row, regex, fmt):
    m = re.search(regex.encode(), row)
    if not m:
        raise ValueError

    dt = datetime.strptime(m.group(1).decode(), fmt)
    return dt.astimezone(timezone.utc).date()

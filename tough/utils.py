import ctypes
from ctypes.util import find_library
from datetime import datetime, timedelta, timezone
import os
import re

from . import indexes
from .config import INDEX_DIR


libc = ctypes.CDLL(find_library('c'))


class struct_tm(ctypes.Structure):
    _fields_ = [
        ('tm_sec', ctypes.c_int),
        ('tm_min', ctypes.c_int),
        ('tm_hour', ctypes.c_int),
        ('tm_mday', ctypes.c_int),
        ('tm_mon', ctypes.c_int),
        ('tm_year', ctypes.c_int),
        ('tm_wday', ctypes.c_int),
        ('tm_yday', ctypes.c_int),
        ('tm_isdst', ctypes.c_int),
        ('tm_gmtoff', ctypes.c_long),
        ('tm_zone', ctypes.c_int)]


strptime_proto = ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_char_p,
                                                   ctypes.c_char_p,
                                                   ctypes.POINTER(struct_tm))

strptime = strptime_proto(('strptime', libc), ((1, 's'),
                                              (1, 'format'),
                                              (2, 'tm')))


def strptime_errcheck(result, func, args):
    if result is None:
        raise ValueError("strptime error.")

    return datetime(
        args[2].tm_year + 1900,
        args[2].tm_mon,
        args[2].tm_mday,
        args[2].tm_hour,
        args[2].tm_min,
        args[2].tm_sec,
    )

strptime.errcheck = strptime_errcheck


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
    m = re.search(regex, row)
    if not m:
        raise ValueError
    dt = strptime(m.group(1), fmt)
    return dt
    return str(dt.astimezone(timezone.utc).date())

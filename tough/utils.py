from datetime import datetime, timedelta
import os

import indexed_gzip as igzip

from .config import INDEX_DIR


def dotify(file_path):
    path, fname = os.path.split(file_path)
    return os.path.join(path, "." + fname)


def fopen(name):
    if name.endswith(".gz"):
        f = igzip.IndexedGzipFile(name)
        gzindex_name = dotify(name + ".gzindex")
        if os.path.isfile(gzindex_name):
            f.import_index(gzindex_name)
        else:
            f.build_full_index()
            f.export_index(gzindex_name)
        return f

    return open(name, "r+b")


def date_range(str_d1, str_d2):
    fmt = "%Y-%m-%d"

    d1 = datetime.strptime(str_d1, fmt)
    d2 = datetime.strptime(str_d2, fmt)

    while d1.date() <= d2.date():
        yield str(d1.date())
        d1 += timedelta(days=1)


def ensure_index_dir():
    os.makedirs(INDEX_DIR, exist_ok=True)

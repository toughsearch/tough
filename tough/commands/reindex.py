from collections import defaultdict
from functools import partial
import glob
from io import BytesIO
import json
import multiprocessing as mp
import os

from .. import get_indexes
from ..config import DATE_INDEX_NAME, INDEX_DIR, NUM_WORKERS
from ..eol_mapper import EOLMapper
from ..opener import fopen
from ..utils import ensure_index_dir, get_datetime_ex

BUF_SIZE = 2 * 1024 * 1024


def run_reindex(index=None):
    ensure_index_dir()
    indexes = get_indexes()
    for index_name, index_conf in indexes.items():
        if index and index_name != index:
            continue

        files = get_index_files(index_conf)
        files = sorted_files(files, index_name)

        pool = mp.Pool(NUM_WORKERS)

        try:
            for path in files:
                add_to_index(path, index_name, pool=pool)

        finally:
            pool.close()
            pool.join()


def get_index_files(index_conf):
    return glob.glob(
        os.path.join(index_conf["base_dir"], index_conf["pattern"])
    )


def sorted_files(files, index_name):
    indexes = get_indexes()
    index_conf = indexes[index_name]
    datetime_regex = index_conf["datetime_regex"]
    datetime_format = index_conf["datetime_format"]

    to_sort = []
    for path in files:
        with fopen(path, index_name) as f:
            first_row = next(f)
            first_datetime = get_datetime_ex(
                first_row, datetime_regex, datetime_format
            )
            to_sort.append((first_datetime, path))

    return [x[1] for x in sorted(to_sort)]


def add_to_index(path, index_name, *, pool):
    filename = os.path.basename(path)
    eol_mapper = EOLMapper(path, index_name)
    eol_mapper.open()

    date_index_path = os.path.join(INDEX_DIR, index_name, DATE_INDEX_NAME)
    date_index = {}
    try:
        date_index = json.load(open(date_index_path, "r"))
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    date_index = defaultdict(dict, date_index)
    cur_lineno = 0

    _indexer = partial(indexer, index_name=index_name)

    opener = fopen(path, index_name)
    with opener as f:
        for lines in pool.imap(_indexer, bufferizer(f, BUF_SIZE)):
            for date, offset in lines:
                eol_mapper.write(cur_lineno, offset)
                date_index[date].setdefault(filename, [])
                if len(date_index[date][filename]) < 2:
                    date_index[date][filename].append(cur_lineno)
                else:
                    date_index[date][filename][1] = cur_lineno
                cur_lineno += 1
        opener.export_index()

    eol_mapper.mark_ok()
    eol_mapper.close()

    json.dump(date_index, open(date_index_path, "w"))


def bufferizer(f, buf_size):
    while True:
        offset = f.tell()
        buf = f.read(buf_size)
        if not buf:
            break

        buf += f.readline()
        yield buf, offset


def indexer(args, index_name):
    indexes = get_indexes()
    index_conf = indexes[index_name]
    datetime_regex = index_conf["datetime_regex"]
    datetime_format = index_conf["datetime_format"]

    buf, offset = args
    stream = BytesIO(buf)

    lines = []
    for line in stream:
        date = get_datetime_ex(line, datetime_regex, datetime_format)
        lines.append((date, offset + stream.tell()))

    return lines

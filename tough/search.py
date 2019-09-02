from datetime import date
from functools import partial
import glob
import multiprocessing as mp
import os
import re
import sys

from . import get_indexes
from .config import NUM_WORKERS
from .opener import fopen
from .utils import get_date_ex

BUF_SIZE = 2 * 1024 * 1024


def get_files(index_conf, date_from: date, date_to: date):
    files = glob.glob(
        os.path.join(index_conf["base_dir"], index_conf["pattern"])
    )

    datetime_regex = index_conf["datetime_regex"]
    datetime_format = index_conf["datetime_format"]

    to_sort = []
    for path in files:
        with fopen(path) as f:
            first_row = next(f)
            first_date = get_date_ex(first_row, datetime_regex, datetime_format)

            to_sort.append((first_date, path))

    to_sort.sort()

    filtered_files = []
    prev_path = ""
    for first_date, path in to_sort:
        if not (date_from <= first_date <= date_to):
            prev_path = path
            continue

        if prev_path:
            filtered_files.append(prev_path)
            prev_path = ""

        filtered_files.append(path)

    if not filtered_files:
        filtered_files = [to_sort[-1][1]]
    return filtered_files


def bufferizer(f, buf_size):
    while True:
        offset = f.tell()
        buf = f.read(buf_size)
        if not buf:
            break

        buf += f.readline()
        yield buf, offset


def searcher(
    path, regex, substring, index_conf, date_from: date, date_to: date
):
    results = []
    check = lambda x: substring in x  # noqa
    if regex is not None:
        check = regex.search

    with fopen(path) as f:
        for line in f:
            line_date = get_date_ex(
                line,
                index_conf["datetime_regex"],
                index_conf["datetime_format"],
            )

            if line_date > date_to:
                break

            if line_date < date_from:
                continue

            if not check(line) or not line:
                continue

            result_line = line.strip()
            results.append(result_line)

    return path, results


def run_search(
    substring,
    regex,
    index,
    date_from: date = date.min,
    date_to: date = date.max,
):
    if not substring and not regex:
        sys.stderr.write("Please provide substring or --regex (-e) parameter\n")
        return

    indexes = get_indexes()
    index_conf = indexes[index]

    to_search = get_files(index_conf, date_from, date_to)
    if not to_search:
        return

    func = partial(
        searcher,
        regex=re.compile(regex.encode()) if regex else None,
        substring=substring.encode(),
        date_from=date_from,
        date_to=date_to,
        index_conf=index_conf,
    )

    pool = mp.Pool(NUM_WORKERS)

    try:
        for _, result in pool.imap(func, to_search):
            sys.stdout.write("\n".join(x.decode() for x in result) + "\n")
    finally:
        pool.close()
        pool.join()

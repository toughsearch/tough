from collections import defaultdict
from functools import partial
import glob
from io import BytesIO
import json
import multiprocessing as mp
import os
from typing import Dict, Generator, List, Tuple

import yaml

from . import get_indexes
from .config import DATE_INDEX_NAME, INDEX_DIR, NUM_WORKERS
from .eol_mapper import EOLMapper
from .opener import fopen
from .utils import get_datetime_ex

BUF_SIZE = 2 * 1024 * 1024


class Index:
    def __init__(
        self,
        name: str,
        base_dir: str,
        pattern: str,
        datetime_regex: str,
        datetime_format: str,
    ):
        self.name = name
        self.base_dir = base_dir
        self.pattern = pattern
        self.datetime_regex = datetime_regex
        self.datetime_format = datetime_format

    def get_files(self) -> List[str]:
        files = glob.glob(os.path.join(self.base_dir, self.pattern))
        to_sort = []
        for path in files:
            with fopen(path, self.name) as f:
                first_row = next(f)
                first_datetime = get_datetime_ex(
                    first_row, self.datetime_regex, self.datetime_format
                )
                to_sort.append((first_datetime, path))

        return [x[1] for x in sorted(to_sort)]

    def reindex(self) -> None:
        pool = mp.Pool(NUM_WORKERS)

        try:
            for path in self.get_files():
                add_to_index(path, self.name, pool=pool)

        finally:
            pool.close()
            pool.join()


class IndexCollection(Dict[str, Index]):
    @classmethod
    def from_yaml(cls, filename: str) -> "IndexCollection":
        raw_conf: Dict[str, Dict[str, str]] = yaml.safe_load(open(filename))
        return cls.from_dict(raw_conf)

    @classmethod
    def from_dict(cls, d: Dict[str, Dict[str, str]]) -> "IndexCollection":
        indexes: Dict[str, Index] = {}
        for name, values in d.items():
            indexes[name] = Index(
                name,
                values["base_dir"],
                values["pattern"],
                values["datetime_regex"],
                values["datetime_format"],
            )

        return cls(indexes)


def add_to_index(path, index_name, *, pool) -> None:
    filename = os.path.basename(path)
    eol_mapper = EOLMapper(path, index_name)
    eol_mapper.open()

    date_index_path = os.path.join(INDEX_DIR, index_name, DATE_INDEX_NAME)
    date_index: dict = {}
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


def bufferizer(f, buf_size) -> Generator[Tuple[str, int], None, None]:
    while True:
        offset = f.tell()
        buf = f.read(buf_size)
        if not buf:
            break

        buf += f.readline()
        yield buf, offset


def indexer(args, index_name) -> List[Tuple[str, int]]:
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

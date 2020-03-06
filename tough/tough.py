import argparse
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta, timezone
from functools import partial
import glob
from io import BytesIO
import json
import multiprocessing as mp
import os
from pathlib import Path
import re
import sys
from types import TracebackType
from typing import (
    BinaryIO,
    Callable,
    ContextManager,
    Dict,
    Generator,
    List,
    Match,
    Optional,
    Tuple,
    Type,
    Union,
)

import indexed_gzip as igzip
from tqdm import tqdm
import yaml

from . import CONF_NAME, get_indexes
from .config import DATE_INDEX_NAME, INDEX_DIR, MIN_CHUNK_LENGTH, NUM_WORKERS

LEN_OFFSET = 5
OK = b"OK"
BUF_SIZE = 2 * 1024 * 1024
BYTE_ORDER = "little"
MapLine = namedtuple("MapLine", ["lineno", "offset", "length"])


class EOLMapper:
    """
    File EOL mapper.
    """

    f: Optional[Union[BinaryIO, igzip.IndexedGzipFile]]

    def __init__(self, fname, index_name) -> None:
        basename = os.path.basename(fname)
        self.map_fname = INDEX_DIR / index_name / f"{basename}.map"
        self.map_fname.touch()

        self.f = None
        self.f_read = open(self.map_fname, "rb")

    def open(self) -> None:
        self.f = open(self.map_fname, "r+b")

    def close(self) -> None:
        if not self.f:
            raise IOError

        self.f.close()

    def read(self, lineno) -> Optional[MapLine]:
        if lineno >= self.count_lines() or lineno < 0:
            return None

        f = self.f_read
        f.seek(LEN_OFFSET * max(lineno - 1, 0))

        if lineno > 0:
            record = f.read(LEN_OFFSET * 2)
            offset_start = int.from_bytes(record[:LEN_OFFSET], BYTE_ORDER)
            offset_end = int.from_bytes(record[LEN_OFFSET:], BYTE_ORDER)
        else:
            record = f.read(LEN_OFFSET)
            offset_start = 0
            offset_end = int.from_bytes(record[:LEN_OFFSET], BYTE_ORDER)

        length = offset_end - offset_start - 1

        return MapLine(lineno, offset_start, length)

    def write(self, lineno, offset) -> None:
        if not self.f:
            raise IOError

        record = offset.to_bytes(LEN_OFFSET, BYTE_ORDER)
        self.f.seek(LEN_OFFSET * lineno)
        self.f.write(record)

    def count_lines(self) -> int:
        return self.map_fname.stat().st_size // LEN_OFFSET

    def mark_ok(self) -> None:
        if not self.f:
            raise IOError
        self.f.seek(0, 2)
        self.f.write(OK)


def chunkify(
    to_search, index_name, min_chunk_length=MIN_CHUNK_LENGTH
) -> Generator[Tuple[str, int, int, int], None, None]:
    for path, lines_range in to_search:
        lines_from = 0
        lines_to = EOLMapper(path, index_name).count_lines()
        length = min_chunk_length
        if lines_range is not None:
            if len(lines_range) == 1:
                lines_from = lines_range[0]
                lines_to = lines_from + 1
                length = 1

            elif len(lines_range) == 2:
                lines_from, lines_to = lines_range
                lines = lines_to - lines_from
                length = max(round(lines / (NUM_WORKERS * 4)), min_chunk_length)

            else:
                raise ValueError("Wrong date index")

        for line_start in range(lines_from, lines_to, length):
            yield path, line_start, length, lines_to


class Indexer:
    def __init__(self):
        ...

    def add(self):
        ...


class Index:
    def __init__(
        self,
        name: str,
        base_dir: str,
        pattern: str,
        datetime_regex: str,
        datetime_format: str,
    ) -> None:
        self.name = name
        self.base_dir = Path(base_dir)
        self.pattern = pattern
        self.datetime_regex = datetime_regex
        self.datetime_format = datetime_format

    def get_files(self) -> List[str]:
        files = glob.glob(str(self.base_dir / self.pattern))
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
                self.add(path, pool=pool)

        finally:
            pool.close()
            pool.join()

    def add(self, path, *, pool) -> None:
        filename = os.path.basename(path)
        eol_mapper = EOLMapper(path, self.name)
        eol_mapper.open()

        date_index_path = os.path.join(INDEX_DIR, self.name, DATE_INDEX_NAME)
        date_index: dict = {}
        try:
            date_index = json.load(open(date_index_path, "r"))
        except (FileNotFoundError, json.JSONDecodeError):
            pass

        date_index = defaultdict(dict, date_index)
        cur_lineno = 0

        _indexer = partial(indexer, index_name=self.name)

        opener = fopen(path, self.name)
        with opener as f:
            for lines in pool.imap(_indexer, self.bufferizer(f, BUF_SIZE)):
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

    @staticmethod
    def bufferizer(f, buf_size) -> Generator[Tuple[str, int], None, None]:
        while True:
            offset = f.tell()
            buf = f.read(buf_size)
            if not buf:
                break

            buf += f.readline()
            yield buf, offset


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


class Opener(ContextManager):
    file: Optional[Union[igzip.IndexedGzipFile, BinaryIO]]

    def __init__(self, name: str, index_name: str) -> None:
        self.name = name
        self.index_name = index_name
        self.file = None

    def __enter__(self) -> Union[igzip.IndexedGzipFile, BinaryIO]:
        self.file = self.open()
        return self.file

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> None:
        if not self.file:
            raise IOError

        self.file.close()

    def open(
        self
    ) -> Union[igzip.IndexedGzipFile, BinaryIO]:  # pragma: no cover
        raise NotImplementedError

    def export_index(self) -> None:
        pass


class TextFileOpener(Opener):
    def open(self) -> BinaryIO:
        return open(self.name, "r+b")


class GzipFileOpener(Opener):
    def open(self) -> Union[igzip.IndexedGzipFile, igzip.IndexedGzipFile]:
        f = igzip.IndexedGzipFile(self.name)
        basename = os.path.basename(self.name)
        gzindex_name = INDEX_DIR / self.index_name / f"{basename}.gzindex"
        if gzindex_name.is_file():
            f.import_index(gzindex_name)

        return f

    def export_index(self):
        if not self.file:
            raise IOError

        basename = os.path.basename(self.name)
        self.file.seek(self.file.tell() - 1)
        gzindex_name = INDEX_DIR / self.index_name / f"{basename}.gzindex"
        self.file.export_index(gzindex_name)


def fopen(name, index_name) -> Opener:
    opener: Type[Opener] = TextFileOpener
    if name.endswith(".gz"):
        opener = GzipFileOpener

    return opener(name, index_name)


def date_range(str_d1: str, str_d2: str) -> Generator[str, None, None]:
    fmt = "%Y-%m-%d"

    d1 = datetime.strptime(str_d1, fmt)
    d2 = datetime.strptime(str_d2, fmt)

    while d1.date() <= d2.date():
        yield str(d1.date())
        d1 += timedelta(days=1)


def ensure_index_dir(index_dir: Path = INDEX_DIR) -> None:
    for index_name in get_indexes():
        (index_dir / index_name).mkdir(parents=True, exist_ok=True)


def get_datetime(row: bytes, index_name: str) -> str:
    indexes = get_indexes()
    return get_datetime_ex(
        row,
        indexes[index_name]["datetime_regex"],
        indexes[index_name]["datetime_format"],
    )


def get_datetime_ex(row: bytes, regex: str, fmt: str) -> str:
    m = re.search(regex.encode(), row)
    if not m:
        raise ValueError

    dt = datetime.strptime(m.group(1).decode(), fmt)
    return str(dt.astimezone(timezone.utc).date())


def run_reindex(index_name: Optional[str] = None) -> None:
    ensure_index_dir()
    indexes = IndexCollection.from_yaml(CONF_NAME)
    for index in indexes.values():
        if index_name and index_name != index.name:
            continue
        index.reindex()


def searcher(
    chunk: Tuple[str, int, int, int],
    regex: bool,
    substring: bytes,
    index_name: str,
) -> Tuple[str, List[Tuple[int, bytes]]]:
    path, line_start, length, line_end = chunk
    mapper = EOLMapper(path, index_name)
    results = []

    check: Union[
        Callable[[bytes], bool],
        Callable[[bytes, int, int], Optional[Match[bytes]]],
    ]
    check = lambda x: substring in x  # noqa
    if regex:
        check = re.compile(substring).search

    with fopen(path, index_name) as f:
        m = mapper.read(line_start)
        if not m:
            raise IOError
        f.seek(m.offset)
        chunk_line_end = line_start + length
        if chunk_line_end > line_end:
            chunk_line_end = line_end + 1
        for lineno in range(line_start, chunk_line_end):
            line = f.readline()
            if chunk_line_end == line_end + 1 and not line:
                break

            if not check(line):
                continue

            result_line = line.strip()
            results.append((lineno, result_line))

    return path, results


def run_search(
    substring: str,
    regex: bool,
    index: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> None:
    if not substring:
        sys.stderr.write("Please provide substring\n")
        return

    indexes = get_indexes()
    index_conf = indexes[index]
    index_data = json.load(
        open(os.path.join(INDEX_DIR, index, DATE_INDEX_NAME))
    )

    to_search: List[Tuple[str, Optional[Tuple]]] = []
    if not date_from or not date_to:
        to_search = [
            (x, None)
            for x in glob.glob(
                os.path.join(index_conf["base_dir"], index_conf["pattern"])
            )
        ]

    else:
        for d in date_range(date_from, date_to):
            if d not in index_data:
                continue
            for filename, lines_range in index_data[d].items():
                to_search.append(
                    (
                        os.path.join(
                            index_conf["base_dir"], filename.strip("/")
                        ),
                        lines_range,
                    )
                )

    func = partial(
        searcher, substring=substring.encode(), regex=regex, index_name=index
    )
    chunks = list(chunkify(to_search, index))
    pool = mp.Pool(NUM_WORKERS)

    try:
        for _, result in tqdm(pool.imap(func, chunks), total=len(chunks)):
            sys.stdout.write("\n".join(x[1].decode() for x in result) + "\n")
    finally:
        pool.close()
        pool.join()


def run() -> None:
    main_parser = argparse.ArgumentParser(description="ToughSearch")

    subparsers = main_parser.add_subparsers(dest="command")

    index_parser = subparsers.add_parser(
        "reindex", help="Reindex using conf.yaml"
    )
    index_parser.add_argument(
        "index", help="Index name to reindex", default="", nargs="?"
    )

    search_parser = subparsers.add_parser("search", help="Searcher")
    search_parser.add_argument(
        "substring", help="Substring to search", default="", nargs="?"
    )
    search_parser.add_argument("index", help="Index to search")
    search_parser.add_argument(
        "-e",
        "--regex",
        help="Substring is a regex pattern",
        action="store_true",
    )
    search_parser.add_argument("-df", "--date-from")
    search_parser.add_argument("-dt", "--date-to")

    args = main_parser.parse_args()
    dict_args = args.__dict__

    commands: Dict[str, Callable] = {
        "search": run_search,
        "reindex": run_reindex,
    }

    commands[dict_args.pop("command")](**dict_args)


if __name__ == "__main__":  # pragma: no cover
    run()

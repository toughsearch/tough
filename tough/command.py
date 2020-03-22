from datetime import datetime, timedelta
from functools import partial
import glob
import json
import multiprocessing as mp
import os
from pathlib import Path
import re
import sys
from typing import Callable, Generator, List, Match, Optional, Tuple, Union

from tqdm import tqdm

from . import CONF_NAME, get_indexes
from .config import DATE_INDEX_NAME, INDEX_DIR, MIN_CHUNK_LENGTH, NUM_WORKERS
from .eol_mapper import EOLMapper
from .index import IndexCollection
from .opener import fopen


class Command:
    def __init__(self, **params):
        ...

    def run(self) -> None:
        ...


class Indexer(Command):
    def __init__(self, index_name: Optional[str] = None) -> None:
        self.index_name = index_name

    def run(self) -> None:
        ensure_index_dir()
        indexes = IndexCollection.from_yaml(CONF_NAME)
        for index in indexes.values():
            if self.index_name and self.index_name != index.name:
                continue
            index.reindex()


class Searcher(Command):
    def __init__(
        self,
        substring: str,
        regex: bool,
        index: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> None:
        self.substring = substring
        self.regex = regex
        self.index = index
        self.date_from = date_from
        self.date_to = date_to

    def run(self) -> None:
        if not self.substring:
            sys.stderr.write("Please provide substring\n")
            return

        indexes = get_indexes()
        index_conf = indexes[self.index]
        index_data = json.load(
            open(os.path.join(INDEX_DIR, self.index, DATE_INDEX_NAME))
        )

        to_search: List[Tuple[str, Optional[Tuple]]] = []
        if not self.date_from or not self.date_to:
            to_search = [
                (x, None)
                for x in glob.glob(
                    os.path.join(index_conf["base_dir"], index_conf["pattern"])
                )
            ]

        else:
            for d in date_range(self.date_from, self.date_to):
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
            searcher,
            substring=self.substring.encode(),
            regex=self.regex,
            index_name=self.index,
        )
        lines_range = LinesRange(to_search, self.index)
        chunks = lines_range.chunkify()
        pool = mp.Pool(NUM_WORKERS)

        try:
            for _, result in tqdm(pool.imap(func, chunks), total=len(chunks)):
                sys.stdout.write(
                    "\n".join(x[1].decode() for x in result) + "\n"
                )
        finally:
            pool.close()
            pool.join()


class LinesRange:
    def __init__(
        self, to_search: List[Tuple[str, Optional[Tuple]]], index_name: str
    ) -> None:
        self.to_search = to_search
        self.index_name = index_name

    def chunkify(
        self, min_chunk_length: int = MIN_CHUNK_LENGTH
    ) -> List[Tuple[str, int, int, int]]:
        chunks = []
        for path, lines_range in self.to_search:
            lines_from = 0
            lines_to = EOLMapper(path, self.index_name).count_lines()
            length = min_chunk_length
            if lines_range is not None:
                if len(lines_range) == 1:
                    lines_from = lines_range[0]
                    lines_to = lines_from + 1
                    length = 1

                elif len(lines_range) == 2:
                    lines_from, lines_to = lines_range
                    lines = lines_to - lines_from
                    length = max(
                        round(lines / (NUM_WORKERS * 4)), min_chunk_length
                    )

                else:
                    raise ValueError("Wrong date index")

            for line_start in range(lines_from, lines_to, length):
                chunks.append((path, line_start, length, lines_to))
        return chunks


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
